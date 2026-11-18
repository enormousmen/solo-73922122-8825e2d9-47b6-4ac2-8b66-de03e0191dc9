"""Local receiver for the fault-proxy exercise.

Single process, two faces:

* asyncio TCP server: reads until EOF (the sender half-closes after uploading
  the sample), computes the byte count and SHA-256 incrementally, then
  **commits the receipt to SQLite first** and only afterwards sends the JSON
  receipt back and half-closes its own write side.
* FastAPI HTTP API (with /docs): query persisted receipts by digest and byte
  count.  Receipts survive container/process restarts because they live in a
  SQLite database on a persistent volume.

Durability / correctness rules implemented here:

* FIN first, commit second, send last.  A receipt is sent to the sender only
  after the corresponding row has been committed (synchronous=FULL).
* No FIN (peer reset / cancelled mid-upload) -> nothing is committed and no
  receipt is sent.
* DB write failure -> the transaction rolls back, no success record is left
  behind and the success receipt is withheld (connection closes silently).
* Once committed, the record is queryable via /receipts even if the receipt
  itself never reached the sender or the container restarts.
* Identical content uploaded multiple times is kept as multiple rows with
  distinct receipt ids (rx-<rowid>); there is no uniqueness constraint on the
  digest.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import sqlite3
import time
from typing import Iterator, Optional

from fastapi import FastAPI, HTTPException, Query

LISTEN_HOST = os.getenv("RECEIVER_HOST", "0.0.0.0")
TCP_PORT = int(os.getenv("RECEIVER_TCP_PORT", "9100"))
HTTP_PORT = int(os.getenv("RECEIVER_HTTP_PORT", "9101"))
DB_PATH = os.getenv("RECEIVER_DB", "receipts.db")
READ_CHUNK = 64 * 1024
DEFAULT_LIMIT = 500
MAX_LIMIT = 5000
STARTED_AT = time.time()  # process start marker, lets clients detect restarts

SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peer TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    chunks INTEGER NOT NULL,
    read_seconds REAL NOT NULL,
    eof_seen INTEGER NOT NULL,
    received_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_receipts_sha256 ON receipts(sha256);
CREATE INDEX IF NOT EXISTS idx_receipts_bytes ON receipts(bytes);
"""


# --------------------------------------------------------------------------- #
# SQLite persistence (sync driver; tiny statements, called via to_thread or
# from FastAPI sync endpoints which already run in a threadpool)
# --------------------------------------------------------------------------- #


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # WAL for concurrent readers during a write; FULL so a committed receipt
    # survives a power loss / container kill (commit = fsync).
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


@contextlib.contextmanager
def _db() -> Iterator[sqlite3.Connection]:
    """Commit on success, roll back on any error, always close."""
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create the schema; fail fast at startup if the DB is not writable.

    The receiver must never accept uploads it cannot durably record, so an
    unusable database is a fatal startup error, not a runtime surprise.
    """
    with _db() as conn:
        conn.executescript(SCHEMA)


def insert_receipt(rec: dict) -> int:
    """INSERT + COMMIT one receipt; return the row id.  Raises on failure."""
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO receipts (peer, bytes, sha256, chunks, read_seconds,"
            " eof_seen, received_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                rec["peer"],
                rec["bytes"],
                rec["sha256"],
                rec["chunks"],
                rec["read_seconds"],
                int(rec["eof_seen"]),
                rec["received_at"],
            ),
        )
        return int(cur.lastrowid)


def _row_to_receipt(row: sqlite3.Row) -> dict:
    return {
        "receipt_id": f"rx-{row['id']}",
        "peer": row["peer"],
        "bytes": row["bytes"],
        "sha256": row["sha256"],
        "chunks": row["chunks"],
        "read_seconds": row["read_seconds"],
        "eof_seen": bool(row["eof_seen"]),
        "received_at": row["received_at"],
    }


def query_receipts(
    sha256: Optional[str] = None, byte_count: Optional[int] = None, limit: int = DEFAULT_LIMIT
) -> list[dict]:
    sql = "SELECT * FROM receipts"
    clauses: list[str] = []
    args: list[object] = []
    if sha256:
        clauses.append("sha256 = ?")
        args.append(sha256.lower())
    if byte_count is not None:
        clauses.append("bytes = ?")
        args.append(byte_count)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(limit)
    with _db() as conn:
        rows = conn.execute(sql, args).fetchall()
    return [_row_to_receipt(row) for row in rows]


def get_receipt(receipt_id: str) -> Optional[dict]:
    if not receipt_id.startswith("rx-") or not receipt_id[3:].isdigit():
        return None
    with _db() as conn:
        row = conn.execute("SELECT * FROM receipts WHERE id = ?", (int(receipt_id[3:]),)).fetchone()
    return _row_to_receipt(row) if row is not None else None


def count_receipts() -> int:
    with _db() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0])


# --------------------------------------------------------------------------- #
# TCP upload endpoint
# --------------------------------------------------------------------------- #


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, RuntimeError):
        pass


async def handle_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    peer_str = f"{peer[0]}:{peer[1]}" if peer else "?"
    started = time.monotonic()
    digest = hashlib.sha256()
    size = 0
    chunks = 0
    try:
        while True:
            data = await reader.read(READ_CHUNK)
            if data == b"":
                break
            digest.update(data)
            size += len(data)
            chunks += 1
    except (ConnectionError, asyncio.CancelledError, RuntimeError, OSError) as exc:
        # No FIN (peer reset / cancelled mid-upload): commit nothing, send
        # nothing.  A partial upload must never leave a success record.
        print(f"receiver: upload from {peer_str} aborted before FIN ({exc!r}); nothing committed")
        await _close_writer(writer)
        return

    # FIN seen and digest final: commit to SQLite FIRST.  Only a durable
    # record entitles us to send a success receipt.
    receipt = {
        "receipt_id": None,  # filled from the committed row id below
        "peer": peer_str,
        "bytes": size,
        "sha256": digest.hexdigest(),
        "chunks": chunks,
        "read_seconds": round(time.monotonic() - started, 6),
        "eof_seen": True,
        "received_at": time.time(),
    }
    try:
        row_id = await asyncio.to_thread(insert_receipt, receipt)
    except Exception as exc:  # sqlite3.Error or anything else
        # DB write failed: transaction rolled back (no success record) and the
        # success receipt is withheld; the sender will time out / fail.
        print(f"receiver: DB commit failed for {peer_str} ({exc!r}); receipt withheld")
        await _close_writer(writer)
        return
    receipt["receipt_id"] = f"rx-{row_id}"

    # The record is durable from here on: even if this receipt never reaches
    # the sender or the process restarts, /receipts can find it by digest and
    # byte count.
    try:
        payload = (json.dumps(receipt, ensure_ascii=False) + "\n").encode()
        writer.write(payload)
        await writer.drain()
        if writer.transport.can_write_eof():
            writer.write_eof()
        # Give the peer a moment to read the receipt before full close.
        await asyncio.sleep(0.05)
    except (ConnectionError, asyncio.CancelledError, RuntimeError, OSError) as exc:
        # Receipt delivery failed (e.g. the proxy force-closed the link):
        # the committed record is unaffected.
        print(f"receiver: receipt {receipt['receipt_id']} for {peer_str} not delivered ({exc!r})")
    finally:
        await _close_writer(writer)


# --------------------------------------------------------------------------- #
# HTTP API (FastAPI, /docs available)
# --------------------------------------------------------------------------- #


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()  # refuse to start if receipts cannot be persisted
    tcp_server = await asyncio.start_server(handle_conn, LISTEN_HOST, TCP_PORT)
    app.state.tcp_server = tcp_server
    print(f"receiver: tcp on {LISTEN_HOST}:{TCP_PORT}, db={DB_PATH}")
    try:
        yield
    finally:
        tcp_server.close()
        await tcp_server.wait_closed()


app = FastAPI(
    title="采样包接收端 (Local Receiver)",
    description=(
        "读到上传 FIN 后计算字节数与 SHA-256，**先提交 SQLite 再发送回执**；"
        "未收到 FIN 或写库失败则不留成功记录、不发成功回执。"
        "回执记录持久化（数据卷），重启后仍可按摘要与字节数查询；"
        "同内容多次上传保留为不同编号的多条记录。"
    ),
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "db": DB_PATH,
        "tcp_port": TCP_PORT,
        "receipts": count_receipts(),
        "started_at": STARTED_AT,
    }


@app.get("/receipts")
def list_receipts(
    sha256: Optional[str] = Query(
        default=None, description="按上传内容 SHA-256（64 位十六进制）精确过滤"
    ),
    bytes_: Optional[int] = Query(
        default=None, alias="bytes", ge=0, description="按字节数精确过滤"
    ),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
) -> list[dict]:
    """最新在前。可用 `sha256` 与 `bytes` 组合过滤，例如判断某采样包是否已完整收到。

    同内容多次上传会返回多条 `receipt_id` 不同的记录。
    """
    return query_receipts(sha256=sha256, byte_count=bytes_, limit=limit)


@app.get("/receipts/{receipt_id}")
def receipt_by_id(receipt_id: str) -> dict:
    """按编号（如 rx-42）查询单条回执。"""
    rec = get_receipt(receipt_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="unknown receipt id")
    return rec


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=LISTEN_HOST, port=HTTP_PORT)
