# TCP 故障代理 / 采样包上传卡顿复现平台

设备接入测试场景：复现 32 MiB 采样包上传卡顿，并判断超时发生在**上传阶段**还是
**回执阶段**；回执链路暂停导致发送器超时、接收端随后重启时，值班员可凭**发送器公开
的摘要**在接收端持久化回执库中判断采样包是否已完整收到。

- **proxy**：FastAPI + asyncio 单进程 TCP 故障代理，固定转发到本地接收端，可按连接、
  按方向在累计转发 N 字节后**暂停**，支持查询、恢复、强断。
- **receiver**：读到上传 FIN 才算全量，先**提交 SQLite** 再发送回执；回执记录落在
  数据卷上，重启后仍可按摘要+字节数查询；同内容多次上传保留为不同编号记录。
- **sender**：进入回执等待**之前**即公开本次 SHA-256 摘要；只有收到摘要与字节数都
  匹配的有效回执才标记成功，否则一律 `failed`。

## 架构

```
 sender (FastAPI)  ──TCP:9000──▶  proxy (asyncio 转发 + FastAPI 控制面:8080)  ──▶  receiver (asyncio:9100)
   /docs 发起上传                    /docs 预设暂停/查询/恢复/强断                    收 FIN→算 SHA-256→先提交 SQLite
   32MiB 后 write_eof                每方向独立 pump 任务                            再发回执 JSON，反向半关闭
   回执等待前公开摘要                 回执查询 + /docs HTTP:9101                     回执落数据卷，重启可查
   无有效回执→failed
```

三个组件各一个容器（`docker compose up`），也可在本机直接用脚本起三个进程。

| 组件 | 端口 | 说明 |
| --- | --- | --- |
| receiver | TCP 9100 / HTTP 9101 | 收到 FIN 才算全量；**先提交 SQLite 再发回执**；`/receipts` 按摘要+字节数查询（持久化），`/docs` 操作入口 |
| proxy | TCP 9000 / HTTP 8080 | 故障转发入口；8080 为控制 API 与 `/docs` |
| sender | HTTP 8081 | `/docs` 发起上传、查看 run 与回执；回执等待前公开摘要 |

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 三个 /docs 操作入口
open http://127.0.0.1:8080/docs    # 故障代理：预设/连接/恢复/强断
open http://127.0.0.1:8081/docs    # 发送器：发起上传、查看 run 与摘要
open http://127.0.0.1:9101/docs    # 接收端：按摘要/字节数查持久化回执
# 命令行查询示例
curl "http://127.0.0.1:9101/receipts?sha256=<64hex>&bytes=33554432"
```

接收端回执库在命名数据卷 `receiver-data`（容器内 `/data/receipts.db`，WAL 模式、
`synchronous=FULL`）。`docker compose restart receiver` 或重建容器后，已提交回执
仍可按摘要和字节数查询。数据库不可写时接收端**拒绝启动**（宁可不可用，不可无记录
却发成功回执）。

无 Docker 时（本机已有 Python 3.11）：

```bash
scripts/run-local.sh        # 建 venv、装依赖、起三个进程（日志 /tmp/fp-logs，回执库 .data/receipts.db）
scripts/stop-local.sh       # 停止
python -I tests/smoke_test.py   # 端到端冒烟（52 项检查）
```

## 值班员操作流：回执链路暂停 + 发送器超时 + 接收端重启

1. 发送器 `/docs` 调 `POST /send` 发起上传，拿到 `run_id`。
2. 代理 `/docs` 设 `PUT /preset` `r2c_pause_after=16`（回执方向 16 字节后暂停），
   模拟回执链路卡住；发送器 run 停在 `waiting-receipt`，直至
   `SENDER_RECEIPT_TIMEOUT`（默认 120s）超时置 `failed`。
3. **此时 run 里已有 `upload_sha256`**（进入回执等待前即公开）。值班员凭它查接收端：
   `GET /receipts?sha256=<upload_sha256>&bytes=<size_bytes>`。
   - 有记录 ⇒ 采样包**已完整收到并持久化**（接收端先提交后发回执，回执没送达不影响记录）；
   - 无记录 ⇒ 未完整收到（接收端未收全 FIN 前绝不提交）。
4. 即使此时 `docker compose restart receiver`，重启后同一查询仍返回该记录
   （数据卷持久化）；同内容再次上传会产生**新的** `receipt_id`，互不覆盖。
5. 发送器侧：无有效回执（缺失/损坏/摘要或字节不符）run 一律 `failed`，绝不标记成功。

## 复现卡顿并定位超时阶段（标准操作流）

1. 在代理 `/docs` 调 `PUT /preset`，设 `c2r_pause_after=1048576`（上传 1 MiB 后暂停）。
2. 在发送器 `/docs` 调 `POST /send`（默认 32 MiB），拿到 `run_id`。
3. `GET /connections` 看到连接 `c2r` 方向 `paused=true`、`forwarded_bytes=1048576`、
   `fin` 未送达；发送器 run 停在 `connected`（drain 被 TCP 背压阻塞）——**说明超时在上传阶段**。
4. `POST /connections/{id}/resume`（`direction=c2r`），积压与 FIN 继续，接收端出回执，
   run 变 `done`，`hash_match=true`。
5. 要定位**回执阶段**超时：`PUT /preset` 设 `r2c_pause_after=16`，此时上传正常完成
   （`c2r.half_closed_delivered=true`），run 停在 `waiting-receipt`，恢复 `r2c` 后才收到回执。
6. `POST /connections/{id}/force-close` 可随时强断，立即打断暂停与阻塞写。

## 控制 API（proxy:8080）

| 方法/路径 | 作用 |
| --- | --- |
| `GET /health` | 健康状态、转发目标、每方向用户态缓冲上限（256 KiB） |
| `GET /preset` / `PUT /preset` | 查看/预设**新连接**每方向暂停阈值（`null` 取消） |
| `GET /connections` | 全部连接（活动在前、已关闭在后），含每方向转发字节、积压、暂停/FIN 状态 |
| `GET /connections/{id}` | 单连接详情 |
| `POST /connections/{id}/pause` | 手动暂停某方向（阈值之外的测试手段） |
| `POST /connections/{id}/resume` | 恢复指定方向；不传 direction 则恢复全部暂停方向 |
| `POST /connections/{id}/force-close` | 强断：abort 两端、取消 pump，任务退出且两端关闭后才返回 |

连接状态字段（每方向）：`forwarded_bytes`（已转发）、`pending_bytes`（暂停时暂扣的积压）、
`paused`、`fin_pending`、`half_closed_delivered`、`eof_seen`、`peer_closed` 等。

## 接收端 API（receiver:9101，含 /docs）

| 方法/路径 | 作用 |
| --- | --- |
| `GET /health` | 健康状态、DB 路径、回执总数、`started_at`（进程启动时间，可判断重启） |
| `GET /receipts` | 最新在前；`sha256`、`bytes` 精确过滤，`limit` 分页（默认 500） |
| `GET /receipts/{receipt_id}` | 按编号（如 `rx-42`）查单条 |

回执字段：`receipt_id`（`rx-<行号>`，单调递增）、`peer`、`bytes`、`sha256`、`chunks`、
`read_seconds`、`eof_seen`、`received_at`。

## 发送器行为（sender:8081，含 /docs）

- `POST /send` 后台上传，`GET /sends/{run_id}` 查状态。
- run 阶段：`connected`（上传中，可被背压阻塞）→ `uploaded-half-closing` →
  `waiting-receipt` → `done` / `failed`。
- **`upload_sha256` 在进入 `waiting-receipt` 前即写入 run**，供值班员交叉查询接收端。
- 仅当收到有效回执（JSON 可解析、`sha256` 与本次摘要一致、`bytes` 与本次大小一致）
  才置 `done`；回执缺失、损坏或不匹配一律 `failed` 并注明原因。

## 关键正确性设计与保证

### 接收端（回执持久化）

- **FIN → 提交 → 发送，顺序不可颠倒**：读到 EOF（FIN）算出字节数与 SHA-256 后，
  先 `INSERT` 并 `COMMIT`（WAL + `synchronous=FULL`，提交即 fsync），提交成功才发送
  回执 JSON。提交后即使回执未送达或进程/容器重启，`/receipts` 仍可查。
- **未收到 FIN 不留痕迹**：读循环中连接复位/取消（含 `ConnectionResetError`、
  `OSError`）直接关闭，不提交、不回执（冒烟场景 9 用 `SO_LINGER=0` 发 RST 验证）。
- **写库失败不发成功回执**：提交失败事务回滚（不留成功记录），连接静默关闭，
  发送器将超时置 `failed`；数据库不可用时接收端启动即失败（fail-fast）。
- **同内容多次上传**：`sha256` 上无唯一约束，每次上传生成新行、`receipt_id`
  （`rx-<行号>`）各不相同。
- 回执发送失败（如代理强断回执方向）只影响本次投递，已提交记录不受影响，
  日志注明 `not delivered`。

### 代理（保持不变）

- **恰好停在 N 字节（跨读取块）**：每次 `read(64KiB)` 后按阈值切分，`head` 转发、
  `tail` 作为该方向唯一的用户态积压（`pending`）；暂停期间不再读 socket。
- **积压与 FIN 不越过暂停**：暂停时不做任何读和 FIN 投递；读到 EOF 也只标记
  `eof_seen/fin_pending`，等恢复后先把积压发完再 `write_eof()`。
- **恢复后不丢、不重、不乱序**：恢复后先 flush `pending` 再继续读，字节顺序唯一；
  测试以双端 SHA-256 与字节数比对验证。
- **背压、无整包缓存、不落盘**：暂停即停读 → 内核接收缓冲填满 → TCP 流控反压到发送端，
  发送端阻塞在 `drain()`；每方向用户态数据 = StreamReader 缓冲（上限 256 KiB）+
  至多一个切分尾块，全程不写磁盘。
- **FIN 半关闭语义**：FIN 等本方向积压清空后才向对端半关闭；反向 pump 是独立任务，
  回执方向不受影响，可一直读到 EOF。
- **强断语义**：`transport.abort()` 让阻塞的 `drain()`/`read()` 立即抛错，两个 pump
  被取消并退出，supervisor 关闭两端 socket、连接从活动列表移除后 API 才报完成；
  其他连接与控制 API 不受影响（冒烟测试覆盖）。

## 测试

`tests/smoke_test.py`（纯标准库）覆盖：

1. 基线哈希一致；
2. 自动暂停精确停 N、积压/FIN 暂扣、背压阻塞、恢复后哈希一致；
3. 奇数 N 跨块精确停点（裸 socket 客户端）；
4. 手动暂停/恢复；
5. 回执方向暂停：上传照常半关闭、**回执等待前摘要已公开**、恢复后回执送达且与摘要一致；
6. 强断与隔离性；
7. 背压下用户态积压 ≤ 256 KiB；
8. **回执可按 sha256+bytes 查询**；同内容两次上传保留为不同 `receipt_id` 的两条记录；
9. **无 FIN（RST 中止）不留记录、不发回执**；
10. **回执先提交后送达**：回执方向暂停中记录已可查；强断回执链路后发送器 `failed`
    （无有效回执绝不成功），已提交记录仍在；
11. （可选，设 `RECEIVER_RESTART_CMD`，如 `docker compose restart receiver`）
    **重启持久化**：进程确实重启（`/health` 的 `started_at` 变化）、记录仍在、
    重启后新上传正常。

可用环境变量 `PROXY_API/SENDER_API/RECEIVER_API/PROXY_TCP/RECEIVER_TCP` 指向 Compose
暴露的端口直接对容器跑。

```bash
python -I tests/smoke_test.py
# 52/52 checks passed（无 RECEIVER_RESTART_CMD 时跳过场景 11）
RECEIVER_RESTART_CMD="docker compose restart receiver" python -I tests/smoke_test.py
# 57/57 checks passed
```

## 实测结果（本环境，Python 3.11.2 / loopback）

环境无 Docker 守护进程，故用等价的三进程本地部署验证（Compose 文件已就绪，逻辑与
容器部署一致，仅主机名改为 127.0.0.1）：

- 冒烟套件 **57/57 通过**（含真实接收端重启：触发器使接收端进程自我终止后由监督
  进程重生，`/health` 的 `started_at` 证实为新进程，重启前已提交回执逐条可查，
  重启后新上传正常）。不带重启变量的运行为 52/52（场景 11 跳过）。
- 回执中断：回执方向暂停 16 字节时，接收端记录**已提交可查**而发送器仍在
  `waiting-receipt`；强断链路后发送器 `failed`（`no valid receipt`），记录保留。
- 重启持久化：另用硬退出（`os._exit`，比 `docker restart` 更严苛）验证——独立
  sqlite3 客户端确认记录已落盘，新进程同库启动后按 sha256+bytes 查到原记录，
  新上传编号递增（rx-1 → rx-2）。
- 写库失败：DB 目录只读时，收到 FIN 也**不发回执、不留记录**（连接静默关闭）；
  恢复可写后回执流程正常；DB 路径不可用时接收端启动即失败。
- 精确停点：N=300000、100007（奇数跨块）等均实测 `forwarded_bytes == N`，
  暂停期间计数冻结、FIN 不送达。
- 背压：16 MiB 载荷在 N=300000 暂停后，发送器实际卡在 `drain()`，代理
  `pending_bytes` 峰值约 62 KiB，全程 **≤ 256 KiB**，接收端无回执。
- 恢复完整性：恢复后字节数与双端 SHA-256 全部一致。
- 强断：暂停中的连接 `force-close` 约 20–40 ms 返回，发送端收到连接复位，
  连接从活动列表移除，控制 API 与后续新连接不受影响。

> 说明：发送器对回执读取带 `SENDER_RECEIPT_TIMEOUT`（默认 120s）超时保护，
> 回执阶段长期无 EOF 时 run 置 `failed` 并注明 receipt-phase stall，而非永久挂起。

## 目录

```
proxy/app.py       故障代理（asyncio 双向 pump + FastAPI 控制面）
sender/app.py      发送器（确定性 32MiB 采样流、半关闭、回执等待前公开摘要、有效回执才成功）
receiver/app.py    接收端（FIN→算 SHA-256→先提交 SQLite→再发回执；/receipts 持久化查询 + /docs）
tests/smoke_test.py 端到端冒烟（52 项，加重启场景 57 项）
scripts/           本机启停脚本
Dockerfile docker-compose.yml requirements.txt   # compose 含 receiver-data 持久化数据卷
```
