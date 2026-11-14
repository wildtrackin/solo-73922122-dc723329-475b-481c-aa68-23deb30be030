# 申请时点收入回填服务（Point-in-time Income Backfill）

为历史贷款申请补齐 **申请当时已知** 的收入，杜绝迟到更正泄漏到训练集。
FastAPI + PostgreSQL，两个共库 API 进程协作，执行者崩溃后另一进程按原输入续算。

## 一、业务规则与数据模型

### 输入

| 数据 | 字段 |
|---|---|
| 历史申请 `applications` | `application_id`、`customer_id`、`application_time`（UTC） |
| 征信变更日志 `credit_event_versions` | `event_id`、`customer_id`、`business_time`（UTC 业务时间）、`known_time`（UTC 获知时间）、整数 `income` 或 `withdrawn=true` 撤回标记 |

- 同一 `event_id` 按 `known_time` 保留 **全部历史版本**（主键 `(event_id, known_time)`）。
- 同一 `event_id` 且同一 `known_time` 出现 **不同值** → 整个导入批次拒绝，不写入任何行；完全相同的重复行幂等忽略。
- 撤回事件不得携带收入；非撤回事件必须携带整数收入（Pydantic 校验）。
- 每个导入批次在 `import_batches` 留痕（accepted/rejected + 原因）。

### 每笔申请的回填算法（单条 SQL 完成，见 `app/worker.py`）

1. **版本还原**：只取 `known_time <= application_time` 且 `ingested_at <= 批次受理时刻`
   的事件版本——即申请时确实已知、且批次受理时确实已提交的数据；每个事件按
   `known_time DESC` 取最新版本（修订与撤回据此生效）。
2. **候选过滤**：剔除撤回版本，剔除 `business_time > application_time`（业务上尚未发生）的事件。
3. **选最近项**：`business_time` 最大者胜；同一业务时刻按 `event_id` 升序取首项。
4. 无任何候选 → 收入留空（`income = NULL`，`basis = no_known_event`）。
5. 结果注明来源：`source_event_id`、`source_version_known_time`、`source_business_time`、
   `source_ingest_batch_id`。

### 不可变批次

- 创建批次时记录 `frozen_ingest_at` 受理栅栏，并把申请清单快照到 `batch_items`。
- 批次计算期间导入照常进行；**更正或尚未提交的导入不会混进旧批次**（双重栅栏
  `known_time` + `ingested_at`）。批次完成后结果永不变化，新批次才能看到新数据。
- 批次全部 `COMPLETED` 后才开放分页结果；`PENDING/RUNNING/FAILED` 查询返回 409。

## 二、并发与续算（两个共库进程）

每个 uvicorn 进程既提供 HTTP API，又运行一个 worker 循环，全部协调状态在 PostgreSQL 中：

- 批次/分块认领用 `SELECT ... FOR UPDATE SKIP LOCKED`，保证同一时刻只有一个执行者。
- 租约 `lease_expires_at` + 心跳续租：执行者被 `SIGKILL` 后心跳停止，租约过期，
  另一进程认领过期批次/分块，**按相同的固化输入续算**；已 `DONE` 的项不重算。
- 重启非执行者：它无租约，不会干扰活跃批次。
- 分块失败自动退回 `PENDING` 重试，尝试次数持久化在 `batch_items.attempts`，
  超过 `MAX_CHUNK_ATTEMPTS` 标记 `FAILED`；修复后 `POST .../resume` 从断点续算。
- 结果由主键保证不重复；未完成不开放，因此不漏算、不重复。

## 三、启动（Docker Compose）

```bash
docker compose up --build
```

- API 进程 1：http://localhost:8001 ，Swagger 文档 http://localhost:8001/docs
- API 进程 2：http://localhost:8002 ，Swagger 文档 http://localhost:8002/docs
- PostgreSQL：localhost:55432（库 `riskdb`，用户/密码 `postgres/postgres`）

compose 中 `CHUNK_SIZE=2`、`CHUNK_DELAY_SECONDS=1` 是演示参数，刻意放慢处理以便观察
重启续算；生产环境把 `CHUNK_DELAY_SECONDS` 设为 `0`、`CHUNK_SIZE` 调大。

## 四、/docs 演示流程

打开 http://localhost:8001/docs ，按下列顺序操作（也可直接跑脚本）：

### 1. 导入虚构数据

```bash
# 5 笔申请 + 8 个事件版本（含修订、撤回、同刻并列）
curl -X POST localhost:8001/api/v1/imports/applications \
  -H 'Content-Type: application/json' -d @data/seed_applications.json
curl -X POST localhost:8001/api/v1/imports/credit-events \
  -H 'Content-Type: application/json' -d @data/seed_events.json
```

### 2. 整批拒绝演示

```bash
# 同 event_id+known_time 两个不同收入 -> 409，无任何行落库
curl -i -X POST localhost:8001/api/v1/imports/credit-events \
  -H 'Content-Type: application/json' -d @data/conflict_events.json
```

### 3. 生成批次并观察两进程协作

```bash
BID=$(curl -s -X POST localhost:8001/api/v1/batches \
  -H 'Content-Type: application/json' \
  -d '{"application_ids":["A1","A2","A3","A4","A5"]}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["batch_id"])')
# 两个端口都能查到状态，worker 身份在 /health
curl localhost:8001/api/v1/batches/$BID
curl localhost:8002/health
```

### 4. 查询时点结果（keyset 分页）

```bash
curl "localhost:8001/api/v1/batches/$BID/results?limit=2"
# 用响应中的 next_cursor 翻页
```

期望结果（虚构数据集的含义）：

| 申请 | 收入 | 来源 | 说明 |
|---|---:|---|---|
| A1 (01-11) | 50000 | E1 @known 01-02 | E1 的 01-12 修订晚于申请，不可见 |
| A2 (02-20) | 空 | — | 唯一事件 E3 已于 02-10 撤回 |
| A3 (02-20) | 空 | — | E4 业务时间 03-01 在未来；E5* 业务时间 03-05 也在未来 |
| A4 (02-20) | 60000 | E2 | E2 业务时间 02-15 新于 E1 |
| A5 (03-10) | 30000 | E5A | E5A/E5B 同业务时刻，按事件 ID 取 E5A |

### 5. 迟到更正不影响已完成批次

```bash
curl -X POST localhost:8002/api/v1/imports/credit-events \
  -H 'Content-Type: application/json' -d @data/late_correction.json
curl "localhost:8001/api/v1/batches/$BID/results"   # A1 仍是 50000
# 更正之后新建的批次才能看到 51000
```

### 6. 重启续算演示

```bash
bash scripts/restart_demo.sh          # Docker 环境：重启非执行者 + SIGKILL 执行者
```

脚本会生成 60 笔数据的大批次，重启非执行者确认批次不受干扰，再 `docker compose kill`
执行者，验证存活进程在租约过期后接管并完成，最后断言无遗漏、无重复、来源完整。

## 五、脚本与测试

| 脚本 | 作用 |
|---|---|
| `scripts/seed.py` | 导入 `data/` 下的虚构申请与事件 |
| `scripts/demo.py` | 端到端演示（导入、拒绝、时点结果、迟到更正隔离、分页、409 闸门） |
| `scripts/gen_bulk.py` | 经 API 生成大批量虚构数据 |
| `scripts/restart_demo.sh` | Docker 下的重启/故障转移演示 |
| `scripts/lease_failover_test.py` | 不依赖 Docker 的租约接管测试（以 SQL 令租约过期精确模拟 SIGKILL 后的状态） |
| `scripts/test_pit_sql.py` | 时点重建 SQL 的 10 项边界单元测试（修订/撤回/未来事件/并列/栅栏/空值） |

无 Docker 的本地验证方式（需要一个 PostgreSQL 并设置 `DATABASE_URL`）：

```bash
pip install -r requirements.txt
# 启动两个共库进程
DATABASE_URL=postgresql://... uvicorn app.main:app --port 8001 &
DATABASE_URL=postgresql://... uvicorn app.main:app --port 8002 &
python3 scripts/demo.py
DATABASE_URL=postgresql://... python3 scripts/test_pit_sql.py
DATABASE_URL=postgresql://... python3 scripts/lease_failover_test.py \
    http://127.0.0.1:8001 http://127.0.0.1:8002 postgresql://... 40
```

## 六、API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/imports/applications` | 整批事务导入申请，重复 ID 全拒绝 |
| POST | `/api/v1/imports/credit-events` | 整批事务导入事件版本，同键异值全拒绝 |
| POST | `/api/v1/batches` | 生成回填批次（固化受理时输入） |
| GET | `/api/v1/batches` / `/api/v1/batches/{id}` | 批次状态 |
| POST | `/api/v1/batches/{id}/resume` | FAILED 批次断点续算 |
| GET | `/api/v1/batches/{id}/results?limit=&cursor=` | 完成后分页查询结果 |
| GET | `/api/v1/applications` / `/api/v1/events` | 已导入数据检查 |
| GET | `/health` | 进程与 worker 身份 |

所有时间字段必须带时区（ISO 8601，推荐 `...Z`），服务端统一转 UTC 存储。
