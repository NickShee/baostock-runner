# baostock-runner

面向 NAS/Docker 的 BaoStock 数据服务：以 Streamable HTTP 提供 MCP 工具，后台线程把股票数据分批次下载到本地 SQLite 形成持久缓存，MCP 读取优先命中本地、不重复消耗 BaoStock 配额。

外部 MCP 请求可以并发到达，但 BaoStock 访问始终由**同一个 Python 进程内的单 worker、单连接、串行队列**执行（BaoStock 官方限制：非线程安全，多会话直连有封号风险）。

## 整体架构

```text
┌─────────────┐   Streamable HTTP   ┌──────────────────────────────┐
│ MCP 客户端   │ ──────────────────► │  FastMCP (server.py)          │
│ LobeHub/CLI │  /mcp               │  • get_stock_daily_bars 等工具 │
└─────────────┘                     │  • 优先读 SQLite 命中即返回    │
                                    └──────────────┬───────────────┘
                                                   │ 强制走唯一队列
                                    ┌──────────────▼───────────────┐
                                    │  Gateway (gateway.py)         │
                                    │  • 单 worker 串行执行          │
                                    │  • 会话保活/失败重连/登录验证   │
                                    │  • 参数缓存 + 每日软/硬预算     │
                                    │  • fetcher 独立 download_usage │
                                    └──────┬──────────────┬────────┘
                                           │              │
                               BaoStock API│              │写入事实表
                                           ▼              ▼
                                 ┌──────────────────────────────────┐
                                 │  SQLite: /data/baostock.sqlite3    │
                                 │  daily_bars/financials/... 共 13 表 │
                                 │  bind → NAS SSD /volume3/docker-   │
                                 │  data/baostock-runner/data         │
                                 └──────────────────────────────────┘
                                    ▲
                                    │ 后台 fetcher 线程（预算隔离 ≤ 2/3）
                                    │ 证券池初始化 → 日线回填 → 财务回填 → 空闲轮询
```

数据流：`BaoStock → fetcher/gateway（单队列）→ SQLite 事实表 → MCP 读取工具命中本地`。审计日志 `baostock-audit.jsonl` 记录每次请求。

### 优先级队列（MCP 查询插队）

`gateway.jobs` 使用 `queue.PriorityQueue`：**MCP 查询（priority=0）总排到后台 fetcher（priority=1）前面**，同优先级按入队顺序 FIFO。

- fetcher 每个请求标记 `priority=1`（`_bounded_call` / `daily_bars` 透传）。
- worker 完成当前请求后优先处理 MCP 请求，再继续 fetcher——**MCP 实时查询不被后台回填阻塞**，无需暂停 fetcher 或把下载挪到闲时。
- 生产实测：财务回填进行中查询 2013 年历史日线（需拉 BaoStock）耗时 4.3s 返回；同场景优化前排队 218s 未完成。

## 后台分批次下载器（P1）

启动后 fetcher 线程自动运行：先初始化股票池（hs300 成分 + 基本信息 + 交易日历 + 行业），然后按批次把日线/财务数据下载到本地 SQLite 事实表。所有 BaoStock 请求仍走唯一 worker 队列，与 MCP 请求共享连接与会话保活。

- 分批次：`BAOSTOCK_FETCH_BATCH_SIZE`（默认 50）只/批；每批完成后回到主循环。
- **MCP 插队优先**：队列为优先级队列，MCP 查询（priority=0）总排到 fetcher（priority=1）前面；
  后台回填进行时实时查询仍秒级响应（生产实测：fetcher 繁忙时历史查询 ~4s 返回，此前排队可卡数分钟），
  查询完成后再继续 fetcher，无需暂停/错峰。
- 断点续传：`download_jobs` 表记录每个批次状态，重启后自动跳过已完成项。
- 当日去重：日线当天已补完的股票当天不再重复拉取；跨天自动重新检查（每日增量）。
- 幂等：所有事实表复合主键 + `INSERT OR REPLACE`，可重复执行。
- **阶段推进**：初始化任务短路完成；日线 → 财务 → 分红 → 复权按轮询逐个推进，任何阶段无活不阻塞后续阶段。

### 预算隔离（不抢占 MCP 额度）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_FETCH_BUDGET_RATIO` | `0.67` | fetcher 占每日总预算 `BAOSTOCK_DAILY_HARD_LIMIT` 的比例（2/3） |
| `BAOSTOCK_FETCH_PAUSE_SLEEP_SECONDS` | `600` | 预算耗尽后的暂停检查间隔；按天计数，跨天自动恢复 |

fetcher 使用独立的 `download_usage` 计数，硬顶 = `hard_limit × ratio`；达到即**软暂停**（不抛错、不抢占），MCP 至少保留 `hard_limit × (1-ratio)`（默认 1/3）额度。`get_backfill_status` 返回预算拆分与各数据集任务统计。

### 全部 fetcher 环境变量

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_FETCH_ENABLED` | `true` | 后台下载器总开关 |
| `BAOSTOCK_FETCH_UNIVERSE` | `hs300` | 股票池：hs300（P1 实现）；all 预留 |
| `BAOSTOCK_FETCH_DAILY_START_DATE` | `2018-01-01` | 日线回填起点 |
| `BAOSTOCK_FETCH_FINANCIAL_START_YEAR` | `2022` | 财务回填起点年份（含），倒序补到当前年 |
| `BAOSTOCK_FETCH_FINANCIAL_DATASETS` | `profit` | 财务 dataset：profit,growth,balance,cash_flow,operation,dupont |
| `BAOSTOCK_FETCH_ADJUSTFLAGS` | `3` | 日线复权方式：3=不复权,1=后复权,2=前复权 |
| `BAOSTOCK_FETCH_INCLUDE_DIVIDENDS` | `false` | 是否回填分红 |
| `BAOSTOCK_FETCH_INCLUDE_ADJUST_FACTORS` | `false` | 是否回填复权因子 |
| `BAOSTOCK_FETCH_IDLE_SLEEP_SECONDS` | `300` | 全部追上最新后的空闲轮询间隔 |

新增 SQLite 事实表：`securities`、`trade_calendar`、`index_constituents`、`financials`、`dividends`、`adjust_factors`、`stock_industry`；任务表 `download_jobs`；预算表 `download_usage`；元数据表 `dataset_meta`。均为 `CREATE TABLE IF NOT EXISTS`，不影响既有表与旧镜像回滚。

## 会话保活与重连（v0.9.x+）

BaoStock 免费长连接在服务端空闲超时或波动时会静默断开。内置三层容错：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_SESSION_IDLE_TIMEOUT_SECONDS` | `1800` | 距上次查询超过该秒数，下次查询前主动重连 |
| `BAOSTOCK_RECONNECT_ON_FAILURE` | `true` | 查询失败（非黑名单）且重试耗尽后，重连一次再试 |
| `BAOSTOCK_VERIFY_AFTER_LOGIN` | `true` | 登录后用 `query_stock_basic` 轻量验证通道真实可用 |

## Worker 心跳看门狗（v0.10.x+）

gateway 单 worker 在 BaoStock 半开连接下可能卡进 `rs.next()` 用户态死循环：
数据零进展、CPU 空转、queue 中 Job 全部排队，而进程表面仍存活（healthcheck 只探 TCP 端口）。
内置"心跳自愈 + 查询级防护"双层兜底：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_WATCHDOG_ENABLED` | `true` | worker 心跳看门狗总开关 |
| `BAOSTOCK_WATCHDOG_TIMEOUT_SECONDS` | `300` | 心跳停滞超过该秒数即终止进程，由容器 `restart` 策略自动拉起 |
| `BAOSTOCK_WATCHDOG_CHECK_INTERVAL_SECONDS` | `15` | 看门狗检查间隔（最小 5s） |
| `BAOSTOCK_MAX_RESULT_ROWS` | `50000` | 单次查询结果集行数上限，防 `rs.next()` 无限累积 |

正常查询（含大数据量遍历）会持续刷新心跳，不会被误杀。

## 已修复的 Bug（实测驱动）

| # | 现象 | 根因 | 修复 |
|---|---|---|---|
| 1 | 日线回填完成后财务阶段永不启动，fetcher 空转 `idle` | `_step_once` 中 `_need_daily_backfill()` 恒真，`return _fetch_daily_batch()` 在日线无活时短路返回，后续财务判断被跳过 | 轮询类任务（日线/财务/分红/复权）逐个尝试、有活即 True；配套回归测试 |
| 2 | 财务回填对未发布季度（如当年 Q4）**反复查询同一 (code,year,quarter)** 形成死循环 | 空报告期不写 financials 行，`financial_exists` 永不命中；job 虽标 done 但跳过判断不查 job | 跳过判断同时检查 `download_jobs` 状态（已 done 即跳过）；配套回归测试 |
| 3 | gateway worker 卡进 `while rs.next()` 用户态死循环：CPU 100% 空转、数据零写入、MCP/fetcher 全部排队 | baostock 半开连接下 `rs.next()` 永不返回 False；结果集读取无行数/超时保护 | 行数上限 + 遍历超时双保险 + worker 心跳看门狗（心跳停滞自动重启容器）；配套回归测试 |
| 3 | `get_daily_pending_codes` 漏掉待补股票 | `LEFT JOIN download_jobs` 无匹配时 `NOT (...)` 求值为 NULL 被 WHERE 过滤（SQL 三值逻辑） | 加 `j.batch_id IS NULL OR` 显式放行 |
| 4 | fetcher 与 MCP 工具调用 `query_stock_basic` 报参数错误 | 真实签名是 `(code, code_name)`，**无 `status` 参数** | 按真实签名修复调用 |
| 5 | 财务配置启用 `operation`/`dupont` 时被拒 | `SUPPORTED_METHODS` 缺这两个 dataset | 补入 |

## 部署运维：设备网关长连接超时坑（自托管 LobeHub）

**现象**：在 NAS 上通过 LobeHub 设备通道执行长工具命令（`sleep 120`、长 docker 操作）时，返回 `502 Bad Gateway`；短命令（<60s）一切正常。

**根因（源码级）**：自托管 `lobehub-gateway-go` 的 `http.Server.WriteTimeout` **默认 60s**（`config.go: durationEnvOrDefault("WRITE_TIMEOUT", time.Minute)`）。tool-call 等待设备执行长命令期间，HTTP 写超时到点即**强制关闭连接**（不返回 504），反向代理（NPM）表现为 `upstream prematurely closed connection` → 502。设备守护进程实际执行成功（`RESULT OK`），但结果回传被丢弃。

**已依次排除的干扰项**（避免再踩）：NPM `proxy_read_timeout`（1d/3500s 均无效）→ 连接空闲超时（心跳 rpc 每 10s 一次全 200，通道从不空闲）→ 路由器/NAT 回流（容器 IP 直连仍断）。**与 NPM 配置、路由器、连接空闲都无关。**

**修复**：`lobehub-gateway` 容器 compose 增加环境变量并重建：

```yaml
gateway:
  environment:
    WRITE_TIMEOUT: "10m"   # 长 tool-call（>60s）不被 HTTP WriteTimeout 掐断
```

验证：120s、150s 长命令全程存活零中断（修复前 85s 必断）。守护进程重连新 gateway 后生效。

**注意**：`READ_TIMEOUT`（默认 30s，读请求体）与 `SHUTDOWN_TIMEOUT`（10s）保持默认即可；设备心跳 90s、认证 10s 为协议级常量。

## 启动 / 部署 / 验证

```bash
mkdir -p data
cp .env.example .env
# 编辑 .env：BAOSTOCK_USER_ID / BAOSTOCK_PASSWORD / BAOSTOCK_HOST_DATA_DIR（SSD 目录）
docker compose up -d --build
```

- 容器内固定使用 `/data`，实际落盘 `BAOSTOCK_HOST_DATA_DIR/baostock.sqlite3`（示例 `/volume3/docker-data/baostock-runner/data`）。不要把数据库放进镜像层。
- 登录凭据只从环境变量读取，不写入审计日志/SQLite/MCP 返回值；不要提交 `.env`。
- Docker 默认 Streamable HTTP，MCP 端点 `http://NAS_IP:8000/mcp`（多客户端共享 Gateway、队列与 BaoStock 会话）。
- 生产只运行一个 `baostock-runner` 容器实例，不要多副本/多进程直连 BaoStock。
- 开发用 `BAOSTOCK_OFFLINE=true` 免网络/免账号验证 MCP 线路。

验证命令：

```bash
docker compose ps
curl -i http://127.0.0.1:8000/mcp
# MCP 调用管理工具查看回填状态与覆盖度
get_backfill_status    # fetcher 状态、预算拆分、任务统计
get_market_coverage    # 各表行数与新鲜度
```

单测（Python 3.12 隔离环境，强制 offline，不登录真实 BaoStock、不访问生产库）：

```bash
scripts/run_tests.sh        # Docker python:3.12 隔离容器（推荐，与生产基镜像一致）
scripts/run_tests.sh -l     # 本机 Python 3.11+（需已安装依赖；测试强制 BAOSTOCK_OFFLINE=true）
# 等价于：python -m unittest discover -s tests -v
```

测试入口与生产入口严格区分：生产启动为 `python -m baostock_runner`（或 `docker compose up`）；
测试入口 `scripts/run_tests.sh` 固定强制 `BAOSTOCK_OFFLINE=true`，防止误连真实服务。
依赖精确版本见 `requirements.lock`（由 `scripts/generate_lock.sh` 在 python:3.12-slim 中生成）。

## 可用 MCP 工具

```text
get_stock_daily_bars(code, start_date, end_date, adjust, fields)
get_trade_calendar(start_date, end_date)
get_all_stocks(trade_date)
get_stock_basic(code, status, fields)
get_stock_industry(code)
get_financial_data(code, year, quarter, dataset)
get_dividend_data(code, year)
get_adjust_factor(code, start_date, end_date)
get_index_constituents(index, date)
get_latest_stock_snapshot(codes, adjust)
gateway_status()
# P1 后台下载器管理
start_backfill(dataset)
get_backfill_status()
get_market_coverage()
```

## 数据存储

- 数据库：`/data/baostock.sqlite3`（bind mount 到 NAS SSD，如 `/volume3/docker-data/baostock-runner/data/`），当前体量约 350MB+。
- 审计日志：`/data/baostock-audit.jsonl`。
- 核心事实表：`daily_bars`（按日期/代码/频率/复权分列 + 索引，日线增量补尾部）、`financials`、`dividends`、`adjust_factors`、`index_constituents`、`stock_industry`、`trade_calendar`、`securities`。

## 后续开发方向与计划

按"数据可信度优先 → 稳定性 → 可观测性 → 体验与扩展"推进：

### P2：数据新鲜度与正确性
- 参数缓存 TTL 分类型（日历/基本 30–90 天；财务/成分 1–7 天；当日日线交易时段 5–15 分钟）。
- 返回值增加 `source`（`sqlite_cache`/`baostock`）、`updated_at`、`data_as_of`、`stale`，让 Agent 能判断数据可信度。
- `refresh_stock_daily_bars` / `cache_status` / 管理员 `invalidate_cache` 工具。
- SQLite 定期备份、保留策略与完整性检查；JSONL 审计轮转。

### P3：可观测性与运维
- `/healthz`（进程/SQLite/worker）、`/readyz`（已登录且未熔断）、`/metrics`（Prometheus，可选）。
- 结构化指标：登录状态、队列长度、请求耗时、重试次数、缓存命中率、每日请求量、数据新鲜度。

### P4：体验与扩展
- MCP 响应统一 `columns + rows + metadata`；复权/报表类型枚举化；大结果分页与 `limit`。
- 复合工具 `get_stock_overview`、`compare_daily_bars`、`get_data_freshness`。
- `BAOSTOCK_FETCH_UNIVERSE=all` 全市场回填（架构已预留）；本地指标计算（均线/涨跌幅/波动率）。
- 抽象 `DataProvider` 接口，为 AkShare/Tushare 预留切换。

## 安全与合规

- 生产前必须补齐缓存 TTL/新鲜度、交易日判断后再补数、告警与备份；BaoStock 返回字段逐一核验。
- 不横向扩容、不启动多个直连 BaoStock 的进程（官方封号风险）。
- 不要将 `.env`、SQLite、JSONL 提交到版本库（`.gitignore` 已覆盖）；备份文件 `*.bak*` 不入库。
