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

### 优先级队列（前台优先并防止后台饥饿）

`gateway.jobs` 使用有界 `queue.PriorityQueue`：默认 MCP/dashboard/maintenance 请求优先于后台 fetcher；后台请求等待超过 60 秒后，每处理 10 个前台请求会放行一个后台请求，避免持续前台流量让回填永久饥饿。同优先级按入队顺序 FIFO。

- fetcher 每个请求标记 `priority=1`（`_bounded_call` / `daily_bars` 透传）。
- worker 完成当前请求后优先处理前台请求，再继续 fetcher；排队超过请求期限的任务会跳过。调用方执行中超时只停止等待，不能取消 BaoStock SDK 内部调用；worker 看门狗继续负责卡死恢复。
- 每个远端尝试（含失败、重试、登录及连接验证）在发起前原子计入总预算；后台调用另受独立比例上限约束。缓存命中不扣预算。请求审计包含 `origin`、`request_id`、状态与失败类型，JSONL 达到配置大小后按时间戳轮转。
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
| `BAOSTOCK_REQUEST_QUEUE_CAPACITY` | `256` | Gateway 等待队列容量；满队列快速失败 |
| `BAOSTOCK_FOREGROUND_DEADLINE_SECONDS` | `120` | MCP/dashboard/maintenance 请求期限 |
| `BAOSTOCK_BACKGROUND_DEADLINE_SECONDS` | `300` | fetcher 请求期限 |
| `BAOSTOCK_BACKGROUND_FAIRNESS_WAIT_SECONDS` | `60` | 后台请求达到此等待时间后启用老化放行 |
| `BAOSTOCK_BACKGROUND_FAIRNESS_FOREGROUND_BURST` | `10` | 每处理这么多个前台请求，最多放行一个已老化后台请求 |
| `BAOSTOCK_AUDIT_LOG_MAX_BYTES` | `20971520` | JSONL 审计单文件轮转阈值；0 关闭轮转 |
| `BAOSTOCK_FETCH_PAUSE_SLEEP_SECONDS` | `600` | 预算耗尽后的暂停检查间隔；按天计数，跨天自动恢复 |

fetcher 使用独立的 `download_usage` 计数，硬顶 = `hard_limit × ratio`；Gateway 在每次远端尝试前强制预留总预算及后台预算，fetcher 调度器发现预算用尽后软暂停。MCP 至少保留 `hard_limit × (1-ratio)`（默认 1/3）额度。`get_backfill_status` 返回预算拆分与各数据集任务统计。

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
- Gateway 状态包含 worker 存活/就绪、熔断、队列容量与忙闲状态；`gateway_status()` 对外提供状态快照，HTTP 健康路由在 E-02 接入。
- 核心事实表：`daily_bars`（按日期/代码/频率/复权分列 + 索引，日线增量补尾部）、`financials`、`dividends`、`adjust_factors`、`index_constituents`、`stock_industry`、`trade_calendar`、`securities`。

## A/B 阶段（2026-09 已实现）

- **A-01 统一时间与日历推进**：业务日/每日预算按 `Asia/Shanghai` 划日，审计时间保存 UTC；
  日历按覆盖终点触发补齐（可配置预拉窗口 `BAOSTOCK_CALENDAR_PRELOOK_DAYS`）；
  日线当日检查默认从上海时间 18:00 开始（`BAOSTOCK_DAILY_CHECK_TIME` 可配置，仅表示开始尝试）；
  盘前以最近已结束交易日为目标；切换预算日口径期间按新旧计数中较保守的已用量限制请求，不重置额度。
- **A-02 日线覆盖、字段与强制刷新**：`preclose/tradestatus/isST/peTTM/pbMRQ/psTTM/pcfNcfTTM`
  提升为可查询列（schema v2 迁移）；窄字段请求不覆盖旧值（字段级合并）；覆盖检查识别头部/中间/尾部
  缺口并合并为连续请求窗口；`get_stock_daily_bars(force_refresh=True)` 同时绕过参数缓存与事实表短路；
  覆盖结果区分 expected/effective/excluded/unknown，行情与估值字段分别统计。
- **A-03 可重查任务与财务修订**：任务状态机 `pending/running/succeeded/waiting_data/retryable_failed/permanent_failed`
  （schema v3）；空财报进入 `waiting_data` 按披露窗口重查（默认 24h）；最近两个已结束季度每日检查修订、
  其余报告每 30 天检查；网络失败指数退避（60s 起、最长 1h）；财务数据与作业完成状态同事务提交；
  进程重启回收过期 `running` 任务；分红/复权按检查窗口增量更新；旧 `done` 状态迁移为 `succeeded`。
- **B-02 请求计数与运维**：Job 带 `origin/request_id/deadline`；队列默认容量 256，前台/后台默认期限 120/300 秒；排队过期请求跳过，执行中调用方超时不声称取消 SDK；失败、重试、登录验证均按远端尝试前计数，缓存命中免费；后台预算独立封顶；等待老化避免饥饿；关停终结未执行任务；结构化 JSONL 审计按大小轮转，并提供 Gateway 健康快照（HTTP 挂载在 E-02）。
- **A-04 缓存有效期与本地读取**：参数缓存分类型 TTL（空结果/近期行情 15 分钟、历史行情 7 天、财务成分 1 天、基本行业 7 天，schema v4）；过期缓存标记 stale 重查，空结果不会永久命中；上游故障时返回本地 stale 数据并标记远端错误；旧无分类缓存到期失效不清空事实表；强制刷新（`use_cache=False`）确实触发远端。
- **C-01 按日批量接口实验工具**：类型化 Adapter 仅允许 `query_daily_history_k_AStock(date)`（全 A 股某日行情），不暴露任意远端方法入口；实验记录固定版本/签名/原始响应/字段映射/证券差集/性能报告，状态 `supported/partial/unsupported/unverified`（无真实访问必须 `unverified`）；真实实验只能走现有唯一 worker（EXT-02）。
- **D-01 最小标准模型与版本留存**：securities 增加 `asset_type/source`（schema v5），保留 code 兼容标识；`security_versions` 观察版本可追溯/重建；`data_batches` 采集批次记录；financials 增加 `pub_date/stat_date` 标准列；字段映射集中在 `standard.py`；历史未知来源标记 unknown 不补造时间。
- **C-02 每日主采集模式**：`fetch_mode` 支持 `per_stock`（默认）/`daily_batch`；`daily_batch` 按最近交易日窗口调批量接口，标准化后校验再提交（截断/重复键/未解释缺失不得标整日完成），缺估值字段追加补充任务；接口不可用降级逐股保留降级状态；未通过真实实验前仅隔离验证，不切换生产默认。
- **D-02 复权映射与版本缓存**：`FactorAdapter` 把官方返回 `dividOperateDate` 映射为统一 `effective_date`（schema v6，不再依赖错误 `date` 假设）；`AdjustmentCalculator` 按累计因子（前/后复权）直接乘原价、不默认连乘；查询起点前因子未知返回不完整（不自动填 1）；复权结果缓存键含证券/区间/口径/基准日/行情版本/因子版本/算法版本；`adjust_factor_versions` 因子版本可追溯；本地算法未通过时可用带来源版本的官方复权结果临时路径。
- **B-01 schema 版本与备份恢复**：`PRAGMA user_version` + 顺序迁移（新增表/列优先，幂等可重跑）；
  未知更高版本启动时拒绝写入；SQLite Online Backup 一致性备份与隔离恢复，完整性/行数/关键字段对账。

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
