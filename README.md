# baostock-runner

一个面向 NAS/Docker 的 BaoStock MCP 雏形。外部 MCP 请求可以通过 Streamable HTTP 并发到达，但 BaoStock 访问始终由同一个 Python 进程内的单 worker、单连接、串行队列执行。

## 当前能力

- MCP 高层工具：日线、交易日历、证券列表、股票基本信息、行业分类、利润/成长/资产负债/现金流财务数据、分红、复权因子、沪深 300/上证 50/中证 500 成分股、限量批量快照；不暴露 `login/logout`。
- SQLite 参数缓存和 JSONL 审计日志。
- 日线事实数据按日期、代码、频率和复权方式分列存储，并建立代码/日期索引；MCP 返回使用紧凑的 `columns + rows` JSON。
- 单连接生命周期管理、请求间隔、每日软/硬预算。
- 错误码 `10001011` 熔断，不自动重试。
- `BAOSTOCK_OFFLINE=true` 可在无网络/无账号环境验证 MCP 线路。
- 日线写入本地 `daily_bars` 表；再次请求相同股票/周期/复权方式时只补本地缺失的尾部日期。
- 会话保活与失败重连：空闲超过阈值主动重连，失败重试耗尽后重连一轮，登录后轻量验证通道。

## 启动

```bash
mkdir -p data
cp .env.example .env
# 编辑 .env，填写 BAOSTOCK_USER_ID、BAOSTOCK_PASSWORD 和 SSD 数据目录
docker compose up -d --build
```

如果 NAS 上的 SSD 数据目录是 `/volume3/docker/baostock-runner/data`，设置：

```env
BAOSTOCK_HOST_DATA_DIR=/volume3/docker/baostock-runner/data
```

容器内固定使用 `/data`，数据库实际文件为 `/volume3/docker/baostock-runner/data/baostock.sqlite3`。不要把数据库目录放进镜像层。

登录凭据只从环境变量读取，不会写入审计日志、SQLite 或 MCP 返回值。两个变量必须同时设置；如果都为空，则使用 BaoStock 客户端默认登录方式。不要将 `.env` 提交到版本库。

Docker 默认启动 Streamable HTTP，MCP 端点为：

```text
http://NAS_IP:8000/mcp
```

MCP 客户端应配置为连接这个 URL，而不是为每个客户端执行一次 `docker exec`。这样多个客户端会共享同一个 Gateway、队列和 BaoStock 会话。

本地调试仍可切换为 stdio：

```json
{
  "mcpServers": {
    "baostock": {
      "command": "docker",
      "args": ["exec", "-i", "baostock-runner", "env", "BAOSTOCK_MCP_TRANSPORT=stdio", "python", "-m", "baostock_runner"]
    }
  }
}
```

生产环境只运行一个 `baostock-runner` 容器实例；不要配置多个副本或多个直接访问 BaoStock 的进程。MCP SDK 的 Streamable HTTP 服务由单个进程监听端口，客户端请求会共享进程内的 `queue.Queue`。

开发时可用 `BAOSTOCK_OFFLINE=true` 运行。真实数据示例：`sh.600000`、`sz.000001`；日期格式为 `YYYY-MM-DD`。

可用 MCP 工具：

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
# P1 后台下载器管理工具
start_backfill(dataset)
get_backfill_status()
get_market_coverage()
```

## 后台分批次下载器（P1）

启动后 fetcher 线程自动运行：先初始化股票池（hs300 成分 + 基本信息 + 交易日历 + 行业），
然后按批次把日线/财务数据下载到本地 SQLite 事实表，MCP 读取工具优先命中本地数据。
所有 BaoStock 请求仍走唯一 worker 队列（官方限制：BaoStock 非线程安全），与 MCP 请求共享连接。

- 分批次：`BAOSTOCK_FETCH_BATCH_SIZE`（默认 50）只/批；每批完成后回到主循环，便于预算/心跳控制。
- 断点续传：`download_jobs` 表记录每个批次状态，重启后自动跳过已完成项。
- 当日去重：日线当天已补完的股票当天不再重复拉取；跨天自动重新检查（每日增量）。
- 幂等：所有事实表复合主键 + `INSERT OR REPLACE`，可重复执行。
- 会话保活：与 MCP 请求共享 gateway 的会话新鲜检查/失败重连。

### 预算隔离（不抢占 MCP 额度）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_FETCH_BUDGET_RATIO` | `0.67` | fetcher 占每日总预算 `BAOSTOCK_DAILY_HARD_LIMIT` 的比例（2/3） |
| `BAOSTOCK_FETCH_PAUSE_SLEEP_SECONDS` | `600` | 预算耗尽后的暂停检查间隔；按天计数，跨天自动恢复 |

fetcher 使用独立的 `download_usage` 计数，硬顶 = `hard_limit × ratio`；达到即**软暂停**
（不抛错、不抢占），MCP 至少保留 `hard_limit × (1-ratio)`（默认 1/3）额度。
`get_backfill_status` 返回预算拆分与各数据集任务统计。

### 其他 fetcher 环境变量

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

新增 SQLite 事实表：`securities`、`trade_calendar`、`index_constituents`、`financials`、
`dividends`、`adjust_factors`、`stock_industry`；任务表 `download_jobs`；预算表 `download_usage`；
元数据表 `dataset_meta`。均为 `CREATE TABLE IF NOT EXISTS`，不影响既有表与旧镜像回滚。

## 会话保活与重连（v0.9.x+）

BaoStock 免费长连接在服务端空闲超时或波动时会静默断开。本版本内置三层容错：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_SESSION_IDLE_TIMEOUT_SECONDS` | `1800` | 距上次查询超过该秒数，下次查询前主动重连 |
| `BAOSTOCK_RECONNECT_ON_FAILURE` | `true` | 查询失败（非黑名单）且重试耗尽后，重连一次再试 |
| `BAOSTOCK_VERIFY_AFTER_LOGIN` | `true` | 登录后用 `query_stock_basic` 轻量验证通道真实可用 |

## 生产前必须补强

这是雏形，不应直接作为高并发生产数据服务。上线前应增加缓存 TTL/数据新鲜度策略、交易日判断后再补数、管理员告警、SQLite 备份，以及对 BaoStock 返回字段逐一核验。不要横向扩展该容器或启动多个直接访问 BaoStock 的实例。
