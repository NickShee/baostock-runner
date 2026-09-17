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
```

## 会话保活与重连（v0.9.x+）

BaoStock 免费长连接在服务端空闲超时或波动时会静默断开。本版本内置三层容错：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `BAOSTOCK_SESSION_IDLE_TIMEOUT_SECONDS` | `1800` | 距上次查询超过该秒数，下次查询前主动重连 |
| `BAOSTOCK_RECONNECT_ON_FAILURE` | `true` | 查询失败（非黑名单）且重试耗尽后，重连一次再试 |
| `BAOSTOCK_VERIFY_AFTER_LOGIN` | `true` | 登录后用 `query_stock_basic` 轻量验证通道真实可用 |

## 生产前必须补强

这是雏形，不应直接作为高并发生产数据服务。上线前应增加缓存 TTL/数据新鲜度策略、交易日判断后再补数、管理员告警、SQLite 备份，以及对 BaoStock 返回字段逐一核验。不要横向扩展该容器或启动多个直接访问 BaoStock 的实例。
