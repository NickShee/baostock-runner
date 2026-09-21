# BaoStock 与 Tushare 数据统一存入 SQLite 的方案

## 1. 目标与总体架构

本方案用于将 BaoStock 与 Tushare 的股票数据统一存入一个 SQLite 数据库，并为后续接入 AKShare、券商行情、Agent、FastAPI、回测与筹码分析预留扩展能力。

核心数据链路：

```text
BaoStock API ──→ BaoStockAdapter ──┐
                                  ├─→ Canonical Schema ─→ SQLite ─→ Agent / FastAPI / 回测
Tushare API  ──→ TushareAdapter  ──┘
```

四条基本原则：

1. 股票代码统一转换为内部 `instrument_id`。
2. 日期、价格、成交量、成交额、市值、比例等字段统一格式与单位。
3. 不同来源的数据保留 `source`，不得简单地后写覆盖前写。
4. 上层业务只访问统一标准层，不直接依赖 BaoStock 或 Tushare 的原始字段。

---

## 2. 统一字段与单位

两套 API 中同名或近似字段的单位并不总是一致。例如：

| 数据项 | BaoStock | Tushare | 标准库 |
|---|---|---|---|
| 证券代码 | `sh.600000` | `600000.SH` | `instrument_id` |
| 日期 | `YYYY-MM-DD` | `YYYYMMDD` | `YYYY-MM-DD` |
| 成交量 | 通常为股 | `vol`，单位为手 | 股 |
| 成交额 | 通常为元 | `amount`，单位为千元 | 元 |
| 换手率 | `%` | `%` | `%` |
| 市值/股本 | 依接口定义 | 常以万元/万股计 | 元/股 |

Tushare 日线标准化示例：

```python
volume_shares = vol * 100
amount_cny = amount * 1000
trade_date = datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d")
```

建议标准库统一规定：

```text
价格        元
成交量      股
成交额      元
市值        元
股本        股
比例/收益率 %
日期        YYYY-MM-DD
时间        ISO 8601
```

对每个接口仍需按照其当期官方文档逐字段确认单位，避免仅按字段名推断。

---

## 3. 证券主数据

数据库中不应混用 `sh.600000`、`600000.SH` 和 `600000`。建议建立内部证券主键：

```sql
CREATE TABLE instrument (
    instrument_id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    asset_type TEXT NOT NULL DEFAULT 'stock',
    name TEXT,
    list_date TEXT,
    delist_date TEXT,
    currency TEXT NOT NULL DEFAULT 'CNY',
    tushare_code TEXT,
    baostock_code TEXT,
    UNIQUE(symbol, exchange)
);
```

示例：

| instrument_id | symbol | exchange | tushare_code | baostock_code |
|---:|---|---|---|---|
| 1 | 600000 | SSE | 600000.SH | sh.600000 |
| 2 | 000001 | SZSE | 000001.SZ | sz.000001 |

Tushare `stock_basic` 可作为证券主数据的主要增强来源；已有 BaoStock 股票清单可先完成内部代码映射。

---

## 4. 数据源登记

```sql
CREATE TABLE data_source (
    source TEXT PRIMARY KEY,
    priority INTEGER NOT NULL,
    description TEXT
);

INSERT INTO data_source(source, priority, description) VALUES
('baostock', 10, 'BaoStock 基础行情及指标'),
('tushare', 20, 'Tushare 行情及增强数据'),
('local_calc', 30, '本地衍生计算');
```

`priority` 用于生成“推荐值”视图，但原始标准化记录仍按来源分别保存。

---

## 5. 日线行情表

```sql
CREATE TABLE daily_price (
    instrument_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    pre_close REAL,
    volume REAL,
    amount REAL,
    pct_change REAL,
    trade_status INTEGER,
    source TEXT NOT NULL,
    source_updated_at TEXT,
    quality_flag TEXT,
    PRIMARY KEY (instrument_id, trade_date, source),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id),
    FOREIGN KEY (source) REFERENCES data_source(source)
);
```

### BaoStock 映射

| BaoStock | Canonical | 转换 |
|---|---|---|
| `code` | `instrument_id` | 查代码映射表 |
| `date` | `trade_date` | 保持 ISO 日期 |
| `open/high/low/close` | 同名字段 | 转数值 |
| `preclose` | `pre_close` | 转数值 |
| `volume` | `volume` | 确认为股 |
| `amount` | `amount` | 确认为元 |
| `pctChg` | `pct_change` | 百分比数值 |
| `tradestatus` | `trade_status` | 转整数 |

### Tushare `daily` 映射

| Tushare | Canonical | 转换 |
|---|---|---|
| `ts_code` | `instrument_id` | 查代码映射表 |
| `trade_date` | `trade_date` | `YYYYMMDD` → `YYYY-MM-DD` |
| `open/high/low/close` | 同名字段 | 转数值 |
| `pre_close` | `pre_close` | 转数值 |
| `pct_chg` | `pct_change` | 百分比数值 |
| `vol` | `volume` | `× 100`，手转股 |
| `amount` | `amount` | `× 1000`，千元转元 |

---

## 6. 每日估值与市场指标

行情与估值不要合并成一张超级宽表：两者来源、更新时点及缺失方式不同。

```sql
CREATE TABLE daily_market_metric (
    instrument_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,
    turnover_rate REAL,
    volume_ratio REAL,
    pe REAL,
    pe_ttm REAL,
    pb REAL,
    ps REAL,
    ps_ttm REAL,
    pcf_ttm REAL,
    dividend_yield REAL,
    dividend_yield_ttm REAL,
    total_shares REAL,
    float_shares REAL,
    free_float_shares REAL,
    total_market_cap REAL,
    float_market_cap REAL,
    source TEXT NOT NULL,
    source_updated_at TEXT,
    PRIMARY KEY (instrument_id, trade_date, source),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
);
```

BaoStock 可映射：

| BaoStock | Canonical |
|---|---|
| `turn` | `turnover_rate` |
| `peTTM` | `pe_ttm` |
| `pbMRQ` | `pb` |
| `psTTM` | `ps_ttm` |
| `pcfNcfTTM` | `pcf_ttm` |

Tushare `daily_basic` 可映射：

| Tushare | Canonical |
|---|---|
| `turnover_rate` | `turnover_rate` |
| `volume_ratio` | `volume_ratio` |
| `pe/pe_ttm/pb/ps/ps_ttm` | 同名字段 |
| `dv_ratio` | `dividend_yield` |
| `dv_ttm` | `dividend_yield_ttm` |
| `total_share` | `total_shares`，按接口单位换算成股 |
| `float_share` | `float_shares`，按接口单位换算成股 |
| `free_share` | `free_float_shares`，按接口单位换算成股 |
| `total_mv` | `total_market_cap`，按接口单位换算成元 |
| `circ_mv` | `float_market_cap`，按接口单位换算成元 |

---

## 7. 财务数据：期间表、指标长表与完整报表

BaoStock 更偏向财务指标，Tushare 同时提供利润表、资产负债表、现金流量表及财务指标。建议使用“完整报表项目 + 统一指标长表”的双层结构。

### 7.1 财务期间

```sql
CREATE TABLE financial_period (
    financial_period_id INTEGER PRIMARY KEY,
    instrument_id INTEGER NOT NULL,
    period_end TEXT NOT NULL,
    announcement_date TEXT,
    actual_announcement_date TEXT,
    report_type TEXT,
    company_type TEXT,
    source TEXT NOT NULL,
    UNIQUE(instrument_id, period_end, report_type, source),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
);
```

### 7.2 统一财务指标长表

```sql
CREATE TABLE financial_metric (
    financial_period_id INTEGER NOT NULL,
    metric_code TEXT NOT NULL,
    value REAL,
    unit TEXT,
    source_field TEXT,
    PRIMARY KEY (financial_period_id, metric_code),
    FOREIGN KEY (financial_period_id)
        REFERENCES financial_period(financial_period_id)
);
```

统一指标示例：

```text
revenue
net_profit
net_profit_excl_nonrecurring
roe
gross_margin
net_profit_margin
debt_ratio
current_ratio
eps_basic
operating_cash_flow
```

BaoStock 的 `roeAvg`、`npMargin`、`gpMargin` 等可分别转换为 `roe`、`net_profit_margin`、`gross_margin`；Tushare `fina_indicator` 的字段也映射到相同的内部指标代码。

### 7.3 完整财务报表项目

```sql
CREATE TABLE financial_statement_item (
    instrument_id INTEGER NOT NULL,
    period_end TEXT NOT NULL,
    announcement_date TEXT,
    report_type TEXT,
    statement_type TEXT NOT NULL,
    item_code TEXT NOT NULL,
    value REAL,
    unit TEXT,
    source TEXT NOT NULL,
    source_field TEXT,
    PRIMARY KEY (
        instrument_id, period_end, report_type,
        statement_type, item_code, source
    ),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
);
```

`statement_type` 建议限定为 `income`、`balance_sheet`、`cash_flow`；`item_code` 使用内部统一命名，例如 `revenue`、`total_assets`、`operating_cash_flow`。

应特别保留公告日、实际公告日、报告类型和公司类型，防止回测出现未来函数，并处理同一报告期的修订版数据。

---

## 8. 个股资金流

```sql
CREATE TABLE money_flow (
    instrument_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,
    buy_small_amount REAL,
    sell_small_amount REAL,
    buy_medium_amount REAL,
    sell_medium_amount REAL,
    buy_large_amount REAL,
    sell_large_amount REAL,
    buy_extra_large_amount REAL,
    sell_extra_large_amount REAL,
    net_inflow REAL,
    source TEXT NOT NULL,
    source_updated_at TEXT,
    PRIMARY KEY (instrument_id, trade_date, source),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
);
```

金额全部统一成元。以后可以把 Tushare、东方财富或同花顺口径同时存入，不能假设各数据源对“大单”的分类口径完全相同。

---

## 9. 行业与概念分类

不要在 `instrument` 中只放一个 `industry` 字段，因为申万、中信、证监会、同花顺及东方财富分类并不等价。

```sql
CREATE TABLE classification (
    classification_id INTEGER PRIMARY KEY,
    system TEXT NOT NULL,
    code TEXT,
    name TEXT NOT NULL,
    level INTEGER,
    parent_id INTEGER,
    UNIQUE(system, code),
    FOREIGN KEY (parent_id) REFERENCES classification(classification_id)
);

CREATE TABLE instrument_classification (
    instrument_id INTEGER NOT NULL,
    classification_id INTEGER NOT NULL,
    start_date TEXT NOT NULL DEFAULT '',
    end_date TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (instrument_id, classification_id, start_date, source),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id),
    FOREIGN KEY (classification_id) REFERENCES classification(classification_id)
);
```

这种设计可以保留行业归属的历史变化，并同时支持多个分类体系。

---

## 10. 事件与专项数据

高频且结构稳定的数据建议单独建表，例如：

```text
margin_daily       两融
top_list           龙虎榜
top_institution    机构席位
block_trade        大宗交易
holder_number      股东人数
```

回购、解禁、增减持、质押及分红等稀疏事件可先统一建模：

```sql
CREATE TABLE corporate_event (
    event_id INTEGER PRIMARY KEY,
    instrument_id INTEGER,
    event_type TEXT NOT NULL,
    event_date TEXT,
    announcement_date TEXT,
    value REAL,
    unit TEXT,
    description TEXT,
    source TEXT NOT NULL,
    source_id TEXT,
    raw_payload TEXT,
    UNIQUE(source, event_type, source_id),
    FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
);
```

当某一事件类型被频繁查询或字段逐渐稳定时，再从通用事件表拆成专用表。

---

## 11. 交易日历与采集日志

```sql
CREATE TABLE trading_calendar (
    exchange TEXT NOT NULL,
    calendar_date TEXT NOT NULL,
    is_open INTEGER NOT NULL,
    previous_open_date TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (exchange, calendar_date, source)
);

CREATE TABLE ingest_log (
    ingest_id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    api_name TEXT NOT NULL,
    request_params TEXT,
    fetched_at TEXT NOT NULL,
    row_count INTEGER,
    status TEXT NOT NULL,
    error_message TEXT,
    data_min_date TEXT,
    data_max_date TEXT
);
```

`request_params` 和必要的 `raw_payload` 使用 JSON 文本保存，以便追踪接口参数、排查转换错误和重新处理历史批次。

---

## 12. 双源冲突与推荐值视图

标准表主键中保留 `source`：

```text
(instrument_id, trade_date, source)
```

如果两个来源收盘价略有差异，两条记录都应保留。生产查询通过视图选择优先级更高且质量合格的数据：

```sql
CREATE VIEW daily_price_best AS
WITH ranked AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY p.instrument_id, p.trade_date
            ORDER BY
                CASE WHEN p.quality_flag IS NULL OR p.quality_flag = 'OK' THEN 0 ELSE 1 END,
                s.priority ASC
        ) AS rn
    FROM daily_price p
    JOIN data_source s ON s.source = p.source
)
SELECT * FROM ranked WHERE rn = 1;
```

可定期执行双源质量校验：

- 收盘价差异小于设定阈值：`OK`。
- 成交量或成交额差异超过阈值：`WARNING`。
- OHLC 逻辑不合法、负成交量、日期格式异常：`ERROR`。
- 停牌状态与成交量矛盾：人工复核或按规则标记。

推荐阈值应根据真实历史数据统计后确定，不宜一开始写死。

---

## 13. Adapter 标准输出

BaoStockAdapter 与 TushareAdapter 应输出相同的数据模型。例如：

```python
canonical = {
    "instrument_id": 1234,
    "trade_date": "2026-09-18",
    "open": 185.42,
    "high": 192.31,
    "low": 183.50,
    "close": 190.21,
    "pre_close": 184.29,
    "volume": 85_324_100,
    "amount": 16_034_210_000,
    "pct_change": 3.21,
    "source": "tushare"
}
```

Adapter 至少承担：

1. 证券代码映射。
2. 日期格式转换。
3. 单位换算。
4. 空字符串、`None`、`NaN` 统一为空值。
5. 数值类型转换和基本范围检查。
6. 来源、抓取时间和质量标记补充。

写入时使用参数化 SQL 与 UPSERT。UPSERT 只能更新同一个来源的同一条业务记录，不能跨来源覆盖。

---

## 14. SQLite 配置与索引

初始化连接后执行：

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
```

建议索引：

```sql
CREATE INDEX idx_daily_price_date
ON daily_price(trade_date);

CREATE INDEX idx_daily_price_instrument_date
ON daily_price(instrument_id, trade_date DESC);

CREATE INDEX idx_market_metric_instrument_date
ON daily_market_metric(instrument_id, trade_date DESC);

CREATE INDEX idx_financial_metric_code
ON financial_metric(metric_code);

CREATE INDEX idx_statement_instrument_period
ON financial_statement_item(instrument_id, period_end DESC);

CREATE INDEX idx_money_flow_date
ON money_flow(trade_date);

CREATE INDEX idx_money_flow_instrument_date
ON money_flow(instrument_id, trade_date DESC);

CREATE INDEX idx_event_instrument_date
ON corporate_event(instrument_id, event_date DESC);
```

SQLite 足以支撑约 5000 只 A 股的日频行情、财务、资金流与事件数据。建议保持单写入者、批量事务写入，并避免多个任务同时争抢写锁。

---

## 15. 推荐的数据源分工

### BaoStock：基础与历史底座

- 历史日线行情
- 基础估值指标
- 历史财务指标
- 基础指数数据

### Tushare：增强数据

- `stock_basic`
- `daily_basic`
- 利润表、资产负债表、现金流量表
- `fina_indicator`
- 个股资金流
- 两融、龙虎榜和机构席位
- 回购、解禁、股东变化、质押和大宗交易
- 申万行业及其他特色数据

两者进入同一个 Canonical Database，而不是维护两个互不兼容的业务数据库。

---

## 16. 推荐实施顺序

### 第一阶段：稳定核心行情

1. 建立 `instrument` 与代码映射。
2. 创建 `data_source`、`daily_price`、`daily_market_metric`。
3. 将现有 BaoStock 数据转换进标准表。
4. 接入 Tushare 日线和每日指标。
5. 建立双源校验与 `daily_price_best`。

### 第二阶段：财务与资金行为

1. 建立财务期间、指标长表和完整报表项目表。
2. 接入 Tushare 三大报表与 `fina_indicator`。
3. 接入 `money_flow`、两融、龙虎榜和大宗交易。
4. 严格保留公告日期，防止回测未来函数。

### 第三阶段：行业、事件和衍生指标

1. 接入多套行业/概念分类。
2. 接入回购、解禁、股东、质押等事件。
3. 在 `source='local_calc'` 下生成筹码分布及其他本地因子。
4. 对 Agent 暴露只读视图或 API，不直接开放底层原始表。

---

## 17. 最终建议的首版表清单

```text
MASTER
├── instrument
├── data_source
├── trading_calendar
├── classification
└── instrument_classification

MARKET
├── daily_price
├── daily_market_metric
├── money_flow
└── margin_daily

FUNDAMENTAL
├── financial_period
├── financial_metric
└── financial_statement_item

EVENT
├── corporate_event
├── top_list
└── block_trade

SYSTEM
└── ingest_log
```

首版控制在约 15 张核心表即可。接口增多后优先增加 Adapter 和字段映射，不要轻易改变上层统一模型。

---

## 18. 结论

最合适的路线是：

> **BaoStock 作为基础历史数据源，Tushare 作为增强源，两者经 Adapter 完成代码、日期、单位和语义标准化后，统一写入带来源标识的 SQLite Canonical Schema。**

这种设计既能避免单位混乱和覆盖冲突，也能用双源交叉校验提升数据质量。以后加入 AKShare、券商接口、本地筹码模型或实时行情时，上层 Agent、回测和 API 基本无需重构。

