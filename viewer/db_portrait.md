# 数据库画像（DB Portrait）

本文档用于快速理解当前项目的 DuckDB 数据库边界、表结构和 viewer 查询行为。

## 数据库文件

项目默认使用两个本地 DuckDB 文件：

| 数据库 | 资产范围 | 说明 |
| --- | --- | --- |
| `stock_data.db` | 股票 | 只保存 `stock_*` 行情表、股票基础信息、交易日历和股票因子表 |
| `etf_data.db` | ETF | 只保存 `etf_*` 行情表、ETF 基础信息和交易日历 |

`DuckDBManager` 会根据 `asset_type` 创建对应资产的表。股票库不会创建 ETF 行情表，ETF 库也不会创建股票行情表。

## 表结构总览

### 股票库

| 表名 | 类型 | 说明 |
| --- | --- | --- |
| `stock_daily` | 行情表 | 股票日线 |
| `stock_weekly` | 行情表 | 股票周线 |
| `stock_monthly` | 行情表 | 股票月线 |
| `stock_info` | 基础信息 | 股票代码和名称 |
| `trade_calendar` | 日历表 | 交易日历 |
| `factor_rps_daily` | 因子表 | RPS 日频因子 |
| `factor_update_log` | 因子日志 | 因子更新记录 |

### ETF 库

| 表名 | 类型 | 说明 |
| --- | --- | --- |
| `etf_daily` | 行情表 | ETF 日线 |
| `etf_weekly` | 行情表 | ETF 周线 |
| `etf_monthly` | 行情表 | ETF 月线 |
| `etf_info` | 基础信息 | ETF 代码和名称 |
| `trade_calendar` | 日历表 | 交易日历 |

## 行情表

适用表：

- `stock_daily`
- `stock_weekly`
- `stock_monthly`
- `etf_daily`
- `etf_weekly`
- `etf_monthly`

主要字段：

| 字段 | 说明 |
| --- | --- |
| `code` | 项目内部证券代码，如 `sh.600000`、`sz.000001` |
| `date` | 交易日期 |
| `open` | 开盘价 |
| `high` | 最高价 |
| `low` | 最低价 |
| `close` | 收盘价 |
| `preclose` | 前收盘价 |
| `volume` | 成交量 |
| `amount` | 成交额 |
| `adjustflag` | 复权标记 |
| `turn` | 换手率相关字段 |
| `tradestatus` | 交易状态 |
| `pctChg` | 涨跌幅 |
| `isST` | ST 标记 |

行情表不保存 `name`。证券名称属于基础信息，保存在 `stock_info` 或 `etf_info` 中。viewer 在展示、导出、涨跌幅榜和按代码汇总时会自动 JOIN 基础信息表返回 `name`。

## 基础信息表

| 表名 | 所属库 | 字段 | 说明 |
| --- | --- | --- | --- |
| `stock_info` | `stock_data.db` | `code`、`name`、`update_time` | 股票基础信息 |
| `etf_info` | `etf_data.db` | `code`、`name`、`update_time` | ETF 基础信息 |

基础信息表用于去重存储证券名称，避免在日线、周线、月线中重复写入同一个名称。

## 交易日历表

| 表名 | 所属库 | 字段 |
| --- | --- | --- |
| `trade_calendar` | 股票库和 ETF 库 | `date`、`is_trading_day`、`market`、`updated_at` |

交易日历用于后续增量更新、回测对齐和因子计算的日期基准。

## 因子表

### `factor_rps_daily`

RPS 使用专表保存，不混入 `stock_daily`。

| 字段 | 说明 |
| --- | --- |
| `code` | 股票代码 |
| `date` | 交易日期 |
| `rps_5` / `rps_10` / `rps_20` | 短周期 RPS |
| `rps_50` / `rps_120` / `rps_250` | 中长周期 RPS |
| `universe` | 因子计算股票池 |
| `factor_version` | 因子算法或口径版本 |
| `updated_at` | 更新时间 |

### `factor_update_log`

| 字段 | 说明 |
| --- | --- |
| `factor_name` | 因子名称 |
| `universe` | 股票池 |
| `factor_version` | 因子版本 |
| `start_date` / `end_date` | 本次更新覆盖日期 |
| `updated_at` | 更新时间 |
| `status` | 更新状态 |
| `message` | 补充信息或错误信息 |

## Viewer 查询行为

viewer 的表访问使用白名单，允许访问：

```text
stock_daily
stock_weekly
stock_monthly
stock_info
etf_daily
etf_weekly
etf_monthly
etf_info
trade_calendar
factor_rps_daily
factor_update_log
```

名称显示规则：

- `/api/table`：行情表返回 `code, name, date, ...`，关键词同时匹配代码和名称。
- `/api/export`：行情表导出 CSV 时包含 `name`。
- `/api/top_movers`：涨跌幅榜返回 `name`。
- `/api/summary_by_code`：按代码汇总返回 `name`。
- `/api/kline` 和 `/api/trend`：指定 `code` 时响应中包含对应 `name`。

## 扩展建议

- 新增通用因子时优先放入因子层，避免污染行情表。
- 因子表建议保留 `universe`、`factor_version`、`updated_at`，方便回测复现和口径升级。
- 需要面向前端展示的冗余字段，优先通过查询 JOIN 生成，不直接写入高频行情明细表。
