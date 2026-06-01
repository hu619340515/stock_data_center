# Web 可视化管理界面

`viewer/server.py` 提供基于 Flask 的本地 Web 管理服务。前端由 `viewer/index.html`、`viewer/styles.css` 和 `viewer/app.js` 组成，是一个面向本地量化研究的数据资产工作台。界面用于浏览股票 / ETF 数据库、查看市场快照和证券 K 线、导出 CSV、启动后台下载任务并观察实时进度和日志。

## 启动方式

推荐从项目根目录启动：

```bash
python cli.py start-viewer
```

默认地址：

```text
http://127.0.0.1:5678
```

也可以直接启动 Flask 服务：

```bash
python viewer/server.py
```

指定端口：

```bash
python cli.py start-viewer --host 127.0.0.1 --port 5679
```

## 数据库路径

viewer 默认读取根目录 `config.yaml`：

```yaml
database:
  stock_path: "stock_data.db"
  etf_path: "etf_data.db"
```

也可以用环境变量覆盖：

```bash
set STOCK_DB_PATH=S:\path\stock_data.db
set ETF_DB_PATH=S:\path\etf_data.db
python cli.py start-viewer
```

当前 API 会根据表名自动选择数据库：

- `stock_daily`、`stock_weekly`、`stock_monthly`、`stock_info`、`factor_rps_daily`、`factor_update_log` 读取股票库。
- `etf_daily`、`etf_weekly`、`etf_monthly`、`etf_info` 读取 ETF 库。
- `trade_calendar` 在股票库和 ETF 库均可存在，viewer 默认按股票库读取。

## 页面能力

- 数据资产总览：聚合股票库、ETF 库、交易日历和因子层状态，显示健康度、核心规模、新鲜度矩阵和质量提醒。
- 市场快照：按资产、周期和日期查看涨跌幅分布、上涨 / 下跌 / 平盘数量、平均涨跌幅和涨跌榜。
- 证券研究：按代码或名称联想证券，查看 K 线、成交量、成交额和 MA5 / MA10 / MA20 / MA60。
- 数据浏览：按表、关键词、日期范围分页查询；行情表自动关联 `stock_info` / `etf_info` 显示证券名称，点击行情行可进入证券研究。
- 同步中心：区分日常增量更新、智能全周期更新、派生数据计算和高成本全量下载。
- 因子中心：查看 RPS 因子状态、支持周期和计算记录，并启动 RPS 计算。
- 质量与日志：集中查看空表、异常字段、表级状态和实时日志。
- 任务抽屉：查看当前后台任务、进度、成功 / 失败统计、最近任务历史和实时日志。
- 数据导出：把当前筛选结果导出为 UTF-8 BOM CSV，行情表导出包含 `name` 展示列。
- 高级维护：先预览影响条数，再通过确认参数删除指定代码或日期范围的数据。

## 表白名单

为了避免任意 SQL 表访问，viewer 只允许查询以下表：

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
etf_factor_rps_daily
etf_factor_update_log
```

行情表自身不存储 `name` 字段，名称来自 `stock_info` / `etf_info`。以下接口会自动 JOIN 基础信息表并返回名称：

- `/api/table`
- `/api/export`
- `/api/top_movers`
- `/api/summary_by_code`
- `/api/kline`
- `/api/trend`

`/api/table` 和 `/api/export` 的 `keyword` 参数支持同时匹配证券代码和证券名称。

## API 清单

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 返回前端页面 |
| `GET` | `/favicon.ico` | 返回空响应，避免浏览器图标请求报错 |
| `GET` | `/api/status` | 查看股票库和 ETF 库连接状态 |
| `GET` | `/api/dashboard` | 查看首页聚合指标、健康度和质量提醒 |
| `GET` | `/api/data_quality` | 查看质量诊断和表级状态 |
| `GET` | `/api/task_history` | 查看当前任务和最近任务历史 |
| `GET` | `/api/progress` | 查看后台任务进度 |
| `GET` | `/api/logs` | 查看应用日志和错误日志 |
| `GET` | `/api/overview` | 查看所有表概览或指定表概览 |
| `GET` | `/api/table` | 分页查询表格数据 |
| `GET` | `/api/codes` | 获取指定表的代码列表 |
| `GET` | `/api/security_search` | 按代码或名称联想股票或 ETF |
| `GET` | `/api/kline` | 获取 K 线数据 |
| `GET` | `/api/stats` | 获取数值字段统计 |
| `GET` | `/api/top_movers` | 获取涨跌幅排行 |
| `GET` | `/api/distribution` | 获取价格、成交量、涨跌幅等字段分布 |
| `GET` | `/api/trend` | 获取按日期聚合的趋势数据 |
| `GET` | `/api/refresh_status` | 查看各表最新日期、最早日期和记录数 |
| `GET` | `/api/export` | 导出 CSV |
| `POST` | `/api/delete` | 删除指定代码或日期范围的数据 |
| `POST` | `/api/delete_preview` | 删除前预览影响条数 |
| `GET` | `/api/schema` | 查看表字段结构 |
| `GET` | `/api/summary_by_code` | 按代码聚合统计 |
| `POST` | `/api/stop_task` | 停止当前后台任务 |
| `POST` | `/api/daily_download` | 股票或 ETF 日线全量下载 |
| `POST` | `/api/daily_to_latest` | 股票或 ETF 日线增量更新 |
| `POST` | `/api/download_stock_all_cycles` | 股票日 / 周 / 月全周期下载或更新 |
| `POST` | `/api/download_etf_all_cycles` | ETF 日 / 周 / 月全周期下载或更新 |
| `POST` | `/api/aggregate_weekly` | 顺序下载股票和 ETF 周线 |
| `POST` | `/api/aggregate_monthly` | 顺序下载股票和 ETF 月线 |
| `POST` | `/api/calculate_rps` | 根据 `target: "stock" | "etf"` 重算对应资产池的 RPS 日频因子 |
| `POST` | `/api/rebuild_trade_calendar` | 根据股票库和 ETF 库中已有的日线日期重建开闭市日历 |
| `POST` | `/api/etf_download` | ETF 日线全量下载 |
| `POST` | `/api/etf_update_latest` | ETF 日线增量更新 |

## 查询参数示例

分页查询：

```text
GET /api/table?table=stock_daily&page=0&page_size=50&keyword=600000&start=2023-01-01&end=2023-12-31
```

K 线：

```text
GET /api/kline?table=etf_daily&code=sh.510300&start=2024-01-01&end=2024-12-31
```

涨跌幅排行：

```text
GET /api/top_movers?table=stock_daily&direction=rise&limit=20
```

导出 CSV：

```text
GET /api/export?table=stock_daily&code=sh.600000&start=2023-01-01&end=2023-12-31
```

删除数据需要 POST JSON，并显式传入 `confirm: true`：

```json
{
  "table": "stock_daily",
  "code": "sh.600000",
  "start": "2023-01-01",
  "end": "2023-01-31",
  "confirm": true
}
```

## 开发与测试

运行 viewer 相关测试：

```bash
python -m pytest test/test_viewer_api.py test/test_viewer_routes.py
```

接口实现中对表名使用白名单校验，对查询条件使用 DuckDB 参数占位符，避免把用户输入直接拼接进 SQL 条件。

## Latest-day derived-field repair

The quality page exposes a guarded repair action for the latest market-data date. It only reconstructs fields that can be derived safely from local data:

- `preclose` from the previous stored trading bar for the same security.
- `pctChg` from `close` and `preclose`.
- Current stock `isST` from `stock_info.name`.

Turnover is intentionally not rewritten by this local repair. New QMT downloads derive `turn` from K-line volume and current circulating shares, while an exact historical turnover backfill requires historical circulating-capital data.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/api/repair_latest_derived_fields` | Repair locally derivable latest-day fields after an explicit `confirm: true`. |

## Trade-calendar rebuild

`trade_calendar` is rebuilt from the daily bars already stored in each asset database. The rebuild covers every natural day from the first daily bar through the latest daily bar, marks dates with stored bars as open days, and records the adjacent open dates for weekends and holidays. Daily stock and ETF pipelines refresh the calendar automatically after successful writes; `/api/rebuild_trade_calendar` is the manual repair entry point.

## 2026-06-01 Viewer update note

- Browser table rendering is now defensive against malformed payloads:
  - `rows` defaults to `[]` when absent/non-array.
  - `columns` falls back to `Object.keys(rows[0])` when missing.
  - `total` falls back to `rows.length` when missing/non-numeric.
  - This prevents runtime errors like `Cannot read properties of undefined (reading 'map')`.
- RPS factor table display now supports `ret_5` and `rps_5` for both stock and ETF factor tables.
- `/api/table` now has safer connection cleanup in the factor-table query path to reduce connection leak risk on exceptions.
- Factor-table response compatibility:
  - Preferred fields include `ret_5/rps_5`.
  - If an upgraded backend reads an old factor table missing those columns, API returns `NULL AS ret_5/rps_5` instead of failing.
