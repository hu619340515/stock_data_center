# Web 可视化管理界面

`viewer/server.py` 提供基于 Flask 的本地 Web 管理服务，前端页面为 `viewer/index.html`。界面用于浏览股票 / ETF 数据库、查看 K 线和统计图表、导出 CSV、启动后台下载任务并观察实时进度和日志。

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

- `stock_daily`、`stock_weekly`、`stock_monthly` 读取股票库。
- `etf_daily`、`etf_weekly`、`etf_monthly` 读取 ETF 库。

## 页面能力

- 数据库状态：查看股票库和 ETF 库是否可连接、包含哪些表。
- 数据概览：查看记录数、证券数量、最早日期、最新日期、最新上涨数和下跌数。
- 表格浏览：按表、关键词、日期范围分页查询。
- K 线图：按代码和日期范围查看 K 线、成交量、成交额和 MA5 / MA10 / MA20 / MA60。
- 统计分析：查看数值列统计、涨跌幅排行、价格或成交量分布、聚合趋势。
- 数据导出：把当前筛选结果导出为 UTF-8 BOM CSV。
- 后台任务：启动股票或 ETF 的下载、增量更新、周线 / 月线任务和全周期任务。
- 任务进度：查看总量、已处理、成功、失败、速度、预计剩余时间和实时日志。
- 数据删除：通过确认参数删除指定代码或日期范围的数据。

## 表白名单

为了避免任意 SQL 表访问，viewer 只允许查询以下表：

```text
stock_daily
stock_weekly
stock_monthly
etf_daily
etf_weekly
etf_monthly
```

## API 清单

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 返回前端页面 |
| `GET` | `/favicon.ico` | 返回空响应，避免浏览器图标请求报错 |
| `GET` | `/api/status` | 查看股票库和 ETF 库连接状态 |
| `GET` | `/api/progress` | 查看后台任务进度 |
| `GET` | `/api/logs` | 查看应用日志和错误日志 |
| `GET` | `/api/overview` | 查看所有表概览或指定表概览 |
| `GET` | `/api/table` | 分页查询表格数据 |
| `GET` | `/api/codes` | 获取指定表的代码列表 |
| `GET` | `/api/kline` | 获取 K 线数据 |
| `GET` | `/api/stats` | 获取数值字段统计 |
| `GET` | `/api/top_movers` | 获取涨跌幅排行 |
| `GET` | `/api/distribution` | 获取价格、成交量、涨跌幅等字段分布 |
| `GET` | `/api/trend` | 获取按日期聚合的趋势数据 |
| `GET` | `/api/refresh_status` | 查看各表最新日期、最早日期和记录数 |
| `GET` | `/api/export` | 导出 CSV |
| `POST` | `/api/delete` | 删除指定代码或日期范围的数据 |
| `GET` | `/api/schema` | 查看表字段结构 |
| `GET` | `/api/summary_by_code` | 按代码聚合统计 |
| `POST` | `/api/stop_task` | 停止当前后台任务 |
| `POST` | `/api/daily_download` | 股票或 ETF 日线全量下载 |
| `POST` | `/api/daily_to_latest` | 股票或 ETF 日线增量更新 |
| `POST` | `/api/download_stock_all_cycles` | 股票日 / 周 / 月全周期下载或更新 |
| `POST` | `/api/download_etf_all_cycles` | ETF 日 / 周 / 月全周期下载或更新 |
| `POST` | `/api/aggregate_weekly` | 周线下载 |
| `POST` | `/api/aggregate_monthly` | 月线下载 |
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
