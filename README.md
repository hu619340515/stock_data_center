# A 股 / ETF 数据采集与可视化平台

这是一个面向本地量化研究的数据采集、存储和可视化工具。项目使用国金 QMT / miniQMT 的 `xtquant` 本地行情接口拉取 A 股和 ETF 历史行情，使用 DuckDB 做本地列式存储，并提供命令行工具和 Flask Web 管理界面。

## 更新日志

### 2026-06-01
- **ETF RPS 查询稳定性修复**：修复数据浏览页在异常响应结构下出现 `Cannot read properties of undefined (reading 'map')` 的问题。前端 `loadBrowserTable` 现在会对 `columns/rows/total` 做防御性兜底，不再因字段缺失直接崩溃。
- **RPS 周期扩展（股票+ETF）**：RPS 日频因子新增 `5日` 维度，计算与存储字段扩展为 `ret_5`、`rps_5`（同时保留 `20/50/120/250`）。
- **历史库兼容**：因子表初始化新增自动补列逻辑（缺少 `ret_5/rps_5` 时自动 `ALTER TABLE ADD COLUMN`），避免升级后老库查询报错。
- **接口兼容与回退**：`/api/table` 查询 `factor_rps_daily` / `etf_factor_rps_daily` 时优先返回 `ret_5/rps_5`；若当前库尚未补列则回退为 `NULL` 占位，保证接口可用。

### 2026-05-31
- **数据库边界清理**：`stock_data.db` 只创建和保留 `stock_*`、股票基础信息、交易日历和股票因子表；`etf_data.db` 只创建和保留 `etf_*`、ETF 基础信息和交易日历，避免两个库混放全套表。
- **基础信息拆分**：行情表不再存储 `name`，证券名称统一保存在 `stock_info` / `etf_info`，避免日线、周线、月线中重复写入名称。
- **交易日历与因子层**：新增 `trade_calendar`、`factor_rps_daily`、`factor_update_log`，RPS 不混入行情表，支持 `universe`、`factor_version`、`updated_at` 等版本追踪字段。
- **前端名称适配**：viewer 查询、导出、涨跌幅榜、按代码汇总、K 线和趋势接口会自动关联基础信息表返回名称，关键词搜索支持按代码或名称匹配。

### 2026-05-29
- **核心修复**：修复进程复活时进度偏移量未累积导致任务丢失的问题，新增 `process_base_completed` 追踪已完成偏移
- **配置优化**：心跳超时从 30 秒调整到 120 秒，避免 `upload_batch` 写入大量数据时误杀子进程
- **兼容性**：修复 DuckDB 不同配置同时连接导致的连接冲突问题
- **启动体验**：`start_viewer.bat` 新增依赖自动检查与安装（使用国内镜像源），避免启动闪退
- **自动化**：QMT 路径支持自动检测与配置

当前版本只保留 QMT / xtquant 数据源链路。使用前请确认 QMT 或 miniQMT 客户端已启动，并且当前 Python 环境可以导入 `xtquant`。

## 功能概览

- 支持股票和 ETF 两类资产。
- 支持日线、周线、月线三个周期，命令行统一使用 `d`、`w`、`m` 表示。
- 股票与 ETF 分库存储，默认写入 `stock_data.db` 和 `etf_data.db`。
- 使用 DuckDB 存储行情数据，股票库和 ETF 库分别只创建自身资产相关表。
- 基础信息独立存储在 `stock_info` / `etf_info`，行情表通过查询时 JOIN 展示证券名称。
- 提供交易日历表和因子层，RPS 使用 `factor_rps_daily` 专表保存。
- 支持全量下载、增量更新、断点续传、失败重试、动态并发和进程复活。
- 支持按代码、日期范围、频率、资产类型导出 `csv`、`parquet`、`json`。
- 提供 Web 界面查看概览、表格、K 线、涨跌排行、分布统计、日志和后台任务进度。

## 目录结构

```text
.
├── cli.py                     # 命令行入口
├── config.py                  # 配置加载与默认值
├── config.yaml                # 本地配置文件
├── core.py                    # 下载、更新、进度、临时库合并等核心流程
├── database.py                # DuckDB 表结构、写入、查询、导出和维护
├── data_source.py             # QMT / xtquant 数据源实现
├── data_source_factory.py     # 数据源工厂，目前仅支持 QMT / xtquant
├── data_source_interface.py   # 数据源接口定义
├── logger_config.py           # 日志配置
├── requirements.txt           # Python 依赖
├── check_deps.py              # 依赖自动检测与安装脚本
├── start_viewer.bat           # Windows 快速启动脚本
├── viewer/
│   ├── server.py              # Flask API 服务
│   ├── index.html             # Web 单页界面
│   ├── README.md              # viewer 子模块说明
│   └── db_portrait.md         # 数据库画像说明
└── test/                      # 单元测试与接口测试
```

运行后会在项目根目录生成或使用这些本地文件：

```text
stock_data.db                  # 股票数据库
etf_data.db                    # ETF 数据库
logs/app.log                   # 应用日志
error_log.txt                  # 错误日志
```

## 环境要求

- Python 3.10 或更高版本。
- DuckDB、Flask、pandas、PyYAML、psutil、click、numpy 等依赖。
- 国金 QMT / miniQMT 客户端。
- 可用的 `xtquant` Python SDK。

安装依赖：

```bash
pip install -r requirements.txt
```

如果 `xtquant` 无法通过普通 `pip` 安装，请按券商或 QMT 客户端提供的方式把 SDK 加入当前 Python 环境。

## 配置说明

主要配置写在 `config.yaml`，程序通过 `config.py` 读取。常用项如下：

```yaml
database:
  path: "quant_data.db"
  stock_path: "stock_data.db"
  etf_path: "etf_data.db"

qmt:
  ip: "127.0.0.1"
  port: 58610
  data_dir: ""
  dividend_type: "front"
  download_before_query: true
  sync_sector_data: false
  code_list_data_dir: ""
  stock_sectors: ["沪深A股"]
  etf_sectors: ["沪深ETF", "沪深基金"]

datasource:
  default: "qmt"
  stock_source: "qmt"
  etf_source: "qmt"
  priority: ["qmt"]
```

关键配置：

| 配置项 | 说明 |
| --- | --- |
| `database.stock_path` | 股票数据库路径，默认 `stock_data.db` |
| `database.etf_path` | ETF 数据库路径，默认 `etf_data.db` |
| `qmt.ip` / `qmt.port` | QMT 行情服务地址和端口，miniQMT 常用行情端口为 `58610` |
| `qmt.data_dir` | 可选，指定 QMT 本地行情缓存目录 |
| `qmt.code_list_data_dir` | 可选，板块列表不可用时用于从本地 `datadir` 推导代码列表 |
| `qmt.stock_sectors` | QMT 股票板块名称，可按本机券商环境调整 |
| `qmt.etf_sectors` | QMT ETF / 基金板块名称，可按本机券商环境调整 |
| `qmt.dividend_type` | 复权方式：`none`、`front`、`back`、`front_ratio`、`back_ratio` |
| `data.start_date_full` | 股票全量下载起始日期 |
| `data.start_date_full_etf` | ETF 全量下载起始日期 |
| `data.end_date` | 可选，固定拉取结束日期；不配置时自动按当前日期判断 |
| `concurrency.max_workers` | 初始并发数 |
| `concurrency.dynamic_concurrency` | 是否启用动态并发调整 |
| `batch.size` | 批量写入大小 |
| `retry.max_retries` | 数据源调用最大重试次数 |
| `process_monitor.enable_revive` | 是否启用子进程异常复活 |
| `process_monitor.heartbeat_timeout` | 子进程心跳超时时间（秒） |
| `process_monitor.max_revive_times` | 单个进程最大复活次数 |

`config.py` 支持在配置值中使用 `${ENV_NAME}` 引用环境变量。

## 命令行使用

查看全部命令：

```bash
python cli.py --help
```

### 股票数据

全量下载股票数据：

```bash
python cli.py download
python cli.py download -f d
python cli.py download -f w
python cli.py download -f m
```

`download` 是 `full` 的别名，也可以使用：

```bash
python cli.py full -f d
```

增量更新股票数据：

```bash
python cli.py update
python cli.py update -f w
python cli.py update -f m
```

### ETF 数据

全量下载 ETF 数据：

```bash
python cli.py download-etf
python cli.py download-etf -f d
python cli.py download-etf -f w
python cli.py download-etf -f m
```

`download-etf` 是 `etf-full` 的别名。

增量更新 ETF 数据：

```bash
python cli.py update-etf
python cli.py update-etf -f d
python cli.py update-etf -f w
python cli.py update-etf -f m
```

`update-etf` 是 `etf-update` 的别名。

### 查看状态

```bash
python cli.py status
python cli.py status -t stock -f d
python cli.py status -t etf -f m
```

参数说明：

- `-t, --type`：资产类型，支持 `stock`、`etf`。
- `-f, --frequency`：频率，支持 `d`、`w`、`m`。

### 导出数据

导出全部股票日线：

```bash
python cli.py export -o stock_daily.csv
```

按代码和日期范围导出：

```bash
python cli.py export -c sh.600000 -s 2023-01-01 -e 2023-12-31 -o 600000.csv
```

导出 ETF 周线为 Parquet：

```bash
python cli.py export -t etf -f w -m parquet -o etf_weekly.parquet
```

参数说明：

- `-c, --code`：证券代码，留空表示全部。
- `-s, --start-date`：开始日期，格式 `YYYY-MM-DD`。
- `-e, --end-date`：结束日期，默认当天。
- `-o, --output`：输出文件路径，必填。
- `-f, --frequency`：频率，支持 `d`、`w`、`m`。
- `-m, --format`：输出格式，支持 `csv`、`parquet`、`json`。
- `-t, --type`：资产类型，支持 `stock`、`etf`。

### 删除数据

删除指定代码：

```bash
python cli.py delete -c sh.600000 -y
```

删除指定日期范围：

```bash
python cli.py delete -s 2023-01-01 -e 2023-01-31 -y
```

删除 ETF 月线中的指定代码：

```bash
python cli.py delete -t etf -f m -c sh.510300 -y
```

如果不传 `-y`，命令会在删除前要求确认。

### 数据库维护

维护全部数据库：

```bash
python cli.py vacuum
```

只维护股票库或 ETF 库：

```bash
python cli.py vacuum -t stock
python cli.py vacuum -t etf
```

## Web 管理界面

Windows 下推荐使用快速启动脚本：

```bash
start_viewer.bat
```

脚本会自动检查并安装缺失的依赖（使用国内镜像源），然后启动 Web 服务。

或手动启动：

```bash
python cli.py start-viewer
```

默认访问地址：

```text
http://127.0.0.1:5678
```

也可以指定监听地址和端口：

```bash
python cli.py start-viewer --host 0.0.0.0 --port 5678
```

Web 界面支持：

- 查看股票库和 ETF 库连接状态。
- 查看日线、周线、月线表的记录数、日期范围和证券数量。
- 按表、代码、关键词、日期范围分页浏览数据。
- 查看单只股票或 ETF 的 K 线和 MA5、MA10、MA20、MA60。
- 查看涨跌幅排行、数值列统计、成交量或价格分布。
- 导出当前筛选结果为 CSV。
- 发起股票 / ETF 的日线下载、增量更新，以及日 / 周 / 月全周期下载。
- 查看任务进度、后台运行状态、应用日志和错误日志。
- 停止当前后台任务。

viewer 默认读取 `config.yaml` 中的 `database.stock_path` 和 `database.etf_path`。也可以通过环境变量覆盖：

```bash
set STOCK_DB_PATH=S:\path\stock_data.db
set ETF_DB_PATH=S:\path\etf_data.db
python cli.py start-viewer
```

`DB_PATH` 仍保留为兼容变量，但当前接口会根据表名前缀自动选择股票库或 ETF 库。

## 数据库表

项目默认使用两个 DuckDB 文件，股票和 ETF 分库管理：

| 数据库 | 资产边界 | 主要表 |
| --- | --- | --- |
| `stock_data.db` | 股票数据 | `stock_daily`、`stock_weekly`、`stock_monthly`、`stock_info`、`trade_calendar`、`factor_rps_daily`、`factor_update_log` |
| `etf_data.db` | ETF 数据 | `etf_daily`、`etf_weekly`、`etf_monthly`、`etf_info`、`trade_calendar` |

行情表：

| 表名 | 资产 | 周期 |
| --- | --- | --- |
| `stock_daily` | 股票 | 日线 |
| `stock_weekly` | 股票 | 周线 |
| `stock_monthly` | 股票 | 月线 |
| `etf_daily` | ETF | 日线 |
| `etf_weekly` | ETF | 周线 |
| `etf_monthly` | ETF | 月线 |

行情表主要字段：

| 字段 | 说明 |
| --- | --- |
| `code` | 项目内部证券代码，如 `sh.600000`、`sz.000001` |
| `date` | 交易日期 |
| `open` / `high` / `low` / `close` | 开高低收 |
| `preclose` | 前收盘价 |
| `volume` | 成交量 |
| `amount` | 成交额 |
| `adjustflag` | 复权标记 |
| `turn` | 换手率相关字段 |
| `tradestatus` | 交易状态 |
| `pctChg` | 涨跌幅 |
| `isST` | ST 标记 |

注意：行情表不再存储 `name`。前端查询、CSV 导出、涨跌幅榜、按代码汇总等展示接口会自动关联基础信息表返回名称。

基础信息表：

| 表名 | 所属库 | 说明 | 主要字段 |
| --- | --- | --- | --- |
| `stock_info` | `stock_data.db` | 股票基础信息 | `code`、`name`、`update_time` |
| `etf_info` | `etf_data.db` | ETF 基础信息 | `code`、`name`、`update_time` |

交易日历表：

| 表名 | 所属库 | 说明 | 主要字段 |
| --- | --- | --- | --- |
| `trade_calendar` | 股票库和 ETF 库 | 交易日历 | `date`、`is_trading_day`、`market`、`updated_at` |

因子表：

| 表名 | 所属库 | 说明 | 主要字段 |
| --- | --- | --- | --- |
| `factor_rps_daily` | `stock_data.db` | RPS 日频因子 | `code`、`date`、`rps_5`、`rps_10`、`rps_20`、`rps_50`、`rps_120`、`rps_250`、`universe`、`factor_version`、`updated_at` |
| `factor_update_log` | `stock_data.db` | 因子更新日志 | `factor_name`、`universe`、`factor_version`、`start_date`、`end_date`、`updated_at`、`status`、`message` |

## Web API 摘要

常用接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/status` | 查看股票库和 ETF 库连接状态 |
| `GET` | `/api/overview` | 查看所有表概览或指定表概览 |
| `GET` | `/api/table` | 分页查询表格数据 |
| `GET` | `/api/codes` | 获取指定表的代码列表 |
| `GET` | `/api/kline` | 获取指定代码的 K 线数据 |
| `GET` | `/api/stats` | 获取数值字段统计 |
| `GET` | `/api/top_movers` | 获取涨跌幅排行 |
| `GET` | `/api/distribution` | 获取价格、成交量或涨跌幅分布 |
| `GET` | `/api/trend` | 获取按日期聚合的趋势数据 |
| `GET` | `/api/refresh_status` | 查看各表最新更新时间 |
| `GET` | `/api/export` | 导出 CSV |
| `POST` | `/api/delete` | 删除指定数据 |
| `GET` | `/api/schema` | 查看表结构 |
| `GET` | `/api/summary_by_code` | 按代码聚合统计 |
| `GET` | `/api/progress` | 查看后台任务进度 |
| `GET` | `/api/logs` | 查看应用日志和错误日志 |
| `POST` | `/api/stop_task` | 停止当前后台任务 |

任务类接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/daily_download` | 股票或 ETF 日线全量下载，body 可传 `{"target":"stock"}` 或 `{"target":"etf"}` |
| `POST` | `/api/daily_to_latest` | 股票或 ETF 日线增量更新 |
| `POST` | `/api/download_stock_all_cycles` | 股票日 / 周 / 月全周期下载或更新 |
| `POST` | `/api/download_etf_all_cycles` | ETF 日 / 周 / 月全周期下载或更新 |
| `POST` | `/api/aggregate_weekly` | 顺序下载股票和 ETF 周线 |
| `POST` | `/api/aggregate_monthly` | 顺序下载股票和 ETF 月线 |
| `POST` | `/api/calculate_rps` | 根据股票日线重算 RPS 日频因子 |
| `POST` | `/api/etf_download` | ETF 日线全量下载 |
| `POST` | `/api/etf_update_latest` | ETF 日线增量更新 |

查询示例：

```text
GET /api/table?table=stock_daily&page=0&page_size=50&keyword=600000&start=2023-01-01&end=2023-12-31
GET /api/kline?table=etf_daily&code=sh.510300&start=2024-01-01
GET /api/top_movers?table=stock_daily&direction=rise&limit=20
```

## 测试

运行全部测试：

```bash
python -m pytest test
```

或使用项目内测试入口：

```bash
python test/run_tests.py
```

测试覆盖配置加载、数据源、数据库、核心流程和 viewer API / 路由。

## 常见问题

### `ImportError: 未安装 xtquant`

确认 QMT / miniQMT 已安装，并且当前 Python 环境可以导入 `xtquant`。不同券商环境的 SDK 安装方式可能不同，必要时把 QMT 提供的 Python 包路径加入 `PYTHONPATH`。

### 无法获取股票或 ETF 列表

优先检查 `qmt.stock_sectors` 和 `qmt.etf_sectors` 是否与本机 QMT 板块名称一致。如果板块接口不可用，可以配置 `qmt.code_list_data_dir` 指向 QMT 本地 `datadir`，程序会尝试从本地文件推导代码列表。

### QMT 接口报端口或服务错误

确认使用的是行情服务端口。miniQMT 常见行情端口是 `58610`，普通交易端口不一定提供完整历史行情接口。

### Web 页面没有数据显示

先运行：

```bash
python cli.py status -t stock -f d
python cli.py status -t etf -f d
```

确认数据库中已有对应表和记录。然后检查 `config.yaml` 的数据库路径或 `STOCK_DB_PATH`、`ETF_DB_PATH` 环境变量。

### 数据库文件太大

运行：

```bash
python cli.py vacuum
```

DuckDB 的维护操作可以回收部分空间并优化数据库文件。

### 进程异常退出与任务丢失

如果下载过程中出现进程频繁退出、心跳超时，或任务完成后统计显示有股票/ETF未处理，通常是以下两个问题已修复：

1. **心跳超时配置过短**：`config.yaml` 中 `heartbeat_timeout` 默认设为 120 秒，避免 `upload_batch` 写入大量数据时阻塞导致误杀子进程。
2. **进程复活时进度偏移量未累积**：已修复复活逻辑，新增 `process_base_completed` 追踪已完成的偏移量，解决重复处理和任务丢失问题。

### DuckDB 连接错误：不同配置冲突

当后台正在合并数据库（`merge_to_main_db`）时，Web 前端查询可能会报错：`Can't open a connection to same database file with a different configuration than existing connections`。已修复 viewer 的 `get_conn`，当遇到连接配置冲突时自动回退到默认模式。

### 快速启动依赖问题

Windows 下使用 `start_viewer.bat` 时闪退，已将依赖检查逻辑迁移到 `check_deps.py`，脚本会自动检测缺失的包并尝试安装（使用国内镜像源）。

## 许可

MIT License
