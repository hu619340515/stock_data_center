### A股量化数据中枢

一个基于 **Python + DuckDB + Baostock** 的高性能、零配置 A股历史数据自动化处理系统。

专为量化交易策略回测设计，利用 **DuckDB 的列式存储**特性，实现了比传统 MongoDB/MySQL 方案快数十倍的数据读写与分析速度，且无需安装任何数据库服务。

#### 核心特性

- **极速分析引擎**：采用 DuckDB 列式存储，查询速度比 MongoDB 快 10-100 倍，特别适合计算均线、因子等聚合分析。
- **零配置部署**：无需安装数据库服务，**单文件数据库** (`quant_data.db`)，开箱即用，方便迁移和备份。
- **全自动化流水线**：一键执行全量历史数据下载或每日增量更新。
- **流式处理**：数据在内存中流转 (`API -> Pandas -> DuckDB`)，不产生中间 CSV 文件，节省磁盘 I/O。
- **智能容错**：内置重试机制与智能日期回退，有效应对网络波动和非交易日。

#### ️ 技术栈

- **数据库**: **DuckDB** (嵌入式分析型数据库)
- **数据源**: Baostock (证券宝 - 免费 A股数据接口)
- **核心库**: `duckdb`, `pandas`, `baostock`, `click`

#### 项目结构

```
stock_data_center/
├── config.py              # 全局配置 (数据库路径、线程数)
├── database.py            # DuckDB 封装 (核心优化：列对齐、类型转换)
├── data_source.py         # Baostock 接口封装 (智能日期处理)
├── core.py                # 核心业务逻辑 (多线程流水线)
├── cli.py                 # 命令行入口工具
├── test.py                # 快速测试脚本
├── logger_config.py       # 日志配置
└── quant_data.db          # (运行后生成) 你的本地数据库文件
```

#### 快速开始

**1. 环境准备**

无需安装数据库软件，只需安装 Python 依赖：

```
pip install duckdb pandas baostock click
```

**2. 运行测试**

在正式全量下载前，建议先运行测试脚本，验证流程：

```
python test.py
```

*该脚本仅处理前 10 只股票，速度快，用于验证环境。*

**3. 数据下载**

- **全量下载** (首次使用，耗时较长):

```
python cli.py full
```

- **每日增量更新** (日常使用，秒级完成):

```
python cli.py update
```

- **查看状态**:

```
python cli.py status
```

#### 数据库设计

系统使用 **DuckDB** 存储数据，采用**单文件**模式。

- **数据库文件**: `quant_data.db` (位于项目根目录)
- **表名**: `stock_daily`
- **数据结构**:

| 字段名 | 类型 | 说明 |
| ------ |------ |------ |
| **code** | VARCHAR | 股票代码 (如 sh.600000) |
| **date** | DATE | 交易日期 |
| **open** | DOUBLE | 开盘价 |
| **high** | DOUBLE | 最高价 |
| **low** | DOUBLE | 最低价 |
| **close** | DOUBLE | 收盘价 |
| **volume** | BIGINT | 成交量 |
| **amount** | DOUBLE | 成交额 |
| ... | ... | 其他字段 |

- **主键**: `(code, date)`，确保数据不重复。

#### ️ 命令行工具说明

| 命令 | 描述 |
| ------ |------ |
| `python cli.py full` | 启动全量流式下载流水线（从1999年至今） |
| `python cli.py update` | 启动增量更新流水线（仅下载最新数据） |
| `python cli.py status` | 查看当前数据库中的总记录数 |
| `python test.py` | 快速测试流程（仅处理前10只股票） |

#### 为什么选择 DuckDB？

相比 MongoDB 或 SQLite，DuckDB 在量化场景下具有压倒性优势：

| 维度 | DuckDB | MongoDB | SQLite |
| ------ |------ |------ |------ |
| **定位** | **分析型 (OLAP)** | 文档型 (NoSQL) | 嵌入式事务型 |
| **查询速度** | **极快** (列式存储) | 慢 (行式存储) | 快 (但聚合查询慢) |
| **部署难度** | **零** (单文件) | 高 (需安装服务) | 零 (单文件) |
| **Pandas集成** | **完美** (直接读写) | 需转换格式 | 需转换格式 |

#### 许可证

MIT License

