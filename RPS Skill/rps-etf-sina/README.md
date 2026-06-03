# rps-etf-sina

`rps-etf-sina` 是一个可复制到小龙虾/OpenClaw 使用的 ETF RPS 技能项目。它从新浪免费数据源获取市场 ETF 近一年日线数据，将原始 ETF 元数据和 OHLCV 行情写入 DuckDB，并在需要生成结果时实时计算 RPS5、RPS10、RPS20、RPS30、RPS50、RPS120，最终输出中文 PNG 表格。

## 功能

- 从新浪 ETF 基金池获取 ETF 名称、代码和交易所前缀。
- 从新浪日 K JSON 接口获取 ETF 日线数据。
- 使用 DuckDB 保存原始数据，不保存 RPS 派生字段。
- 按最新可用交易日计算 ETF 截面 RPS。
- 按 `RPS合计` 从高到低取前 100。
- 生成中文图片表格，列顺序为：`名称`、`代码`、`综合排名`、`RPS5`、`RPS10`、`RPS20`、`RPS30`、`RPS50`、`RPS120`、`RPS合计`。
- 提供 Linux cron 安装脚本，可每天北京时间 09:00 自动更新和生成图片。

## 目录结构

```text
rps-etf-sina/
├── SKILL.md
├── README.md
├── requirements.txt
├── agents/
│   └── openai.yaml
├── scripts/
│   ├── install_cron.sh
│   └── rps_etf_sina.py
└── tests/
    └── test_rps_etf_sina.py
```

## 安装

```bash
cd rps-etf-sina
pip install -r requirements.txt
```

Linux 如果图片中文显示为方框，安装 CJK 字体：

```bash
sudo apt-get install fonts-noto-cjk
```

## 使用

更新 DuckDB：

```bash
python scripts/rps_etf_sina.py update --db data/etf_daily.duckdb
```

从已有 DuckDB 生成图片：

```bash
python scripts/rps_etf_sina.py render --db data/etf_daily.duckdb --out output/rps_etf_top100.png
```

更新数据并生成图片：

```bash
python scripts/rps_etf_sina.py run --db data/etf_daily.duckdb --out output/rps_etf_top100.png
```

## 定时任务

在 Linux 服务器执行：

```bash
bash scripts/install_cron.sh
```

脚本会注册每天北京时间 09:00 的 cron 任务，运行 `run` 命令，日志写入：

```text
logs/rps_etf_sina.log
```

## 数据库

DuckDB 只保存原始数据。

`etf_meta`：

```text
symbol, code, name, market, source, updated_at
```

`etf_daily`：

```text
symbol, trade_date, open, high, low, close, volume, fetched_at
```

RPS 不入库，渲染时按代码实时计算。

## RPS 口径

对每个窗口 `N`：

```text
N日涨跌幅 = close / close_N_trading_days_ago - 1
RPSN = 最新交易日 ETF 截面的百分位排名 * 100
```

窗口固定为：

```text
5, 10, 20, 30, 50, 120
```

ETF 如果没有足够历史数据计算全部窗口，会被排除。最终按 `RPS合计` 降序排名。

## 测试

```bash
python -m unittest discover -s tests -v
```

也可以验证 skill 结构：

```bash
PYTHONUTF8=1 python C:/Users/旺仔/.codex/skills/.system/skill-creator/scripts/quick_validate.py .
```

Windows 运行校验脚本时建议设置 `PYTHONUTF8=1`，避免中文说明文件被默认 GBK 编码读取。

## 部署到小龙虾/OpenClaw

将整个 `rps-etf-sina` 目录复制到小龙虾/OpenClaw 的 skills 目录。触发后优先使用：

```bash
python scripts/rps_etf_sina.py run --db data/etf_daily.duckdb --out output/rps_etf_top100.png
```

生成后把 `output/rps_etf_top100.png` 作为图片结果发送。
