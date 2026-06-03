---
name: rps-etf-sina
description: Fetch China market ETF daily bars from Sina, store raw ETF metadata and OHLCV data in DuckDB, compute RPS5/RPS10/RPS20/RPS30/RPS50/RPS120 on demand, and render a Chinese top-100 ETF RPS PNG table. Use when the user asks for ETF RPS ranking, Sina ETF data refresh, DuckDB ETF daily data maintenance, or a daily ETF strength image for OpenClaw/XiaoLongXia.
---

# RPS ETF Sina

## Workflow

Use `scripts/rps_etf_sina.py` as the single entry point. Install dependencies first:

```bash
pip install -r requirements.txt
```

Run these commands from the skill directory:

```bash
python scripts/rps_etf_sina.py update --db data/etf_daily.duckdb
python scripts/rps_etf_sina.py render --db data/etf_daily.duckdb --out output/rps_etf_top100.png
python scripts/rps_etf_sina.py run --db data/etf_daily.duckdb --out output/rps_etf_top100.png
```

When asked to produce the current ETF RPS result, prefer `run` so the database refreshes before rendering. After the PNG is generated, return or send the image at `output/rps_etf_top100.png` when the chat surface supports images.

## Data Rules

- Fetch the ETF universe from Sina `etf_hq_fund`; do not include LOF funds.
- Fetch daily K-line data from Sina `CN_MarketData.getKLineData` with `scale=240`, `ma=no`, and `datalen=400`.
- Store only raw metadata and OHLCV rows in DuckDB.
- Do not store RPS columns in DuckDB. Always calculate RPS in code at render time.
- Keep approximately the latest one calendar year of daily rows in the database.

DuckDB tables:

```sql
etf_meta(symbol, code, name, market, source, updated_at)
etf_daily(symbol, trade_date, open, high, low, close, volume, fetched_at)
```

## RPS Rules

Calculate RPS on the latest available trading date in the database:

- `N-day return = close / close_N_trading_days_ago - 1`
- Windows: 5, 10, 20, 30, 50, 120
- For each window, rank the latest-date ETF cross section with `rank(pct=True) * 100`; the strongest ETF receives 100.
- Exclude ETFs that do not have enough history for all required windows.
- Sort by `RPS合计` descending and render the top 100.

The PNG table columns must stay in this order:

```text
名称, 代码, 综合排名, RPS5, RPS10, RPS20, RPS30, RPS50, RPS120, RPS合计
```

## Scheduling

For Linux deployment, install the daily 09:00 Beijing-time cron job:

```bash
bash scripts/install_cron.sh
```

The cron job runs `python scripts/rps_etf_sina.py run`, writes logs to `logs/rps_etf_sina.log`, and renders `output/rps_etf_top100.png`.

If Chinese characters render as boxes on Linux, install a CJK font package such as:

```bash
sudo apt-get install fonts-noto-cjk
```
