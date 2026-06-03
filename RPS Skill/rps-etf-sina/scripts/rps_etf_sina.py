#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont


SINA_ETF_LIST_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/jsonp.php/"
    "IO.XSRV2.CallbackList['rps_etf_sina']/Market_Center.getHQNodeDataSimple"
)
SINA_KLINE_URL = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}
RPS_WINDOWS = (5, 10, 20, 30, 50, 120)
SOURCE = "sina"
CHINA_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")


@dataclass(frozen=True)
class FetchResult:
    symbol: str
    rows: list[dict[str, Any]]
    error: str | None = None


def now_china() -> datetime:
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Sina ETF data and render RPS ranking.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_data_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--db", default="data/etf_daily.duckdb", help="DuckDB path")
        subparser.add_argument("--workers", type=int, default=8, help="Concurrent fetch workers")
        subparser.add_argument("--datalen", type=int, default=400, help="Sina K-line datalen")
        subparser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")

    update_parser = subparsers.add_parser("update", help="Fetch ETF daily data into DuckDB")
    add_common_data_args(update_parser)

    render_parser = subparsers.add_parser("render", help="Render top ETF RPS PNG from DuckDB")
    render_parser.add_argument("--db", default="data/etf_daily.duckdb", help="DuckDB path")
    render_parser.add_argument("--out", default="output/rps_etf_top100.png", help="Output PNG path")
    render_parser.add_argument("--top-n", type=int, default=100, help="Number of ranked ETFs to render")
    render_parser.add_argument("--font-path", default=None, help="Optional Chinese font path")

    run_parser = subparsers.add_parser("run", help="Update DuckDB, then render top ETF RPS PNG")
    add_common_data_args(run_parser)
    run_parser.add_argument("--out", default="output/rps_etf_top100.png", help="Output PNG path")
    run_parser.add_argument("--top-n", type=int, default=100, help="Number of ranked ETFs to render")
    run_parser.add_argument("--font-path", default=None, help="Optional Chinese font path")

    return parser.parse_args()


def extract_jsonp_array(text: str) -> list[dict[str, Any]]:
    start = text.find("([")
    if start >= 0:
        start += 1
        end = text.rfind("])")
        if end >= 0:
            end += 1
        else:
            end = text.rfind("]") + 1
    else:
        start = text.find("[{")
        end = text.rfind("]") + 1

    if start < 0 or end <= start:
        raise ValueError("Cannot locate JSONP array in Sina ETF response")

    return json.loads(text[start:end])


def fetch_etf_universe(timeout: float = 15.0) -> pd.DataFrame:
    params = {
        "page": "1",
        "num": "5000",
        "sort": "symbol",
        "asc": "0",
        "node": "etf_hq_fund",
    }
    response = requests.get(SINA_ETF_LIST_URL, params=params, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    data = extract_jsonp_array(response.text)

    rows: list[dict[str, Any]] = []
    for item in data:
        symbol = str(item.get("symbol", "")).strip().lower()
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if not re.fullmatch(r"(sh|sz)\d{6}", symbol):
            continue
        if not code:
            code = symbol[2:]
        rows.append(
            {
                "symbol": symbol,
                "code": code,
                "name": name,
                "market": symbol[:2],
                "source": SOURCE,
                "updated_at": now_china(),
            }
        )

    if not rows:
        raise RuntimeError("Sina ETF universe is empty")

    return pd.DataFrame(rows).drop_duplicates(subset=["symbol"], keep="first")


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def to_int(value: Any) -> int | None:
    number = to_float(value)
    if number is None:
        return None
    return int(number)


def fetch_symbol_daily(
    symbol: str,
    start_date: date,
    datalen: int = 400,
    timeout: float = 15.0,
    attempts: int = 3,
) -> FetchResult:
    params = {"symbol": symbol, "scale": "240", "ma": "no", "datalen": str(datalen)}
    last_error: str | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(SINA_KLINE_URL, params=params, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            data = json.loads(response.text.strip() or "[]")
            if not isinstance(data, list):
                raise ValueError("Unexpected K-line payload")

            rows: list[dict[str, Any]] = []
            fetched_at = now_china()
            for item in data:
                trade_date = datetime.strptime(item["day"], "%Y-%m-%d").date()
                if trade_date < start_date:
                    continue
                close_value = to_float(item.get("close"))
                if close_value is None:
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "open": to_float(item.get("open")),
                        "high": to_float(item.get("high")),
                        "low": to_float(item.get("low")),
                        "close": close_value,
                        "volume": to_int(item.get("volume")),
                        "fetched_at": fetched_at,
                    }
                )
            return FetchResult(symbol=symbol, rows=rows)
        except Exception as exc:  # noqa: BLE001 - retry and report data-source errors.
            last_error = str(exc)
            if attempt < attempts:
                time.sleep(0.5 * attempt)

    return FetchResult(symbol=symbol, rows=[], error=last_error or "unknown error")


def initialize_database(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_meta (
            symbol VARCHAR PRIMARY KEY,
            code VARCHAR,
            name VARCHAR,
            market VARCHAR,
            source VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_daily (
            symbol VARCHAR,
            trade_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            fetched_at TIMESTAMP,
            PRIMARY KEY (symbol, trade_date)
        )
        """
    )


def write_database(
    db_path: Path,
    meta_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    start_date: date,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        initialize_database(con)
        con.execute("BEGIN TRANSACTION")
        try:
            con.register("meta_df", meta_df)
            con.execute(
                """
                DELETE FROM etf_meta
                USING meta_df
                WHERE etf_meta.symbol = meta_df.symbol
                """
            )
            con.execute(
                """
                INSERT INTO etf_meta
                SELECT symbol, code, name, market, source, updated_at
                FROM meta_df
                """
            )

            if not daily_df.empty:
                clean_daily = daily_df.drop_duplicates(
                    subset=["symbol", "trade_date"], keep="last"
                ).copy()
                clean_daily["trade_date"] = pd.to_datetime(clean_daily["trade_date"]).dt.date
                con.register("daily_df", clean_daily)
                con.execute(
                    """
                    DELETE FROM etf_daily
                    USING daily_df
                    WHERE etf_daily.symbol = daily_df.symbol
                      AND etf_daily.trade_date = daily_df.trade_date
                    """
                )
                con.execute(
                    """
                    INSERT INTO etf_daily
                    SELECT symbol, trade_date, open, high, low, close, volume, fetched_at
                    FROM daily_df
                    """
                )

            con.execute("DELETE FROM etf_daily WHERE trade_date < ?", [start_date])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()


def update_database(
    db_path: str | Path,
    datalen: int = 400,
    workers: int = 8,
    timeout: float = 15.0,
) -> dict[str, Any]:
    start_date = now_china().date() - timedelta(days=365)
    meta_df = fetch_etf_universe(timeout=timeout)

    daily_rows: list[dict[str, Any]] = []
    failures: list[str] = []
    symbols = meta_df["symbol"].tolist()
    max_workers = max(1, workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_symbol_daily, symbol, start_date, datalen, timeout): symbol
            for symbol in symbols
        }
        for index, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            if result.error:
                failures.append(f"{result.symbol}: {result.error}")
            daily_rows.extend(result.rows)
            if index % 100 == 0:
                print(f"Fetched {index}/{len(symbols)} ETF symbols...", file=sys.stderr)

    daily_df = pd.DataFrame(daily_rows)
    if daily_df.empty:
        raise RuntimeError("No ETF daily rows fetched from Sina")

    write_database(Path(db_path), meta_df, daily_df, start_date)
    return {
        "db": str(db_path),
        "symbols": len(symbols),
        "daily_rows": len(daily_df),
        "failures": failures,
        "start_date": start_date.isoformat(),
    }


def load_daily_data(db_path: str | Path) -> pd.DataFrame:
    db_file = Path(db_path)
    if not db_file.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_file}")

    con = duckdb.connect(str(db_file), read_only=True)
    try:
        return con.execute(
            """
            SELECT
                d.symbol,
                m.code,
                m.name,
                d.trade_date,
                d.close
            FROM etf_daily d
            JOIN etf_meta m ON d.symbol = m.symbol
            ORDER BY d.symbol, d.trade_date
            """
        ).fetch_df()
    finally:
        con.close()


def compute_rps_table(price_df: pd.DataFrame, top_n: int = 100) -> tuple[pd.DataFrame, date]:
    if price_df.empty:
        raise RuntimeError("No ETF daily data available for RPS calculation")

    df = price_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["symbol", "trade_date", "close"])
    df = df.sort_values(["symbol", "trade_date"])

    for window in RPS_WINDOWS:
        df[f"ret{window}"] = df.groupby("symbol")["close"].pct_change(periods=window)

    as_of = df["trade_date"].max()
    latest = df[df["trade_date"] == as_of].copy()
    return_cols = [f"ret{window}" for window in RPS_WINDOWS]
    latest = latest.dropna(subset=return_cols)
    if latest.empty:
        raise RuntimeError("No ETF has enough history for all RPS windows")

    for window in RPS_WINDOWS:
        latest[f"RPS{window}"] = latest[f"ret{window}"].rank(pct=True) * 100

    rps_cols = [f"RPS{window}" for window in RPS_WINDOWS]
    latest["RPS合计"] = latest[rps_cols].sum(axis=1)
    latest = latest.sort_values(
        ["RPS合计", "RPS120", "RPS50", "symbol"],
        ascending=[False, False, False, True],
    )
    latest["综合排名"] = range(1, len(latest) + 1)

    output_cols = ["name", "code", "综合排名", *rps_cols, "RPS合计"]
    result = latest[output_cols].head(top_n).rename(columns={"name": "名称", "code": "代码"})
    for col in [*rps_cols, "RPS合计"]:
        result[col] = result[col].round(2)
    return result, as_of


def find_chinese_font(font_path: str | None = None) -> str:
    candidates: list[str] = []
    if font_path:
        candidates.append(font_path)

    candidates.extend(
        [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.otf",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/truetype/arphic/uming.ttc",
            "/System/Library/Fonts/PingFang.ttc",
            "/Library/Fonts/Arial Unicode.ttf",
            "C:/Windows/Fonts/msyh.ttc",
            "C:/Windows/Fonts/simhei.ttf",
            "C:/Windows/Fonts/simsun.ttc",
        ]
    )

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate

    raise RuntimeError(
        "No Chinese font found. On Linux, install fonts-noto-cjk or pass --font-path."
    )


def load_font(font_path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(font_path, size=size)


def text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def ellipsize(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> str:
    if text_width(draw, text, font) <= max_width:
        return text
    suffix = "..."
    trimmed = text
    while trimmed and text_width(draw, trimmed + suffix, font) > max_width:
        trimmed = trimmed[:-1]
    return trimmed + suffix if trimmed else suffix


def draw_cell_text(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    text: str,
    font: ImageFont.ImageFont,
    fill: str,
    align: str = "left",
) -> None:
    left, top, right, bottom = box
    padding = 10
    available_width = max(1, right - left - padding * 2)
    text = ellipsize(draw, text, font, available_width)
    bbox = draw.textbbox((0, 0), text, font=font)
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    if align == "right":
        x = right - padding - width
    elif align == "center":
        x = left + (right - left - width) / 2
    else:
        x = left + padding
    y = top + (bottom - top - height) / 2 - 1
    draw.text((x, y), text, font=font, fill=fill)


def render_table_image(
    table_df: pd.DataFrame,
    as_of: date,
    out_path: str | Path,
    font_path: str | None = None,
) -> Path:
    if table_df.empty:
        raise RuntimeError("RPS table is empty")

    font_file = find_chinese_font(font_path)
    title_font = load_font(font_file, 30)
    header_font = load_font(font_file, 19)
    cell_font = load_font(font_file, 18)
    footer_font = load_font(font_file, 15)

    columns = ["名称", "代码", "综合排名", "RPS5", "RPS10", "RPS20", "RPS30", "RPS50", "RPS120", "RPS合计"]
    widths = [260, 105, 105, 100, 100, 100, 100, 100, 110, 120]
    margin_x = 32
    margin_y = 24
    title_h = 58
    header_h = 40
    row_h = 31
    footer_h = 34
    table_width = sum(widths)
    image_width = table_width + margin_x * 2
    image_height = margin_y + title_h + header_h + row_h * len(table_df) + footer_h + margin_y

    image = Image.new("RGB", (image_width, image_height), "#f7f8fa")
    draw = ImageDraw.Draw(image)

    title = f"ETF RPS 强度前 {len(table_df)}（{as_of.isoformat()}）"
    draw.text((margin_x, margin_y + 6), title, font=title_font, fill="#172033")
    draw.text(
        (margin_x, margin_y + 40),
        "数据源：新浪财经 | RPS按最新交易日截面百分位计算",
        font=footer_font,
        fill="#697386",
    )

    y = margin_y + title_h
    x = margin_x
    header_fill = "#1f4e79"
    grid = "#d9dee8"
    draw.rectangle((x, y, x + table_width, y + header_h), fill=header_fill)
    current_x = x
    for col, width in zip(columns, widths, strict=True):
        draw_cell_text(
            draw,
            (current_x, y, current_x + width, y + header_h),
            col,
            header_font,
            "#ffffff",
            "center",
        )
        current_x += width

    y += header_h
    for row_index, row in enumerate(table_df[columns].itertuples(index=False), start=1):
        fill = "#ffffff" if row_index % 2 else "#eef3f8"
        draw.rectangle((x, y, x + table_width, y + row_h), fill=fill)
        current_x = x
        for col, value, width in zip(columns, row, widths, strict=True):
            if col == "综合排名":
                text = str(int(value))
                align = "center"
            elif col.startswith("RPS"):
                text = f"{float(value):.2f}"
                align = "right"
            else:
                text = str(value)
                align = "left" if col == "名称" else "center"
            draw_cell_text(
                draw,
                (current_x, y, current_x + width, y + row_h),
                text,
                cell_font,
                "#1f2937",
                align,
            )
            draw.line((current_x, y, current_x, y + row_h), fill=grid)
            current_x += width
        draw.line((x + table_width, y, x + table_width, y + row_h), fill=grid)
        draw.line((x, y + row_h, x + table_width, y + row_h), fill=grid)
        y += row_h

    footer = f"生成时间：{now_china().strftime('%Y-%m-%d %H:%M:%S')} 北京时间"
    draw.text((margin_x, y + 10), footer, font=footer_font, fill="#697386")

    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    image.save(out_file)
    return out_file


def render_rps_image(
    db_path: str | Path,
    out_path: str | Path,
    top_n: int = 100,
    font_path: str | None = None,
) -> dict[str, Any]:
    price_df = load_daily_data(db_path)
    table_df, as_of = compute_rps_table(price_df, top_n=top_n)
    output = render_table_image(table_df, as_of, out_path, font_path=font_path)
    return {"out": str(output), "rows": len(table_df), "as_of": as_of.isoformat()}


def main() -> int:
    args = parse_args()
    try:
        if args.command == "update":
            summary = update_database(args.db, args.datalen, args.workers, args.timeout)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0

        if args.command == "render":
            summary = render_rps_image(args.db, args.out, args.top_n, args.font_path)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            print(f"IMAGE_PATH={summary['out']}")
            return 0

        if args.command == "run":
            update_summary = update_database(args.db, args.datalen, args.workers, args.timeout)
            render_summary = render_rps_image(args.db, args.out, args.top_n, args.font_path)
            print(
                json.dumps(
                    {"update": update_summary, "render": render_summary},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            print(f"IMAGE_PATH={render_summary['out']}")
            return 0

        raise ValueError(f"Unknown command: {args.command}")
    except Exception as exc:  # noqa: BLE001 - CLI should report readable errors.
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
