import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

import sys

SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPT_DIR))

import rps_etf_sina as app  # noqa: E402


class RpsEtfSinaTests(unittest.TestCase):
    def build_price_frame(self) -> pd.DataFrame:
        dates = pd.date_range("2025-01-01", periods=130, freq="D").date
        rows = []
        for symbol_index in range(5):
            symbol = f"sh51000{symbol_index}"
            for day_index, trade_date in enumerate(dates):
                rows.append(
                    {
                        "symbol": symbol,
                        "code": symbol[2:],
                        "name": f"测试ETF{symbol_index}",
                        "trade_date": trade_date,
                        "close": 100 + day_index * (symbol_index + 1),
                    }
                )

        for trade_date in dates[-60:]:
            rows.append(
                {
                    "symbol": "sz159999",
                    "code": "159999",
                    "name": "短历史ETF",
                    "trade_date": trade_date,
                    "close": 100,
                }
            )
        return pd.DataFrame(rows)

    def test_compute_rps_table_excludes_short_history_and_orders_by_sum(self):
        table, as_of = app.compute_rps_table(self.build_price_frame(), top_n=10)

        self.assertEqual(str(as_of), "2025-05-10")
        self.assertNotIn("159999", set(table["代码"]))
        self.assertEqual(table.iloc[0]["名称"], "测试ETF4")
        self.assertEqual(table.iloc[0]["综合排名"], 1)
        self.assertEqual(table.iloc[0]["RPS5"], 100.0)

    def test_write_database_is_idempotent_for_same_symbol_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "etf.duckdb"
            now = datetime(2025, 1, 1, 9, 0, 0)
            meta_df = pd.DataFrame(
                [
                    {
                        "symbol": "sh510050",
                        "code": "510050",
                        "name": "上证50ETF",
                        "market": "sh",
                        "source": "sina",
                        "updated_at": now,
                    }
                ]
            )
            daily_df = pd.DataFrame(
                [
                    {
                        "symbol": "sh510050",
                        "trade_date": datetime(2025, 1, 2).date(),
                        "open": 1.0,
                        "high": 1.1,
                        "low": 0.9,
                        "close": 1.05,
                        "volume": 1000,
                        "fetched_at": now,
                    }
                ]
            )

            app.write_database(db_path, meta_df, daily_df, datetime(2024, 1, 1).date())
            app.write_database(db_path, meta_df, daily_df, datetime(2024, 1, 1).date())

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                count = con.execute("SELECT count(*) FROM etf_daily").fetchone()[0]
            finally:
                con.close()
            self.assertEqual(count, 1)

    def test_render_table_image_creates_png_when_chinese_font_exists(self):
        try:
            app.find_chinese_font()
        except RuntimeError:
            self.skipTest("No Chinese font available on this host")

        table, as_of = app.compute_rps_table(self.build_price_frame(), top_n=5)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "rps.png"
            result = app.render_table_image(table, as_of, out)
            self.assertTrue(result.exists())
            self.assertGreater(result.stat().st_size, 1000)


if __name__ == "__main__":
    unittest.main()
