import os
import tempfile
import unittest

import duckdb
import pandas as pd

import database
from database import DuckDBManager
import viewer.server as server


class TestViewerApi(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.mktemp(suffix=".db")
        self.temp_factor_db = tempfile.mktemp(suffix=".db")
        self.temp_etf_factor_db = tempfile.mktemp(suffix=".db")
        self.original_stock_path = server.STOCK_DB_PATH
        self.original_etf_path = server.ETF_DB_PATH
        self.original_stock_factor_path = server.STOCK_FACTOR_DB_PATH
        self.original_etf_factor_path = server.ETF_FACTOR_DB_PATH
        server.STOCK_DB_PATH = self.temp_db
        server.ETF_DB_PATH = self.temp_db
        server.STOCK_FACTOR_DB_PATH = self.temp_factor_db
        server.ETF_FACTOR_DB_PATH = self.temp_etf_factor_db
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

        self.db = DuckDBManager(db_path=self.temp_db, asset_type="stock", factor_db_path=self.temp_factor_db)
        info_df = pd.DataFrame({
            "code": ["sh.600000", "sh.600001"],
            "name": ["\u6d66\u53d1\u94f6\u884c", "\u6d4b\u8bd5\u80a1\u7968"],
        })
        self.db.save_asset_info(info_df, asset_type="stock")
        df = pd.DataFrame({
            "code": ["sh.600000", "sh.600001"],
            "name": ["浦发银行", "测试股票"],
            "date": ["2024-01-02", "2024-01-03"],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.0, 19.0],
            "close": [10.5, 20.5],
            "preclose": [9.8, 19.8],
            "volume": [1000, 2000],
            "amount": [10000.0, 40000.0],
            "adjustflag": ["2", "2"],
            "turn": [1.0, 2.0],
            "tradestatus": ["1", "1"],
            "pctChg": [1.2, -0.5],
            "isST": ["0", "0"],
        })
        self.db.upload_batch([df])
        self.factor_con = duckdb.connect(self.temp_factor_db)
        database._create_factor_tables_for_connection(self.factor_con, "stock")
        self.factor_con.execute("""
            INSERT INTO factor_rps_daily (
                code, date, ret_20, ret_50, ret_120, ret_250,
                rps_20, rps_50, rps_120, rps_250, universe, factor_version
            ) VALUES (
                'sh.600000', '2024-01-02', 0.01, 0.02, 0.03, 0.04,
                80, 70, 60, 50, 'all_stocks', 'rps_v1'
            )
        """)
        self.factor_con.commit()
        self.factor_con.close()
        self.factor_con = None
        self.db.close()
        self.db = None

    def tearDown(self):
        if self.db is not None:
            self.db.close()
        if getattr(self, "factor_con", None) is not None:
            self.factor_con.close()
        server.STOCK_DB_PATH = self.original_stock_path
        server.ETF_DB_PATH = self.original_etf_path
        server.STOCK_FACTOR_DB_PATH = self.original_stock_factor_path
        server.ETF_FACTOR_DB_PATH = self.original_etf_factor_path
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)
        if os.path.exists(self.temp_factor_db):
            os.remove(self.temp_factor_db)
        if os.path.exists(self.temp_etf_factor_db):
            os.remove(self.temp_etf_factor_db)

    def test_delete_requires_confirm(self):
        response = self.client.post("/api/delete", json={
            "table": "stock_daily",
            "code": "sh.600000",
        })

        self.assertEqual(response.status_code, 400)
        self.assertIn("confirm", response.get_json()["msg"])

    def test_repair_latest_derived_fields_requires_confirm(self):
        response = self.client.post("/api/repair_latest_derived_fields", json={})

        self.assertEqual(response.status_code, 400)
        self.assertIn("confirm", response.get_json()["msg"])

    def test_delete_with_code_uses_parameterized_where(self):
        response = self.client.post("/api/delete", json={
            "table": "stock_daily",
            "code": "sh.600000",
            "confirm": True,
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["deleted"], 1)
        db = DuckDBManager(db_path=self.temp_db, asset_type="stock")
        remaining = db.con.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
        db.close()
        self.assertEqual(remaining, 1)

    def test_summary_by_code_accepts_date_range(self):
        response = self.client.get("/api/summary_by_code?table=stock_daily&start=2024-01-01&end=2024-01-31")

        self.assertEqual(response.status_code, 200)
        rows = response.get_json()
        self.assertEqual(len(rows), 2)
        self.assertIn("code", rows[0])
        self.assertIn("name", rows[0])

    def test_table_includes_name_from_info_table(self):
        response = self.client.get("/api/table?table=stock_daily&page=0&page_size=10")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertIn("name", body["columns"])
        self.assertEqual(body["rows"][0]["name"], "\u6d4b\u8bd5\u80a1\u7968")

    def test_table_keyword_search_matches_name(self):
        response = self.client.get("/api/table?table=stock_daily&page=0&page_size=10&keyword=\u6d66\u53d1")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["rows"][0]["code"], "sh.600000")
        self.assertEqual(body["rows"][0]["name"], "\u6d66\u53d1\u94f6\u884c")

    def test_top_movers_includes_name(self):
        response = self.client.get("/api/top_movers?table=stock_daily&date=2024-01-02&limit=5")

        self.assertEqual(response.status_code, 200)
        rows = response.get_json()["data"]
        self.assertEqual(rows[0]["name"], "\u6d66\u53d1\u94f6\u884c")

    def test_pct_chg_distribution_uses_fixed_market_bands(self):
        db = DuckDBManager(db_path=self.temp_db, asset_type="stock")
        db.con.execute("""
            INSERT INTO stock_daily (
                code, date, open, high, low, close, preclose, volume, amount,
                adjustflag, turn, tradestatus, pctChg, isST
            ) VALUES
                ('sh.600002', '2024-01-02', 10, 11, 9, 11, 10, 1000, 11000, '2', 1, '1', 10, '0'),
                ('sh.600003', '2024-01-02', 10, 11, 9, 9, 10, 1000, 9000, '2', 1, '1', -10, '0'),
                ('sh.600004', '2024-01-02', 10, 10, 10, 10, 10, 1000, 10000, '2', 1, '1', 0, '0')
        """)
        db.con.commit()
        db.close()

        response = self.client.get("/api/distribution?table=stock_daily&field=pctChg&date=2024-01-02")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        counts_by_bin = dict(zip(body["bins"], body["counts"]))
        self.assertEqual(counts_by_bin["10~20%"], 1)
        self.assertEqual(counts_by_bin["-20~-10%"], 1)
        self.assertEqual(counts_by_bin["0%"], 1)
        self.assertEqual(body["rise_count"], 2)
        self.assertEqual(body["fall_count"], 1)
        self.assertEqual(body["flat_count"], 1)

    def test_export_includes_name(self):
        response = self.client.get("/api/export?table=stock_daily&limit=5")

        self.assertEqual(response.status_code, 200)
        csv_text = response.data.decode("utf-8-sig")
        self.assertTrue(csv_text.startswith("code,name,date,"))

    def test_logs_endpoint_returns_log_state(self):
        response = self.client.get("/api/logs?limit=5")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("logs", body)
        self.assertIn("log_path", body)

    def test_dashboard_returns_health_summary_and_quality_issues(self):
        response = self.client.get("/api/dashboard?refresh=1")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertIn("health_score", body)
        self.assertIn("summary", body)
        self.assertEqual(body["summary"]["stock_daily_count"], 2)
        self.assertIn("issues", body)

    def test_security_search_matches_name(self):
        response = self.client.get("/api/security_search?asset_type=stock&keyword=\u6d66\u53d1")

        self.assertEqual(response.status_code, 200)
        rows = response.get_json()
        self.assertEqual(rows, [{"code": "sh.600000", "name": "\u6d66\u53d1\u94f6\u884c"}])

    def test_trend_rejects_unknown_field(self):
        response = self.client.get("/api/trend?table=stock_daily&field=close)%20FROM%20stock_daily")

        self.assertEqual(response.status_code, 400)
        self.assertIn("\u4e0d\u652f\u6301\u7684\u5b57\u6bb5", response.get_json()["msg"])

    def test_factor_table_includes_name_and_supports_keyword(self):
        response = self.client.get("/api/table?table=factor_rps_daily&page=0&page_size=10&keyword=\u6d66\u53d1")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertIn("name", body["columns"])
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["rows"][0]["name"], "\u6d66\u53d1\u94f6\u884c")

    def test_factor_table_adds_rps_score_and_sorts_it(self):
        factor_con = duckdb.connect(self.temp_factor_db)
        factor_con.execute("""
            INSERT INTO factor_rps_daily (
                code, date, ret_20, ret_50, ret_120, ret_250,
                rps_20, rps_50, rps_120, rps_250, universe, factor_version
            ) VALUES
                (
                    'sh.600002', '2024-01-02', 0.09, 0.09, 0.09, 0.09,
                    99, 98, 97, 96, 'all_stocks', 'rps_v1'
                ),
                (
                    'sh.600000', '2024-01-03', 0.01, 0.01, 0.01, 0.01,
                    10, 20, 30, 40, 'all_stocks', 'rps_v1'
                ),
                (
                    'sh.600001', '2024-01-03', 0.02, 0.03, 0.04, 0.05,
                    95, 85, 75, 65, 'all_stocks', 'rps_v1'
                )
        """)
        factor_con.commit()
        factor_con.close()

        response = self.client.get("/api/table?table=factor_rps_daily&page=0&page_size=10&sort=rps_score&order=desc")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        columns = body["columns"]
        self.assertEqual(columns[:4], ["rps_score", "code", "name", "rps_5"])
        self.assertLess(columns.index("rps_250"), columns.index("ret_5"))
        self.assertEqual(columns[-1], "date")
        self.assertEqual(body["total"], 2)
        self.assertEqual(body["rows"][0]["code"], "sh.600001")
        self.assertEqual(body["rows"][0]["rps_score"], 320)
        self.assertTrue(all("03 Jan 2024" in row["date"] for row in body["rows"]))

        asc_response = self.client.get("/api/table?table=factor_rps_daily&page=0&page_size=10&sort=rps_score&order=asc")
        self.assertEqual(asc_response.status_code, 200)
        self.assertEqual(asc_response.get_json()["rows"][0]["code"], "sh.600000")

        date_filtered = self.client.get(
            "/api/table?table=factor_rps_daily&page=0&page_size=10"
            "&sort=rps_score&order=desc&start=2024-01-01&end=2024-01-02"
        )
        self.assertEqual(date_filtered.status_code, 200)
        date_filtered_body = date_filtered.get_json()
        self.assertEqual(date_filtered_body["total"], 2)
        self.assertTrue(all("03 Jan 2024" in row["date"] for row in date_filtered_body["rows"]))

        keyword_response = self.client.get(
            "/api/table?table=factor_rps_daily&page=0&page_size=10"
            "&sort=rps_score&order=desc&keyword=\u6d66\u53d1"
        )
        self.assertEqual(keyword_response.status_code, 200)
        keyword_body = keyword_response.get_json()
        self.assertEqual(keyword_body["total"], 1)
        self.assertEqual(keyword_body["rows"][0]["code"], "sh.600000")

        regular_response = self.client.get("/api/table?table=factor_rps_daily&page=0&page_size=10")
        self.assertEqual(regular_response.status_code, 200)
        self.assertEqual(regular_response.get_json()["total"], 4)

    def test_factor_table_serializes_nan_as_null(self):
        factor_con = duckdb.connect(self.temp_factor_db)
        factor_con.execute("""
            INSERT INTO factor_rps_daily (
                code, date, ret_20, ret_50, ret_120, ret_250,
                rps_20, rps_50, rps_120, rps_250, universe, factor_version
            ) VALUES (
                'sh.600001', '2024-01-03', 0.01, 0.02, 0.03, CAST('NaN' AS DOUBLE),
                80, 70, 60, 50, 'all_stocks', 'rps_v1'
            )
        """)
        factor_con.commit()
        factor_con.close()

        response = self.client.get("/api/table?table=factor_rps_daily&page=0&page_size=10&keyword=600001")

        self.assertEqual(response.status_code, 200)
        row = response.get_json()["rows"][0]
        self.assertIsNone(row["ret_250"])

    def test_delete_preview_returns_matching_count(self):
        response = self.client.post("/api/delete_preview", json={
            "table": "stock_daily",
            "code": "sh.600000",
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["count"], 1)


if __name__ == "__main__":
    unittest.main()
