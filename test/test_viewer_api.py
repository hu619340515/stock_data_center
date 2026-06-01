import os
import tempfile
import unittest

import pandas as pd

from database import DuckDBManager
import viewer.server as server


class TestViewerApi(unittest.TestCase):
    def setUp(self):
        self.temp_db = tempfile.mktemp(suffix=".db")
        self.original_stock_path = server.STOCK_DB_PATH
        self.original_etf_path = server.ETF_DB_PATH
        server.STOCK_DB_PATH = self.temp_db
        server.ETF_DB_PATH = self.temp_db
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

        self.db = DuckDBManager(db_path=self.temp_db, asset_type="stock")
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
        self.db.con.execute("""
            INSERT INTO factor_rps_daily (
                code, date, ret_20, ret_50, ret_120, ret_250,
                rps_20, rps_50, rps_120, rps_250, universe, factor_version
            ) VALUES (
                'sh.600000', '2024-01-02', 0.01, 0.02, 0.03, 0.04,
                80, 70, 60, 50, 'all_stocks', 'rps_v1'
            )
        """)
        self.db.con.commit()
        self.db.close()
        self.db = None

    def tearDown(self):
        if self.db is not None:
            self.db.close()
        server.STOCK_DB_PATH = self.original_stock_path
        server.ETF_DB_PATH = self.original_etf_path
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

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

    def test_factor_table_serializes_nan_as_null(self):
        db = DuckDBManager(db_path=self.temp_db, asset_type="stock")
        db.con.execute("""
            INSERT INTO factor_rps_daily (
                code, date, ret_20, ret_50, ret_120, ret_250,
                rps_20, rps_50, rps_120, rps_250, universe, factor_version
            ) VALUES (
                'sh.600001', '2024-01-03', 0.01, 0.02, 0.03, CAST('NaN' AS DOUBLE),
                80, 70, 60, 50, 'all_stocks', 'rps_v1'
            )
        """)
        db.close()

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
