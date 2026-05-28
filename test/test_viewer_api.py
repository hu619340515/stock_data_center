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

        self.db = DuckDBManager(db_path=self.temp_db)
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

    def test_delete_with_code_uses_parameterized_where(self):
        response = self.client.post("/api/delete", json={
            "table": "stock_daily",
            "code": "sh.600000",
            "confirm": True,
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["deleted"], 1)
        db = DuckDBManager(db_path=self.temp_db)
        remaining = db.con.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
        db.close()
        self.assertEqual(remaining, 1)

    def test_summary_by_code_accepts_date_range(self):
        response = self.client.get("/api/summary_by_code?table=stock_daily&start=2024-01-01&end=2024-01-31")

        self.assertEqual(response.status_code, 200)
        rows = response.get_json()
        self.assertEqual(len(rows), 2)
        self.assertIn("code", rows[0])

    def test_logs_endpoint_returns_log_state(self):
        response = self.client.get("/api/logs?limit=5")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("logs", body)
        self.assertIn("log_path", body)


if __name__ == "__main__":
    unittest.main()
