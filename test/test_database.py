import unittest
import os
import tempfile
from database import DuckDBManager

class TestDatabase(unittest.TestCase):
    """数据库操作测试"""
    
    def setUp(self):
        """设置测试环境"""
        # 创建临时数据库文件
        self.temp_db = tempfile.mktemp(suffix='.db')
        self.db = DuckDBManager(db_path=self.temp_db, asset_type="stock")
    
    def tearDown(self):
        """清理测试环境"""
        self.db.close()
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)
    
    def test_create_tables(self):
        """测试创建表"""
        # 检查表是否存在
        tables = self.db.con.execute("SHOW TABLES").df()
        self.assertIn('stock_daily', tables['name'].values)
        self.assertIn('stock_weekly', tables['name'].values)
        self.assertIn('stock_monthly', tables['name'].values)
    
    def test_upload_batch(self):
        """测试批量上传数据"""
        import pandas as pd
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.1],
            'high': [10.2, 10.3],
            'low': [9.9, 10.0],
            'close': [10.1, 10.2],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1200000],
            'amount': [10100000.0, 12240000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.12],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 0.99],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 验证数据是否正确插入
        result = self.db.con.execute("SELECT * FROM stock_daily").df()
        self.assertEqual(len(result), 2)
        self.assertEqual(result['code'].iloc[0], 'sh.600000')
    
    def test_get_last_date(self):
        """测试获取最后日期"""
        import pandas as pd
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.1],
            'high': [10.2, 10.3],
            'low': [9.9, 10.0],
            'close': [10.1, 10.2],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1200000],
            'amount': [10100000.0, 12240000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.12],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 0.99],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 获取最后日期
        last_date = self.db.get_last_date('sh.600000')
        self.assertEqual(last_date, '2024-01-02')
    
    def test_get_missing_date_ranges(self):
        """测试获取缺失日期范围"""
        import pandas as pd
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-03'],
            'open': [10.0, 10.2],
            'high': [10.2, 10.4],
            'low': [9.9, 10.1],
            'close': [10.1, 10.3],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1300000],
            'amount': [10100000.0, 13390000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.13],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 1.98],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 获取缺失日期范围
        missing_ranges = self.db.get_missing_date_ranges('sh.600000', '2024-01-01', '2024-01-03')
        self.assertEqual(len(missing_ranges), 1)
        self.assertEqual(missing_ranges[0], ('2024-01-02', '2024-01-02'))
    
    def test_export_data(self):
        """测试导出数据"""
        import pandas as pd
        import tempfile
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.1],
            'high': [10.2, 10.3],
            'low': [9.9, 10.0],
            'close': [10.1, 10.2],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1200000],
            'amount': [10100000.0, 12240000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.12],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 0.99],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 导出数据
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            output_file = f.name
        
        try:
            success = self.db.export_data('sh.600000', '2024-01-01', '2024-01-02', output_file, 'd', 'csv')
            self.assertTrue(success)
            self.assertTrue(os.path.exists(output_file))
            
            # 验证导出的数据
            exported_df = pd.read_csv(output_file)
            self.assertEqual(len(exported_df), 2)
        finally:
            if os.path.exists(output_file):
                os.remove(output_file)

    def test_repair_latest_derived_fields(self):
        """Repair latest-day fields that can be reconstructed locally."""
        import pandas as pd

        self.db.save_asset_info(pd.DataFrame({
            "code": ["sh.600053"],
            "name": ["*ST九鼎"],
        }))
        self.db.upload_batch([pd.DataFrame({
            "code": ["sh.600053", "sh.600053"],
            "date": ["2024-01-01", "2024-01-02"],
            "open": [9.0, 10.0],
            "high": [10.0, 11.0],
            "low": [9.0, 10.0],
            "close": [10.0, 11.0],
            "preclose": [9.0, 0.0],
            "volume": [1000, 1200],
            "amount": [10000.0, 13200.0],
            "adjustflag": ["2", "2"],
            "turn": [1.0, 0.0],
            "tradestatus": ["1", "1"],
            "pctChg": [11.1111, 0.0],
            "isST": ["0", "0"],
        })])

        result = self.db.repair_latest_derived_fields()

        self.assertEqual(result["latest_date"], "2024-01-02")
        self.assertEqual(result["preclose_repaired"], 1)
        self.assertEqual(result["pctchg_repaired"], 1)
        self.assertEqual(result["st_repaired"], 1)
        repaired = self.db.con.execute("""
            SELECT preclose, pctChg, isST, turn
            FROM stock_daily
            WHERE code = 'sh.600053' AND date = '2024-01-02'
        """).fetchone()
        self.assertEqual(repaired, (10.0, 10.0, "1", 0.0))

    def test_rebuild_trade_calendar_from_daily(self):
        """Rebuild open and closed dates with adjacent trading-day links."""
        import pandas as pd

        self.db.upload_batch([pd.DataFrame({
            "code": ["sh.600000", "sh.600000"],
            "date": ["2024-01-01", "2024-01-03"],
            "open": [10.0, 10.2],
            "high": [10.2, 10.4],
            "low": [9.9, 10.1],
            "close": [10.1, 10.3],
            "preclose": [10.0, 10.1],
            "volume": [1000, 1200],
            "amount": [10100.0, 12360.0],
            "adjustflag": ["2", "2"],
            "turn": [1.0, 1.2],
            "tradestatus": ["1", "1"],
            "pctChg": [1.0, 1.98],
            "isST": ["0", "0"],
        })])

        result = self.db.rebuild_trade_calendar_from_daily()

        self.assertEqual(result["calendar_rows"], 3)
        self.assertEqual(result["open_days"], 2)
        self.assertEqual(result["closed_days"], 1)
        rows = self.db.con.execute("""
            SELECT date, is_open, prev_trade_date, next_trade_date, source
            FROM trade_calendar
            ORDER BY date
        """).fetchall()
        self.assertEqual(rows, [
            (pd.Timestamp("2024-01-01").date(), True, None, pd.Timestamp("2024-01-03").date(), "derived:stock_daily"),
            (pd.Timestamp("2024-01-02").date(), False, pd.Timestamp("2024-01-01").date(), pd.Timestamp("2024-01-03").date(), "derived:stock_daily"),
            (pd.Timestamp("2024-01-03").date(), True, pd.Timestamp("2024-01-01").date(), None, "derived:stock_daily"),
        ])

    def test_calculate_rps_daily_ranks_cross_sectional_returns(self):
        """测试RPS按同一交易日的横截面收益率排名"""
        import pandas as pd

        dates = pd.date_range("2024-01-01", periods=21, freq="D")
        rows = []
        for code, last_close in [("sh.600000", 200.0), ("sh.600001", 110.0)]:
            for index, date in enumerate(dates):
                close = last_close if index == 20 else 100.0
                rows.append({
                    "code": code,
                    "date": date.strftime("%Y-%m-%d"),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "preclose": close,
                    "volume": 1000,
                    "amount": 100000.0,
                    "adjustflag": "2",
                    "turn": 0.0,
                    "tradestatus": "1",
                    "pctChg": 0.0,
                    "isST": "0",
                })

        self.db.upload_batch([pd.DataFrame(rows)])

        count = self.db.calculate_rps_daily()

        self.assertEqual(count, 42)
        result = self.db.con.execute("""
            SELECT code, ROUND(ret_20, 2), ROUND(rps_20, 2)
            FROM factor_rps_daily
            WHERE date = '2024-01-21'
            ORDER BY code
        """).fetchall()
        self.assertEqual(result, [
            ("sh.600000", 1.0, 100.0),
            ("sh.600001", 0.1, 0.0),
        ])
        log = self.db.con.execute("""
            SELECT status, message
            FROM factor_update_log
            ORDER BY updated_at DESC
            LIMIT 1
        """).fetchone()
        self.assertEqual(log, ("success", "calculated 42 rows"))

    def test_calculate_etf_rps_daily_ranks_cross_sectional_returns(self):
        """ETF RPS uses the ETF universe and writes to the ETF factor tables."""
        import pandas as pd

        etf_temp_db = tempfile.mktemp(suffix=".db")
        etf_db = DuckDBManager(db_path=etf_temp_db, asset_type="etf")
        try:
            dates = pd.date_range("2024-01-01", periods=21, freq="D")
            rows = []
            for code, last_close in [("sh.510300", 150.0), ("sz.159915", 120.0)]:
                for index, date in enumerate(dates):
                    close = last_close if index == 20 else 100.0
                    rows.append({
                        "code": code,
                        "date": date.strftime("%Y-%m-%d"),
                        "open": close,
                        "high": close,
                        "low": close,
                        "close": close,
                        "preclose": close,
                        "volume": 1000,
                        "amount": 100000.0,
                        "adjustflag": "2",
                        "turn": 0.0,
                        "tradestatus": "1",
                        "pctChg": 0.0,
                        "isST": "0",
                    })

            etf_db.upload_batch([pd.DataFrame(rows)], asset_type="etf")
            count = etf_db.calculate_rps_daily()

            self.assertEqual(count, 42)
            result = etf_db.con.execute("""
                SELECT code, ROUND(ret_20, 2), ROUND(rps_20, 2), universe
                FROM etf_factor_rps_daily
                WHERE date = '2024-01-21'
                ORDER BY code
            """).fetchall()
            self.assertEqual(result, [
                ("sh.510300", 0.5, 100.0, "all_etfs"),
                ("sz.159915", 0.2, 0.0, "all_etfs"),
            ])
            log = etf_db.con.execute("""
                SELECT status, message
                FROM etf_factor_update_log
                ORDER BY updated_at DESC
                LIMIT 1
            """).fetchone()
            self.assertEqual(log, ("success", "calculated 42 rows"))
        finally:
            etf_db.close()
            if os.path.exists(etf_temp_db):
                os.remove(etf_temp_db)

    def test_calculate_rps_preserves_original_error(self):
        import database
        import pandas as pd

        self.db.upload_batch([pd.DataFrame({
            "code": ["sh.600000"],
            "date": ["2024-01-01"],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "preclose": [10.0],
            "volume": [1000],
            "amount": [10000.0],
            "adjustflag": ["2"],
            "turn": [1.0],
            "tradestatus": ["1"],
            "pctChg": [0.0],
            "isST": ["0"],
        })])
        original_periods = database.RPS_PERIODS
        database.RPS_PERIODS = original_periods + (999,)
        try:
            with self.assertRaisesRegex(Exception, "ret_999"):
                self.db.calculate_rps_daily()
        finally:
            database.RPS_PERIODS = original_periods

        log = self.db.con.execute("""
            SELECT status, message
            FROM factor_update_log
            ORDER BY updated_at DESC
            LIMIT 1
        """).fetchone()
        self.assertEqual(log[0], "failed")
        self.assertIn("ret_999", log[1])

if __name__ == '__main__':
    unittest.main()
