import unittest

import pandas as pd

import data_source
from data_source import QMTClient


class TestQMTDataSource(unittest.TestCase):
    """QMT数据源适配层测试（不依赖本机安装xtquant）。"""

    def test_code_conversion(self):
        self.assertEqual(QMTClient._to_qmt_code("sh.600000"), "600000.SH")
        self.assertEqual(QMTClient._to_qmt_code("sz.000001"), "000001.SZ")
        self.assertEqual(QMTClient._from_qmt_code("600000.SH"), "sh.600000")
        self.assertEqual(QMTClient._from_qmt_code("000001.SZ"), "sz.000001")

    def test_standardize_history_df(self):
        client = QMTClient()
        raw = pd.DataFrame({
            "time": ["20240102", "20240103"],
            "open": [10.0, 10.2],
            "high": [10.5, 10.8],
            "low": [9.8, 10.1],
            "close": [10.3, 10.6],
            "volume": [1000, 2000],
            "amount": [10000.0, 21000.0],
            "preClose": [10.0, 10.3],
            "suspendFlag": [0, 1],
        })

        df = client._standardize_history_df(
            raw,
            "sh.600000",
            float_volume=1_000_000,
            security_name="*ST测试",
        )

        self.assertEqual(df.columns.tolist(), QMTClient.TARGET_COLUMNS)
        self.assertEqual(df["code"].iloc[0], "sh.600000")
        self.assertEqual(df["date"].tolist(), ["2024-01-02", "2024-01-03"])
        self.assertEqual(df["adjustflag"].iloc[0], "2")
        self.assertEqual(df["tradestatus"].iloc[0], "1")
        self.assertEqual(df["tradestatus"].iloc[1], "0")
        self.assertAlmostEqual(df["pctChg"].iloc[0], 3.0)
        self.assertAlmostEqual(df["turn"].iloc[0], 10.0)
        self.assertEqual(df["isST"].iloc[0], "1")

    def test_get_stock_history_downloads_from_qmt_cache(self):
        class FakeXtData:
            def __init__(self):
                self.download_calls = []
                self.market_calls = []

            def connect(self, ip, port):
                self.connected = (ip, port)
                return "ok"

            def download_history_data(self, qmt_code, period, start_time, end_time):
                self.download_calls.append((qmt_code, period, start_time, end_time))

            def get_instrument_detail(self, qmt_code):
                return {
                    "InstrumentName": "浦发银行",
                    "FloatVolume": 1_000_000,
                }

            def get_market_data_ex(self, fields, codes, **kwargs):
                self.market_calls.append((fields, codes, kwargs))
                return {
                    "600000.SH": pd.DataFrame({
                        "time": ["20240102"],
                        "open": [10.0],
                        "high": [10.5],
                        "low": [9.8],
                        "close": [10.3],
                        "volume": [1000],
                        "amount": [10000.0],
                        "preClose": [10.0],
                        "suspendFlag": [0],
                    })
                }

        old_xtdata = data_source._XTDATA
        fake = FakeXtData()
        data_source._XTDATA = fake
        try:
            df = QMTClient().get_stock_history("sh.600000", "2024-01-02", "2024-01-03")
        finally:
            data_source._XTDATA = old_xtdata

        self.assertEqual(fake.download_calls, [("600000.SH", "1d", "20240102", "20240103")])
        self.assertEqual(fake.market_calls[0][1], ["600000.SH"])
        self.assertEqual(fake.market_calls[0][0], QMTClient.HISTORY_FIELDS)
        self.assertFalse(df.empty)
        self.assertEqual(df["code"].iloc[0], "sh.600000")
        self.assertEqual(df["date"].iloc[0], "2024-01-02")
        self.assertAlmostEqual(df["pctChg"].iloc[0], 3.0)
        self.assertAlmostEqual(df["turn"].iloc[0], 10.0)


if __name__ == '__main__':
    unittest.main()
