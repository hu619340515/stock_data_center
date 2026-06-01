import unittest

import pandas as pd

from core import DB_MARKET_COLUMNS, prepare_market_df_for_db


class TestCoreHelpers(unittest.TestCase):
    def test_prepare_market_df_for_db_excludes_display_name(self):
        df = pd.DataFrame({
            "code": ["sz.159982"],
            "date": ["2026-05-27"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.05],
            "preclose": [1.0],
            "volume": [1000],
            "amount": [1000.0],
            "adjustflag": ["2"],
            "turn": [0.0],
            "tradestatus": ["1"],
            "pctChg": [5.0],
            "isST": ["0"],
        })

        result = prepare_market_df_for_db(df, "测试ETF")

        self.assertEqual(result.columns.tolist(), DB_MARKET_COLUMNS)
        self.assertNotIn("name", result.columns)

    def test_prepare_market_df_for_db_infers_current_st_flag_from_name(self):
        frame = pd.DataFrame({
            "code": ["sh.600053"],
            "date": ["2026-05-29"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "preclose": [1.0],
            "volume": [1],
            "amount": [1.0],
            "adjustflag": ["2"],
            "turn": [0.1],
            "tradestatus": ["1"],
            "pctChg": [0.0],
            "isST": ["0"],
        })

        result = prepare_market_df_for_db(frame, "*ST九鼎")

        self.assertEqual(result["isST"].iloc[0], "1")


if __name__ == "__main__":
    unittest.main()
