import duckdb
import pandas as pd
import os
from config import DATABASE_PATH
from logger_config import setup_logger

logger = setup_logger("Database")

class DuckDBManager:
    def __init__(self):
        # 1. 连接到本地文件
        self.con = duckdb.connect(DATABASE_PATH)
        
        # 2. 初始化表结构
        self._create_table()
        logger.info(f"✅ DuckDB 初始化完成 (文件: {DATABASE_PATH})")

    def _create_table(self):
        """
        创建股票日线数据表
        注意：列的顺序很重要！
        """
        sql = """
        CREATE TABLE IF NOT EXISTS stock_daily (
            code VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            preclose DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            adjustflag VARCHAR,
            turn DOUBLE,
            tradestatus VARCHAR,
            pctChg DOUBLE,
            isST VARCHAR,
            PRIMARY KEY (code, date)
        )
        """
        self.con.execute(sql)

    def upload_df(self, df: pd.DataFrame) -> bool:
        """
        将 Pandas DataFrame 写入 DuckDB
        """
        if df.empty:
            return False
            
        try:
            # 1. 数据清洗与重排
            df_clean = self._clean_data(df)
            
            # 2. 直接写入
            # 现在 df_clean 的列顺序已经和数据库表完全一致了
            self.con.execute("""
                INSERT OR REPLACE INTO stock_daily 
                SELECT * FROM df_clean
            """)
            
            logger.info(f"✅ 成功写入 {len(df_clean)} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"❌ 写入失败: {e}")
            return False

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据清洗：类型转换 + 列顺序重排
        """
        df_copy = df.copy()
        
        # --- 1. 类型转换 ---
        
        # 转换日期
        if 'date' in df_copy.columns:
            df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
            
        # 转换数值类型
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
        
        # --- 2. 关键修复：强制列顺序与数据库表结构一致 ---
        # 数据库表顺序：code, date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, isST
        target_columns = [
            'code', 'date', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'
        ]
        
        # 重新排列列，如果原数据中有额外的列会被丢弃，缺少的列会报错（但Baostock通常返回完整数据）
        return df_copy[target_columns]

    def get_last_date(self, stock_code: str):
        """获取某只股票在数据库中的最后交易日"""
        try:
            res = self.con.execute(
                "SELECT MAX(date) FROM stock_daily WHERE code = ?", 
                [stock_code]
            ).fetchone()
            
            return res[0] if res[0] else None
        except Exception as e:
            logger.error(f"查询最后日期失败: {e}")
            return None
            
    def close(self):
        """关闭连接"""
        self.con.close()