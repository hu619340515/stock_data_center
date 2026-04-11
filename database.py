import duckdb
import pandas as pd
import os
from config import DATABASE_PATH
from logger_config import setup_logger

logger = setup_logger("Database")

class DuckDBManager:
    def __init__(self):
        self.con = duckdb.connect(DATABASE_PATH)
        self._create_table()
        logger.info(f"✅ DuckDB 初始化完成 (文件: {DATABASE_PATH})")

    def _create_table(self):
        """
        创建股票日线数据表
        注意：DuckDB 不支持在 CREATE TABLE 中直接使用 ORDER BY
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
        
        # ✅ 优化：手动创建索引以加速查询
        # 如果索引已存在，IGNORE 会避免报错
        try:
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_code_date ON stock_daily (code, date)")
        except Exception as e:
            logger.warning(f"索引创建提示: {e}")

    def upload_df(self, df: pd.DataFrame) -> bool:
        """单只股票写入（保留用于兼容）"""
        if df.empty: return False
        try:
            df_clean = self._clean_data(df)
            self.con.execute("INSERT OR REPLACE INTO stock_daily SELECT * FROM df_clean")
            return True
        except Exception as e:
            logger.error(f"❌ 写入失败: {e}")
            return False

    def upload_batch(self, df_list: list) -> int:
        """
        🚀 批量写入优化
        """
        if not df_list: return 0
        
        try:
            combined_df = pd.concat(df_list, ignore_index=True)
            if combined_df.empty: return 0

            df_clean = self._clean_data(combined_df)
            count = len(df_clean)
            self.con.execute("INSERT OR REPLACE INTO stock_daily SELECT * FROM df_clean")
            
            logger.info(f"💾 批量写入完成: {count} 条记录")
            return count
            
        except Exception as e:
            logger.error(f"❌ 批量写入失败: {e}")
            return 0

    def get_finished_stocks(self) -> set:
        """
        🛡️ 断点续传支持
        """
        try:
            res = self.con.execute("SELECT DISTINCT code FROM stock_daily").fetchall()
            return {row[0] for row in res}
        except Exception as e:
            logger.error(f"查询已存在股票失败: {e}")
            return set()

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

    def vacuum(self):
        """压缩数据库，释放空间"""
        logger.info("🧹 正在执行数据库维护...")
        try:
            self.con.execute("FORCE CHECKPOINT")
            logger.info("✅ 数据库维护完成")
        except Exception as e:
            logger.error(f"维护失败: {e}")
            
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗：类型转换 + 列顺序重排"""
        df_copy = df.copy()
        if 'date' in df_copy.columns:
            df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
        
        target_columns = [
            'code', 'date', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'
        ]
        return df_copy[target_columns]

    def close(self):
        self.con.close()