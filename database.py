import pandas as pd
from pymongo import MongoClient, UpdateOne
from config import MONGODB_URI, DATABASE_NAME, COLLECTION_NAME, CHUNK_SIZE
from logger_config import setup_logger
import traceback

logger = setup_logger("Database")

class MongoManager:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]
        self._ensure_indexes()

    def _ensure_indexes(self):
        # 创建唯一索引，防止重复插入
        self.collection.create_index([("code", 1), ("date", 1)], unique=True)
        self.collection.create_index([("date", 1)])
        logger.info(f"✅ 索引确保存在于 {COLLECTION_NAME}")

    def upload_df(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
            
        try:
            df_clean = self._clean_data(df)
            records = df_clean.to_dict('records')
            
            # 🚀 批量写入优化，避免数据量过大时的内存溢出
            success_count = 0
            for i in range(0, len(records), CHUNK_SIZE):
                chunk = records[i:i + CHUNK_SIZE]
                operations = [
                    UpdateOne(
                        {'code': r['code'], 'date': r['date']}, 
                        {'$set': r}, 
                        upsert=True
                    ) for r in chunk
                ]
                self.collection.bulk_write(operations, ordered=False)
                success_count += len(chunk)
            
            logger.info(f"✅ 成功同步 {success_count} 条记录到 {COLLECTION_NAME}")
            return True
        except Exception as e:
            logger.error(f"❌ 上传失败: {e}")
            return False

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
        
        if 'date' in df_copy.columns:
            df_copy['date'] = pd.to_datetime(df_copy['date'])
            
        return df_copy

    def get_last_date(self, stock_code: str):
        record = self.collection.find({'code': stock_code}).sort('date', -1).limit(1)
        record_list = list(record)
        return record_list[0]['date'] if record_list else None