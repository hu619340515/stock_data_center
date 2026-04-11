import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from database import DuckDBManager  # ✅ 修改：导入 DuckDBManager
from data_source import BaoStockClient
from config import MAX_WORKERS
from logger_config import setup_logger

logger = setup_logger("Core")

class StockDataPipeline:
    def __init__(self):
        self.db = DuckDBManager()      # ✅ 修改：实例化 DuckDBManager
        self.stock_client = BaoStockClient()

    def _process_single_stock(self, stock_code: str, stock_name: str, start_date: str, end_date: str, retries=3) -> dict:
        """
        单只股票处理（带重试机制）
        """
        for i in range(retries):
            try:
                # 1. 下载
                df = self.stock_client.get_stock_history(stock_code, start_date, end_date)
                
                # 如果是网络错误导致返回空，且还有重试次数，则等待后重试
                if df.empty and i < retries - 1:
                    logger.warning(f"⚠️ {stock_code} 下载为空/失败，{1}秒后重试 ({i+1}/{retries})...")
                    time.sleep(1)
                    continue
                
                if df.empty:
                    return {"code": stock_code, "status": "no_data", "rows": 0}

                # 2. 入库
                success = self.db.upload_df(df) # ✅ 修改：调用 db.upload_df
                status = "success" if success else "upload_failed"
                
                return {"code": stock_code, "status": status, "rows": len(df)}
            
            except Exception as e:
                if i < retries - 1:
                    logger.error(f"❌ {stock_code} 异常: {e}，正在重试...")
                    time.sleep(1)
                else:
                    return {"code": stock_code, "status": f"error: {str(e)}", "rows": 0}
        
        return {"code": stock_code, "status": "failed_after_retries", "rows": 0}

    def full_download_pipeline(self):
        """
        全量下载流水线
        """
        logger.info("🚀 启动全量流式下载流水线 (DuckDB版)...")
        stocks = self.stock_client.get_stock_list()
        
        if stocks.empty:
            logger.error("❌ 未获取到股票列表")
            return

        success_count = 0
        fail_count = 0

        # 🚀 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_code = {
                executor.submit(
                    self._process_single_stock, 
                    row['code'], 
                    row['code_name'], 
                    "1999-01-01", 
                    pd.Timestamp.now().strftime("%Y-%m-%d")
                ): row['code'] 
                for _, row in stocks.iterrows()
            }

            for future in as_completed(future_to_code):
                result = future.result()
                if result["status"] == "success":
                    success_count += 1
                else:
                    fail_count += 1
                # 简化日志输出
                logger.info(f"📊 {result['code']} | {result['status']} | 行数: {result['rows']}")

        logger.info(f"✅ 全量流水线结束 | 成功: {success_count} | 失败: {fail_count}")

    def daily_update_pipeline(self):
        """
        增量更新流水线
        """
        logger.info("🔄 启动增量更新流水线...")
        stocks = self.stock_client.get_stock_list()
        
        if stocks.empty:
            return

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for _, row in stocks.iterrows():
                code = row['code']
                # 获取最后日期
                last_date = self.db.get_last_date(code) # ✅ 修改：调用 db.get_last_date
                
                if last_date:
                    # 注意：last_date 已经是 datetime.date 对象
                    # 需要将其转换为字符串格式以便比较
                    last_date_str = last_date.strftime("%Y-%m-%d")
                    start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_date = "1999-01-01"
                
                end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
                
                if start_date <= end_date:
                    futures.append(
                        executor.submit(self._process_single_stock, code, row['code_name'], start_date, end_date)
                    )

            for future in as_completed(futures):
                result = future.result()
                logger.info(f"📊 增量更新: {result}")

        logger.info("✅ 增量更新完成")