import pandas as pd
from multiprocessing import Process, Queue, current_process
import logging # ✅ 单独导入标准 logging 模块
from database import DuckDBManager
from data_source import BaoStockClient
from config import MAX_WORKERS, START_DATE_FULL

# 配置日志
logger = logging.getLogger("Core")

# 🚀 批量写入配置
BATCH_SIZE = 50

def safe_print(msg):
    """
    🛡️ 安全打印：防止多进程日志乱码
    """
    # 获取 logging 的锁，确保同一时间只有一个进程在输出
    # 注意：这里使用的是标准 logging 的锁
    with logging._lock:
        print(msg)

def worker_download(process_id, stock_list, start_date, end_date, result_queue):
    """
    🏭 子进程：负责下载 + 实时打印进度
    """
    client = BaoStockClient()
    client.login()
    
    # 实时显示进程启动
    safe_print(f"🚀 [进程 {process_id}] 启动，任务量: {len(stock_list)} 只")
    
    try:
        total = len(stock_list)
        for i, (_, row) in enumerate(stock_list.iterrows()):
            code = row['code']
            name = row['code_name']
            
            try:
                # 1. 下载数据
                df = client.get_stock_history(code, start_date, end_date)
                
                # 2. 实时反馈
                if not df.empty:
                    # 将数据放入队列（供主进程入库）
                    result_queue.put(df)
                    
                    # 实时打印进度（每 10 只打印一次，或者最后一只打印，避免刷屏太快）
                    if (i + 1) % 10 == 0 or (i + 1) == total:
                        safe_print(f"✅ [进程 {process_id}] 进度: {i+1}/{total} (当前: {code})")
                else:
                    result_queue.put(None)
                    
            except Exception as e:
                safe_print(f"❌ [进程 {process_id}] {code} 下载异常: {e}")
                result_queue.put(None)
    finally:
        client.logout()
        safe_print(f"🏁 [进程 {process_id}] 任务结束")

class StockDataPipeline:
    def __init__(self):
        self.db = DuckDBManager()
        self.stock_client = BaoStockClient()

    def full_download_pipeline(self):
        """
        🚀 多进程下载 + 实时进度显示
        """
        safe_print(f"🚀 启动多进程下载 (进程数: {MAX_WORKERS})...")
        
        # 1. 获取股票列表
        all_stocks = self.stock_client.get_stock_list()
        if all_stocks.empty: return

        # 2. 断点续传
        existing_codes = self.db.get_finished_stocks()
        safe_print(f"ℹ️ 数据库中已有 {len(existing_codes)} 只股票，将自动跳过。")
        
        todo_stocks = all_stocks[~all_stocks['code'].isin(existing_codes)]
        
        if todo_stocks.empty:
            safe_print("✅ 所有股票已下载完成，无需操作。")
            return

        # 3. 切分任务
        chunks = []
        chunk_size = len(todo_stocks) // MAX_WORKERS
        for i in range(MAX_WORKERS):
            start_idx = i * chunk_size
            if i == MAX_WORKERS - 1:
                chunks.append(todo_stocks.iloc[start_idx:])
            else:
                chunks.append(todo_stocks.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        # 4. 启动多进程
        data_queue = Queue()
        processes = []
        end_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_download, args=(i+1, chunk, START_DATE_FULL, end_date, data_queue))
            p.start()
            processes.append(p)
        
        # 5. 主进程：监听队列并入库
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer = []
        
        while any(p.is_alive() for p in processes) or not data_queue.empty():
            try:
                df = data_queue.get(timeout=0.5)
                if df is not None:
                    batch_buffer.append(df)
                
                # 批量写入
                if len(batch_buffer) >= BATCH_SIZE:
                    self.db.upload_batch(batch_buffer)
                    batch_buffer = []
                    safe_print(f"💾 [主进程] 完成一批入库")
                    
            except:
                pass

        # 6. 等待所有进程结束
        for p in processes:
            p.join()

        # 7. 写入最后剩余的数据
        if batch_buffer:
            self.db.upload_batch(batch_buffer)

        safe_print(f"✅ 全量流水线结束")
        self.db.vacuum()

    def daily_update_pipeline(self):
        """
        🔄 增量更新 (保持单进程)
        """
        safe_print("🔄 启动增量更新流水线...")
        stocks = self.stock_client.get_stock_list()
        if stocks.empty: return

        batch_buffer = []

        for _, row in stocks.iterrows():
            code = row['code']
            last_date = self.db.get_last_date(code)
            
            if last_date:
                start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = "1999-01-01"
            
            end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
            
            if start_date <= end_date:
                try:
                    df = self.stock_client.get_stock_history(code, start_date, end_date)
                    if not df.empty:
                        batch_buffer.append(df)
                        safe_print(f"✅ 增量更新: {code}")
                except Exception as e:
                    safe_print(f"❌ {code} 更新失败: {e}")

            if len(batch_buffer) >= 20:
                self.db.upload_batch(batch_buffer)
                batch_buffer = []

        if batch_buffer:
            self.db.upload_batch(batch_buffer)

        safe_print("✅ 增量更新完成")