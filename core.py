import pandas as pd
import os
from multiprocessing import Process, Queue, current_process, Event
import logging # ✅ 单独导入标准 logging 模块
from typing import List, Tuple, Optional, Set, Any
from database import DuckDBManager
from data_source_factory import DataSourceFactory
from config import MAX_WORKERS, START_DATE_FULL, START_DATE_FULL_ETF, DYNAMIC_CONCURRENCY, MIN_WORKERS, MAX_WORKERS_LIMIT, ERROR_THRESHOLD, SUCCESS_THRESHOLD, BATCH_SIZE, MAX_BATCH_SIZE, MIN_BATCH_SIZE, MEMORY_THRESHOLD, USE_ARROW, COMPRESS_DATA, ERROR_LOG_FILE, MAX_ERRORS_BEFORE_WARNING, DEFAULT_DATA_SOURCE, ENABLE_DATA_SOURCE_FALLBACK, DATA_SOURCE_PRIORITY
import threading
import time

# 配置日志
logger = logging.getLogger("Core")

# 全局进度状态（线程安全）
_progress_lock = threading.Lock()
_progress_state = {
    'is_running': False,
    'task_name': '',
    'total': 0,
    'processed': 0,
    'success': 0,
    'error': 0,
    'speed': 0,  # 只/秒
    'eta': '',   # 预计剩余时间
    'message': '',
    'start_time': None,
}

def get_progress():
    """获取当前进度状态"""
    with _progress_lock:
        return dict(_progress_state)

def set_progress(**kwargs):
    """更新进度状态"""
    with _progress_lock:
        _progress_state.update(kwargs)

def reset_progress():
    """重置进度状态"""
    with _progress_lock:
        _progress_state.update({
            'is_running': False,
            'task_name': '',
            'total': 0,
            'processed': 0,
            'success': 0,
            'error': 0,
            'speed': 0,
            'eta': '',
            'message': '',
            'start_time': None,
        })

# 尝试导入内存监控和Arrow库
try:
    import psutil
    has_psutil = True
except ImportError:
    has_psutil = False

try:
    if USE_ARROW:
        import pyarrow as pa
        has_arrow = True
    else:
        has_arrow = False
except ImportError:
    has_arrow = False

try:
    if COMPRESS_DATA:
        import zlib
        has_zlib = True
    else:
        has_zlib = False
except ImportError:
    has_zlib = False

def safe_print(msg):
    """
    🛡️ 安全打印：防止多进程日志乱码
    """
    # 获取 logging 的锁，确保同一时间只有一个进程在输出
    # 注意：这里使用的是标准 logging 的锁
    with logging._lock:
        print(msg)

def worker_update(process_id: int, stock_list: pd.DataFrame, result_queue: Queue, frequency: str = "d", stop_event=None, asset_type: str = 'stock'):
    """
    🚀 增量更新的工作进程
    
    Args:
        process_id: 进程ID
        stock_list: 股票列表
        result_queue: 结果队列
        frequency: 数据频率
        stop_event: 停止事件（跨进程共享）
        asset_type: 资产类型 (stock 或 etf)
    """
    from data_source_factory import DataSourceFactory
    from config import DEFAULT_DATA_SOURCE, ETF_DATA_SOURCE, STOCK_DATA_SOURCE
    
    # 根据资产类型选择数据源
    if asset_type == 'etf':
        data_source_type = ETF_DATA_SOURCE
    else:
        data_source_type = STOCK_DATA_SOURCE
    
    # 使用工厂创建数据源
    client = DataSourceFactory.create_data_source(data_source_type)
    client.login()
    
    # 实时显示进程启动
    safe_print(f"🚀 [进程 {process_id}] 启动，任务量: {len(stock_list)} 只，资产类型: {asset_type}，数据源: {client.get_data_source_name()}，频率: {frequency}")
    
    try:
        total = len(stock_list)
        for i, (_, row) in enumerate(stock_list.iterrows()):
            # 检查停止信号
            if stop_event is not None and stop_event.is_set():
                safe_print(f"️ [进程 {process_id}] 收到停止信号，退出")
                break
            
            code = row['code']
            # 从row中获取开始日期和结束日期
            start_date = row.get('start_date', '1999-01-01')
            end_date = row.get('end_date', pd.Timestamp.now().strftime('%Y-%m-%d'))
            
            try:
                # 4. 下载数据
                if start_date <= end_date:
                    safe_print(f"📥 [进程 {process_id}] 增量更新 {code} {start_date} 至 {end_date} 的数据 (频率: {frequency})")
                    df = client.get_stock_history(code, start_date, end_date, frequency)
                    
                    # 5. 将数据放入队列（供主进程入库和去重）
                    if not df.empty:
                        safe_print(f"✅ [进程 {process_id}] {code} 增量更新成功，数据量: {len(df)} 条")
                        result_queue.put((df, True, None))  # (数据, 成功标志, 错误信息)
                    else:
                        safe_print(f"️ [进程 {process_id}] {code} 无新数据")
                        result_queue.put((None, True, f"{code} 无新数据"))  # (无数据, 成功标志, 信息)
                else:
                    safe_print(f"ℹ️ [进程 {process_id}] {code} 数据已最新")
                    result_queue.put((None, True, f"{code} 数据已最新"))  # (无数据, 成功标志, 信息)
                
                # 实时打印进度（每 10 只打印一次，或者最后一只打印，避免刷屏太快）
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    safe_print(f"✅ [进程 {process_id}] 进度: {i+1}/{total} (当前: {code})")
                    
            except Exception as e:
                error_msg = str(e)
                safe_print(f"❌ [进程 {process_id}] {code} 更新异常: {error_msg}")
                result_queue.put((None, False, error_msg))  # (异常, 失败标志, 错误信息)
    finally:
        client.logout()
        safe_print(f"🏁 [进程 {process_id}] 任务结束")

def worker_download(process_id: int, stock_list: pd.DataFrame, start_date: str, end_date: str, result_queue: Queue, frequency: str = "d", stop_event=None, asset_type: str = 'stock'):
    """
    🚀 全量下载的工作进程
    
    Args:
        process_id: 进程ID
        stock_list: 股票列表
        start_date: 开始日期
        end_date: 结束日期
        result_queue: 结果队列
        frequency: 数据频率
        stop_event: 停止事件（跨进程共享）
        asset_type: 资产类型 (stock 或 etf)
    """
    from data_source_factory import DataSourceFactory
    from config import DEFAULT_DATA_SOURCE, ETF_DATA_SOURCE, STOCK_DATA_SOURCE
    
    # 根据资产类型选择数据源
    if asset_type == 'etf':
        data_source_type = ETF_DATA_SOURCE
    else:
        data_source_type = STOCK_DATA_SOURCE
    
    # 使用工厂创建数据源
    client = DataSourceFactory.create_data_source(data_source_type)
    client.login()
    
    # 实时显示进程启动
    safe_print(f"🚀 [进程 {process_id}] 启动，任务量: {len(stock_list)} 只，资产类型: {asset_type}，数据源: {client.get_data_source_name()}，频率: {frequency}")
    logger.info(f"🚀 [进程 {process_id}] 启动，任务量: {len(stock_list)} 只，资产类型: {asset_type}，数据源: {client.get_data_source_name()}，频率: {frequency}")
    
    try:
        total = len(stock_list)
        success_count = 0
        error_count = 0
        
        for i, (_, row) in enumerate(stock_list.iterrows()):
            # 检查停止信号
            if stop_event is not None and stop_event.is_set():
                safe_print(f"⏹️ [进程 {process_id}] 收到停止信号，退出")
                logger.info(f"⏹️ [进程 {process_id}] 收到停止信号，已处理 {i+1}/{total}")
                break
            
            code = row['code']
            name = row['code_name'] if 'code_name' in row else '未知'
            
            try:
                # 1. 直接下载完整日期范围的数据（主进程负责过滤已存在的数据）
                safe_print(f"📥 [进程 {process_id}] 下载 {code} ({name}) {start_date} 至 {end_date} 的数据 (频率: {frequency})")
                logger.info(f"📥 [进程 {process_id}] 开始下载 {code} ({name})，日期范围: {start_date} ~ {end_date}")
                
                df = client.get_stock_history(code, start_date, end_date, frequency)
                
                # 2. 将数据放入队列（供主进程入库和去重）
                if not df.empty:
                    safe_print(f"✅ [进程 {process_id}] {code} 下载成功，数据量: {len(df)} 条")
                    logger.info(f"✅ [进程 {process_id}] {code} 下载成功，数据量: {len(df)} 条")
                    result_queue.put((df, True, None))  # (数据, 成功标志, 错误信息)
                    success_count += 1
                else:
                    safe_print(f"ℹ️ [进程 {process_id}] {code} 无历史数据")
                    logger.info(f"ℹ️ [进程 {process_id}] {code} 无历史数据")
                    result_queue.put((None, True, f"{code} 无历史数据"))  # (无数据, 成功标志, 信息)
                    success_count += 1
                    
                # 实时打印进度（每 10 只打印一次，或者最后一只打印，避免刷屏太快）
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    progress = (i + 1) / total * 100
                    safe_print(f"✅ [进程 {process_id}] 进度: {i+1}/{total} ({progress:.1f}%) (当前: {code})")
                    logger.info(f"📊 [进程 {process_id}] 进度: {i+1}/{total} ({progress:.1f}%)，成功: {success_count}，失败: {error_count}")
                    
            except Exception as e:
                error_msg = str(e)
                safe_print(f"❌ [进程 {process_id}] {code} 下载异常: {error_msg}")
                logger.error(f"❌ [进程 {process_id}] {code} 下载异常: {error_msg}")
                result_queue.put((None, False, error_msg))  # (异常, 失败标志, 错误信息)
                error_count += 1
                
    finally:
        client.logout()
        safe_print(f"🏁 [进程 {process_id}] 任务结束，成功: {success_count}，失败: {error_count}")
        logger.info(f"🏁 [进程 {process_id}] 任务结束，成功: {success_count}，失败: {error_count}")

class StockDataPipeline:
    def __init__(self, db_path=None, use_temp_db=False, asset_type='stock'):
        self.use_temp_db = use_temp_db
        self.temp_db_path = None
        self.should_stop = False
        self.stop_event = Event()
        self.asset_type = asset_type
        
        if use_temp_db:
            import tempfile
            self.temp_db_path = os.path.join(tempfile.gettempdir(), f"temp_{asset_type}_{int(time.time())}.db")
            self.db: DuckDBManager = DuckDBManager(db_path=self.temp_db_path)
            safe_print(f"📁 使用临时数据库: {self.temp_db_path}")
        else:
            # 根据资产类型选择数据库路径
            if not db_path:
                if asset_type == 'etf':
                    db_path = ETF_DB_PATH
                else:
                    db_path = STOCK_DB_PATH
            self.db: DuckDBManager = DuckDBManager(db_path=db_path)
        
        self.current_workers: int = MAX_WORKERS
        self.error_count: int = 0
        self.success_count: int = 0
        self.total_count: int = 0
        self.current_batch_size: int = BATCH_SIZE
        self.start_time: Optional[pd.Timestamp] = None
        self.end_time: Optional[pd.Timestamp] = None
        self.processed_count: int = 0
        self.total_stocks: int = 0
        self.error_log_file: str = ERROR_LOG_FILE
        self._init_error_log()
        
        # 数据源配置
        self.data_source_priority = DATA_SOURCE_PRIORITY
        self.current_data_source_idx = 0
        self.current_data_source = self._get_next_data_source()
    
    def stop(self):
        """停止当前任务"""
        self.should_stop = True
        self.stop_event.set()
        safe_print("⏹️ 收到停止信号，正在停止任务...")
    
    def merge_to_main_db(self, main_db_path: str, tables: list = None) -> bool:
        """将临时数据库合并到主数据库"""
        if not self.use_temp_db or not self.temp_db_path:
            return False
        
        try:
            safe_print(f"🔄 开始合并临时数据库到主数据库...")
            logger.info(f"📊 合并源: {self.temp_db_path}")
            logger.info(f"📊 合并目标: {main_db_path}")
            logger.info(f"📊 合并表: {tables}")
            
            # 首先关闭临时数据库连接，避免连接冲突
            self.db.close()
            safe_print(f"✅ 已关闭临时数据库连接")
            
            # 连接主数据库进行合并
            main_db = DuckDBManager(db_path=main_db_path)
            result = main_db.merge_from_db(self.temp_db_path, tables)
            main_db.close()
            safe_print(f"✅ 主数据库合并完成")
            
            # 删除临时数据库
            if os.path.exists(self.temp_db_path):
                os.remove(self.temp_db_path)
                safe_print(f"🗑️ 已删除临时数据库: {self.temp_db_path}")
            
            logger.info(f"✅ 数据库合并成功")
            return result
        except Exception as e:
            safe_print(f"❌ 合并失败: {e}")
            logger.error(f"❌ 数据库合并失败: {e}")
            return False
    
    def cleanup_temp_db(self):
        """清理临时数据库"""
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            try:
                self.db.close()
                os.remove(self.temp_db_path)
                safe_print(f"🗑️ 已清理临时数据库: {self.temp_db_path}")
            except Exception as e:
                safe_print(f"⚠️ 清理临时数据库失败: {e}")
    
    def _get_next_data_source(self):
        """
        🔄 获取下一个可用的数据源实例
        """
        if self.current_data_source_idx < len(self.data_source_priority):
            source_type = self.data_source_priority[self.current_data_source_idx]
            self.current_data_source_idx += 1
            try:
                source = DataSourceFactory.create_data_source(source_type)
                safe_print(f"✅ 初始化数据源: {source.get_data_source_name()}")
                return source
            except Exception as e:
                safe_print(f"❌ 初始化数据源 {source_type} 失败: {e}")
                return self._get_next_data_source()
        else:
            raise RuntimeError("所有数据源均不可用")
    
    def _try_get_stock_list(self):
        """
        🔄 尝试所有数据源获取股票列表，直到成功
        """
        while self.current_data_source_idx <= len(self.data_source_priority):
            try:
                self.current_data_source.login()
                stock_list = self.current_data_source.get_stock_list()
                if not stock_list.empty:
                    return stock_list
                else:
                    safe_print(f"⚠️ {self.current_data_source.get_data_source_name()} 获取股票列表为空，尝试下一个数据源")
            except Exception as e:
                safe_print(f"⚠️ {self.current_data_source.get_data_source_name()} 获取股票列表失败: {e}")
            
            # 切换到下一个数据源
            self.current_data_source = self._get_next_data_source()
        
        raise RuntimeError("所有数据源均无法获取股票列表")
    
    def _try_get_etf_list(self):
        """
        🔄 尝试所有数据源获取ETF列表，直到成功
        """
        while self.current_data_source_idx <= len(self.data_source_priority):
            try:
                self.current_data_source.login()
                etf_list = self.current_data_source.get_etf_list()
                if not etf_list.empty:
                    return etf_list
                else:
                    safe_print(f"⚠️ {self.current_data_source.get_data_source_name()} 获取ETF列表为空，尝试下一个数据源")
            except Exception as e:
                safe_print(f"⚠️ {self.current_data_source.get_data_source_name()} 获取ETF列表失败: {e}")
            
            # 切换到下一个数据源
            self.current_data_source = self._get_next_data_source()
        
        raise RuntimeError("所有数据源均无法获取ETF列表")

    def _init_error_log(self) -> None:
        """
        📝 初始化错误日志文件
        """
        try:
            with open(self.error_log_file, 'w', encoding='utf-8') as f:
                f.write(f"# 错误日志 - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("# 格式: 时间,股票代码,错误信息\n\n")
        except Exception as e:
            logger.warning(f"⚠️ 初始化错误日志文件失败: {e}")

    def log_error(self, code: str, error_msg: str) -> None:
        """
        📝 记录错误信息到日志文件
        
        Args:
            code: 股票代码
            error_msg: 错误信息
        """
        try:
            with open(self.error_log_file, 'a', encoding='utf-8') as f:
                timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"{timestamp},{code},{error_msg}\n")
        except Exception as e:
            logger.warning(f"⚠️ 写入错误日志失败: {e}")

    def _adjust_concurrency(self) -> None:
        """
        📊 动态调整并发数
        """
        if not DYNAMIC_CONCURRENCY or self.total_count == 0:
            return
        
        error_rate = (self.error_count / self.total_count) * 100
        success_rate = (self.success_count / self.total_count) * 100
        
        if error_rate > ERROR_THRESHOLD and self.current_workers > MIN_WORKERS:
            self.current_workers -= 1
            safe_print(f"📉 错误率过高 ({error_rate:.2f}%)，减少并发数至: {self.current_workers}")
        elif success_rate > SUCCESS_THRESHOLD and self.current_workers < MAX_WORKERS_LIMIT:
            self.current_workers += 1
            safe_print(f"📈 成功率高 ({success_rate:.2f}%)，增加并发数至: {self.current_workers}")

    def _get_memory_usage(self) -> float:
        """
        📊 获取当前内存使用率
        
        Returns:
            内存使用率（0-1）
        """
        if not has_psutil:
            return 0.5  # 默认返回50%使用率
        try:
            return psutil.virtual_memory().percent / 100
        except:
            return 0.5

    def _adjust_batch_size(self) -> None:
        """
        📊 动态调整批处理大小
        """
        memory_usage = self._get_memory_usage()
        
        if memory_usage > MEMORY_THRESHOLD and self.current_batch_size > MIN_BATCH_SIZE:
            # 内存使用率高，减少批处理大小
            new_batch_size = max(MIN_BATCH_SIZE, self.current_batch_size // 2)
            if new_batch_size != self.current_batch_size:
                self.current_batch_size = new_batch_size
                safe_print(f"📉 内存使用率高 ({memory_usage:.2f}%)，减少批处理大小至: {self.current_batch_size}")
        elif memory_usage < MEMORY_THRESHOLD * 0.5 and self.current_batch_size < MAX_BATCH_SIZE:
            # 内存使用率低，增加批处理大小
            new_batch_size = min(MAX_BATCH_SIZE, self.current_batch_size * 2)
            if new_batch_size != self.current_batch_size:
                self.current_batch_size = new_batch_size
                safe_print(f"📈 内存使用率低 ({memory_usage:.2f}%)，增加批处理大小至: {self.current_batch_size}")

    def full_download_pipeline(self, frequency: str = "d") -> None:
        """
        🚀 多进程下载 + 实时进度显示
        
        Args:
            frequency: 数据频率
        """
        reset_progress()
        set_progress(is_running=True, task_name=f'全量下载{frequency}', start_time=time.time())
        
        self.start_time = pd.Timestamp.now()
        self.end_time = None
        self.error_count = 0
        self.success_count = 0
        self.total_count = 0
        self.processed_count = 0
        
        safe_print(f"🚀 启动多进程下载 (进程数: {self.current_workers}, 频率: {frequency})...")
        safe_print(f"⏰ 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        all_stocks = self._try_get_stock_list()
        self.total_stocks = len(all_stocks)
        set_progress(total=self.total_stocks)
        
        existing_codes = self.db.get_finished_stocks(frequency)
        safe_print(f"ℹ️ 数据库中已有 {len(existing_codes)} 只股票，将检查每只股票的缺失数据。")
        
        todo_stocks = all_stocks
        safe_print(f"📋 待处理股票数量: {len(todo_stocks)} 只")
        
        if todo_stocks.empty:
            safe_print("✅ 没有股票需要处理，无需操作。")
            set_progress(is_running=False, message='没有股票需要处理')
            return

        chunks: List[pd.DataFrame] = []
        chunk_size = len(todo_stocks) // self.current_workers
        for i in range(self.current_workers):
            start_idx = i * chunk_size
            if i == self.current_workers - 1:
                chunks.append(todo_stocks.iloc[start_idx:])
            else:
                chunks.append(todo_stocks.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        data_queue: Queue = Queue()
        processes: List[Process] = []
        end_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_download, args=(i+1, chunk, START_DATE_FULL, end_date, data_queue, frequency, self.stop_event, 'stock'))
            p.start()
            processes.append(p)
        
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer: List[pd.DataFrame] = []
        
        while (any(p.is_alive() for p in processes) or not data_queue.empty()) and not self.should_stop:
            try:
                item = data_queue.get(timeout=0.5)
                if item:
                    df, success, error_msg = item
                    self.total_count += 1
                    self.processed_count += 1
                    
                    if success and df is not None:
                        batch_buffer.append(df)
                        self.success_count += 1
                    else:
                        self.error_count += 1
                        if error_msg:
                            code = "未知"  
                            if df is not None and hasattr(df, 'code') and not df.empty:
                                code = df['code'].iloc[0] if 'code' in df.columns else "未知"
                            elif error_msg.startswith('sh.') or error_msg.startswith('sz.') or error_msg.startswith('bj.'):
                                code = error_msg.split(' ')[0]
                            self.log_error(code, error_msg)
                    
                    if self.total_count % 50 == 0:
                        self._adjust_concurrency()
                        self._adjust_batch_size()
                    
                    if self.error_count % MAX_ERRORS_BEFORE_WARNING == 0 and self.error_count > 0:
                        safe_print(f"⚠️ 已累计 {self.error_count} 个错误，请检查网络连接和API状态")
                    
                    elapsed = time.time() - _progress_state['start_time'] if _progress_state['start_time'] else 1
                    speed = self.processed_count / elapsed if elapsed > 0 else 0
                    remaining = (self.total_stocks - self.processed_count) / speed if speed > 0 else 0
                    eta_str = f"{int(remaining)}秒" if remaining > 0 else "完成"
                    set_progress(
                        processed=self.processed_count,
                        success=self.success_count,
                        error=self.error_count,
                        speed=round(speed, 1),
                        eta=eta_str,
                        message=f"{self.processed_count}/{self.total_stocks}"
                    )
                    
                    if self.total_count % 10 == 0 or self.total_count == self.total_stocks:
                        elapsed_time = (pd.Timestamp.now() - self.start_time).total_seconds()
                        if elapsed_time > 0:
                            speed = self.processed_count / elapsed_time
                            estimated_total = self.total_stocks / speed if speed > 0 else 0
                            remaining = estimated_total - elapsed_time
                            progress_percent = (self.processed_count / self.total_stocks) * 100
                            safe_print(f"📊 进度: {self.processed_count}/{self.total_stocks} ({progress_percent:.1f}%) | 速度: {speed:.2f} 只/秒 | 剩余: {remaining:.0f} 秒")
                
                if len(batch_buffer) >= self.current_batch_size:
                    self.db.upload_batch(batch_buffer, frequency)
                    batch_buffer = []
                    safe_print(f"💾 [主进程] 完成一批入库 (批大小: {self.current_batch_size}, 频率: {frequency})")
                    
            except Exception as e:
                import queue
                if not isinstance(e, queue.Empty):
                    logger.error(f"❌ 主进程处理队列数据时出错: {e}")
                    self.error_count += 1
                    self.log_error("未知", str(e))

        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)

        if batch_buffer:
            self.db.upload_batch(batch_buffer, frequency)

        self.end_time = pd.Timestamp.now()
        elapsed_time = (self.end_time - self.start_time).total_seconds()
        
        safe_print(f"\n📋 任务统计")
        safe_print(f"✅ 成功: {self.success_count} 只")
        safe_print(f"❌ 失败: {self.error_count} 只")
        safe_print(f"📊 总处理: {self.total_count} 只")
        safe_print(f"⏰ 耗时: {elapsed_time:.2f} 秒")
        if elapsed_time > 0:
            safe_print(f"⚡ 平均速度: {self.total_count / elapsed_time:.2f} 只/秒")
        safe_print(f"📈 成功率: {(self.success_count / self.total_count * 100):.1f}%" if self.total_count > 0 else "📈 成功率: 0%")
        safe_print(f"⏰ 结束时间: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.should_stop:
            set_progress(
                is_running=False,
                processed=self.processed_count,
                success=self.success_count,
                error=self.error_count,
                message='任务已停止'
            )
            safe_print(f"\n⏹️ 任务已停止 (频率: {frequency})")
        else:
            set_progress(
                is_running=False,
                processed=self.total_stocks,
                success=self.success_count,
                error=self.error_count,
                message='下载完成'
            )
            safe_print(f"\n✅ 全量流水线结束 (频率: {frequency})")
            self.db.vacuum()

    def daily_update_pipeline(self, frequency: str = "d") -> None:
        """
        🚀 多进程增量更新流水线
        
        Args:
            frequency: 数据频率
        """
        reset_progress()
        set_progress(is_running=True, task_name=f'增量更新{frequency}', start_time=time.time())
        
        self.start_time = pd.Timestamp.now()
        self.end_time = None
        self.error_count = 0
        self.success_count = 0
        self.total_count = 0
        self.processed_count = 0
        
        safe_print(f"🔄 启动多进程增量更新 (进程数: {self.current_workers}, 频率: {frequency})...")
        safe_print(f"⏰ 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        stocks = self._try_get_stock_list()

        safe_print("📅 计算每只股票的更新日期范围...")
        start_dates = []
        end_dates = []
        
        for _, row in stocks.iterrows():
            code = row['code']
            last_date = self.db.get_last_date(code, frequency)
            
            three_months_ago = (pd.Timestamp.now() - pd.DateOffset(months=3)).strftime("%Y-%m-%d")
            
            if last_date:
                last_date_obj = pd.to_datetime(last_date)
                start_date = (last_date_obj + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                # 确保起始日期不早于3个月前
                if start_date < three_months_ago:
                    start_date = three_months_ago
            else:
                # 没有历史数据的股票，从3个月前开始
                start_date = three_months_ago
            
            today = pd.Timestamp.now().date()
            if pd.Timestamp.now().hour < 18:
                end_date = (today - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                if start_date > end_date:
                    start_date = end_date
            else:
                end_date = today.strftime("%Y-%m-%d")
            
            start_dates.append(start_date)
            end_dates.append(end_date)
        
        stocks['start_date'] = start_dates
        stocks['end_date'] = end_dates

        self.total_stocks = len(stocks)
        set_progress(total=self.total_stocks)
        safe_print(f"📋 待处理股票数量: {self.total_stocks} 只")
        
        if self.total_stocks == 0:
            safe_print("✅ 没有股票需要处理，无需操作。")
            set_progress(is_running=False, message='没有股票需要处理')
            return

        chunks: List[pd.DataFrame] = []
        chunk_size = len(stocks) // self.current_workers
        for i in range(self.current_workers):
            start_idx = i * chunk_size
            if i == self.current_workers - 1:
                chunks.append(stocks.iloc[start_idx:])
            else:
                chunks.append(stocks.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        data_queue: Queue = Queue()
        processes: List[Process] = []

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_update, args=(i+1, chunk, data_queue, frequency, self.stop_event, 'stock'))
            p.start()
            processes.append(p)
        
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer: List[pd.DataFrame] = []
        
        while (any(p.is_alive() for p in processes) or not data_queue.empty()) and not self.should_stop:
            try:
                item = data_queue.get(timeout=0.5)
                if item:
                    df, success, error_msg = item
                    self.total_count += 1
                    self.processed_count += 1
                    
                    if success and df is not None:
                        batch_buffer.append(df)
                        self.success_count += 1
                    else:
                        self.error_count += 1
                        if error_msg:
                            code = "未知"  
                            if df is not None and hasattr(df, 'code') and not df.empty:
                                code = df['code'].iloc[0] if 'code' in df.columns else "未知"
                            elif error_msg.startswith('sh.') or error_msg.startswith('sz.') or error_msg.startswith('bj.'):
                                code = error_msg.split(' ')[0]
                            self.log_error(code, error_msg)
                    
                    if self.total_count % 50 == 0:
                        self._adjust_concurrency()
                        self._adjust_batch_size()
                    
                    if self.error_count % MAX_ERRORS_BEFORE_WARNING == 0 and self.error_count > 0:
                        safe_print(f"⚠️ 已累计 {self.error_count} 个错误，请检查网络连接和API状态")
                    
                    elapsed = time.time() - _progress_state['start_time'] if _progress_state['start_time'] else 1
                    speed = self.processed_count / elapsed if elapsed > 0 else 0
                    remaining = (self.total_stocks - self.processed_count) / speed if speed > 0 else 0
                    eta_str = f"{int(remaining)}秒" if remaining > 0 else "完成"
                    set_progress(
                        processed=self.processed_count,
                        success=self.success_count,
                        error=self.error_count,
                        speed=round(speed, 1),
                        eta=eta_str,
                        message=f"{self.processed_count}/{self.total_stocks}"
                    )
                    
                    if self.total_count % 10 == 0 or self.total_count == self.total_stocks:
                        elapsed_time = (pd.Timestamp.now() - self.start_time).total_seconds()
                        if elapsed_time > 0:
                            speed = self.processed_count / elapsed_time
                            estimated_total = self.total_stocks / speed if speed > 0 else 0
                            remaining = estimated_total - elapsed_time
                            progress_percent = (self.processed_count / self.total_stocks) * 100
                            safe_print(f"📊 进度: {self.processed_count}/{self.total_stocks} ({progress_percent:.1f}%) | 速度: {speed:.2f} 只/秒 | 剩余: {remaining:.0f} 秒")
                    
                    if len(batch_buffer) >= self.current_batch_size:
                        self.db.upload_batch(batch_buffer, frequency)
                        safe_print(f"💾 批量写入完成: {len(batch_buffer)} 条记录 (表: {self.db._get_table_name(frequency)})")
                        batch_buffer = []
            except Exception as e:
                import queue
                if not isinstance(e, queue.Empty):
                    logger.error(f"❌ 主进程处理队列数据时出错: {e}")
                    self.error_count += 1
                    self.log_error("未知", str(e))
        
        if batch_buffer:
            self.db.upload_batch(batch_buffer, frequency)
            safe_print(f"💾 最终批量写入完成: {len(batch_buffer)} 条记录 (表: {self.db._get_table_name(frequency)})")
        
        self.end_time = pd.Timestamp.now()
        elapsed_time = (self.end_time - self.start_time).total_seconds()
        
        safe_print(f"\n📋 任务统计")
        safe_print(f"✅ 成功: {self.success_count} 只")
        safe_print(f"❌ 失败: {self.error_count} 只")
        safe_print(f"📊 总处理: {self.total_count} 只")
        safe_print(f"⏰ 耗时: {elapsed_time:.2f} 秒")
        if elapsed_time > 0:
            safe_print(f"⚡ 平均速度: {self.total_count / elapsed_time:.2f} 只/秒")
        safe_print(f"📈 成功率: {(self.success_count / self.total_count * 100):.1f}%" if self.total_count > 0 else "📈 成功率: 0%")
        safe_print(f"⏰ 结束时间: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        set_progress(
            is_running=False,
            processed=self.total_stocks,
            success=self.success_count,
            error=self.error_count,
            message='更新完成'
        )
        
        safe_print(f"\n✅ 增量流水线结束 (频率: {frequency})")

    def etf_download_pipeline(self, frequency: str = "d") -> None:
        """
        🚀 多进程下载ETF数据 + 实时进度显示
        
        Args:
            frequency: 数据频率
        """
        # 初始化进度条
        reset_progress()
        set_progress(is_running=True, task_name=f"全量下载ETF({frequency})", start_time=time.time())
        
        # 记录开始时间
        self.start_time = pd.Timestamp.now()
        self.end_time = None
        self.error_count = 0
        self.success_count = 0
        self.total_count = 0
        self.processed_count = 0
        
        safe_print(f"🚀 启动多进程ETF下载 (进程数: {self.current_workers}, 频率: {frequency})...")
        safe_print(f"⏰ 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. 获取ETF列表（自动尝试所有数据源）
        all_etfs = self._try_get_etf_list()

        self.total_stocks = len(all_etfs)
        set_progress(total=self.total_stocks)  # 更新进度状态中的总数
        
        # 2. 断点续传 - 不再跳过已有的ETF，而是下载每只ETF的缺失数据
        existing_codes = self.db.get_finished_stocks(frequency, asset_type="etf")
        safe_print(f"ℹ️ 数据库中已有 {len(existing_codes)} 只ETF，将检查每只ETF的缺失数据。")
        
        todo_etfs = all_etfs
        safe_print(f"📋 待处理ETF数量: {len(todo_etfs)} 只")
        
        if todo_etfs.empty:
            safe_print("✅ 没有ETF需要处理，无需操作。")
            return

        # 3. 切分任务
        chunks: List[pd.DataFrame] = []
        chunk_size = len(todo_etfs) // self.current_workers
        for i in range(self.current_workers):
            start_idx = i * chunk_size
            if i == self.current_workers - 1:
                chunks.append(todo_etfs.iloc[start_idx:])
            else:
                chunks.append(todo_etfs.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        # 4. 启动多进程
        data_queue: Queue = Queue()
        processes: List[Process] = []
        end_date = pd.Timestamp.now().strftime("%Y-%m-%d")

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_download, args=(i+1, chunk, START_DATE_FULL_ETF, end_date, data_queue, frequency, self.stop_event, 'etf'))
            p.start()
            processes.append(p)
        
        # 5. 主进程：监听队列并入库
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer: List[pd.DataFrame] = []
        
        while (any(p.is_alive() for p in processes) or not data_queue.empty()) and not self.should_stop:
            try:
                item = data_queue.get(timeout=0.5)
                if item:
                    df, success, error_msg = item
                    self.total_count += 1
                    self.processed_count += 1
                    
                    if success and df is not None:
                        batch_buffer.append(df)
                        self.success_count += 1
                        # 添加调试信息
                        if self.total_count <= 5:  # 只显示前5次
                            safe_print(f"📥 [主进程] 接收到ETF数据，批次大小: {len(df)} 条，缓冲区: {len(batch_buffer)}/{self.current_batch_size}")
                    else:
                        self.error_count += 1
                        # 记录错误信息
                        if error_msg:
                            # 从df中提取ETF代码，如果df为None则从错误信息中提取
                            code = "未知"  
                            if df is not None and hasattr(df, 'code') and not df.empty:
                                code = df['code'].iloc[0] if 'code' in df.columns else "未知"
                            elif error_msg.startswith('sh.') or error_msg.startswith('sz.') or error_msg.startswith('bj.'):
                                code = error_msg.split(' ')[0]
                            self.log_error(code, error_msg)
                    
                    # 动态调整并发数和批处理大小
                    if self.total_count % 50 == 0:  # 每处理50只ETF调整一次
                        self._adjust_concurrency()
                        self._adjust_batch_size()
                    
                    # 错误数达到阈值时发出警告
                    if self.error_count % MAX_ERRORS_BEFORE_WARNING == 0 and self.error_count > 0:
                        safe_print(f"⚠️ 已累计 {self.error_count} 个错误，请检查网络连接和API状态")
                    
                    # 计算进度并更新状态
                    elapsed = time.time() - _progress_state['start_time'] if _progress_state['start_time'] else 1
                    speed = self.processed_count / elapsed if elapsed > 0 else 0
                    remaining = (self.total_stocks - self.processed_count) / speed if speed > 0 else 0
                    eta_str = f"{int(remaining)}秒" if remaining > 0 else "完成"
                    
                    # 更新进度状态（供前端显示）
                    set_progress(
                        processed=self.processed_count,
                        success=self.success_count,
                        error=self.error_count,
                        speed=round(speed, 1),
                        eta=eta_str,
                        message=f"{self.processed_count}/{self.total_stocks}"
                    )
                    
                    # 显示详细进度
                    if self.total_count % 10 == 0 or self.total_count == self.total_stocks:
                        progress_percent = (self.processed_count / self.total_stocks) * 100
                        safe_print(f"📊 进度: {self.processed_count}/{self.total_stocks} ({progress_percent:.1f}%) | 速度: {speed:.2f} 只/秒 | 剩余: {remaining:.0f} 秒")
                    
                    # 批量入库
                    if len(batch_buffer) >= self.current_batch_size:
                        self.db.upload_batch(batch_buffer, frequency, asset_type="etf")
                        safe_print(f"💾 批量写入完成: {len(batch_buffer)} 条记录 (表: {self.db._get_table_name(frequency, 'etf')})")
                        batch_buffer = []
            except Exception as e:
                import queue
                if not isinstance(e, queue.Empty):
                    logger.error(f"❌ 主进程处理队列数据时出错: {e}")
                    self.error_count += 1
                    self.log_error("未知", str(e))
        
        # 6. 等待所有进程结束
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)

        if batch_buffer:
            self.db.upload_batch(batch_buffer, frequency, asset_type="etf")

        self.end_time = pd.Timestamp.now()
        elapsed_time = (self.end_time - self.start_time).total_seconds()
        
        safe_print(f"\n 任务统计")
        safe_print(f"✅ 成功: {self.success_count} 只")
        safe_print(f"❌ 失败: {self.error_count} 只")
        safe_print(f"📊 总处理: {self.total_count} 只")
        safe_print(f"⏰ 耗时: {elapsed_time:.2f} 秒")
        if elapsed_time > 0:
            safe_print(f"⚡ 平均速度: {self.total_count / elapsed_time:.2f} 只/秒")
        safe_print(f"📈 成功率: {(self.success_count / self.total_count * 100):.1f}%" if self.total_count > 0 else "📈 成功率: 0%")
        safe_print(f" 结束时间: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.should_stop:
            set_progress(
                is_running=False,
                processed=self.processed_count,
                success=self.success_count,
                error=self.error_count,
                message='任务已停止'
            )
            safe_print(f"\n⏹️ ETF任务已停止 (频率: {frequency})")
        else:
            set_progress(
                is_running=False,
                processed=self.total_stocks,
                success=self.success_count,
                error=self.error_count,
                message='下载完成'
            )
            safe_print(f"\n✅ ETF全量流水线结束 (频率: {frequency})")
            self.db.vacuum()

    def etf_update_pipeline(self, frequency: str = "d") -> None:
        """
        🚀 多进程ETF增量更新流水线
        
        Args:
            frequency: 数据频率
        """
        # 初始化进度条
        reset_progress()
        set_progress(is_running=True, task_name=f"增量更新ETF({frequency})", start_time=time.time())
        
        # 记录开始时间
        self.start_time = pd.Timestamp.now()
        self.end_time = None
        self.error_count = 0
        self.success_count = 0
        self.total_count = 0
        self.processed_count = 0
        
        safe_print(f"🔄 启动多进程ETF增量更新 (进程数: {self.current_workers}, 频率: {frequency})...")
        safe_print(f"⏰ 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. 获取ETF列表（自动尝试所有数据源）
        etfs = self._try_get_etf_list()

        # 2. 计算每只ETF的开始日期和结束日期
        safe_print("📅 计算每只ETF的更新日期范围...")
        start_dates = []
        end_dates = []
        
        for _, row in etfs.iterrows():
            code = row['code']
            last_date = self.db.get_last_date(code, frequency, asset_type="etf")
            
            three_months_ago = (pd.Timestamp.now() - pd.DateOffset(months=3)).strftime("%Y-%m-%d")
            
            if last_date:
                last_date_obj = pd.to_datetime(last_date)
                # 计算开始日期（最后日期的下一天）
                start_date = (last_date_obj + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                if start_date < three_months_ago:
                    start_date = three_months_ago
            else:
                start_date = three_months_ago
            
            # 计算结束日期，考虑到数据可能还未更新
            today = pd.Timestamp.now().date()
            # 如果当前时间早于 18:00，使用昨天作为结束日期
            if pd.Timestamp.now().hour < 18:
                end_date = (today - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                # 如果开始日期大于结束日期，使用开始日期作为结束日期
                if start_date > end_date:
                    start_date = end_date
            else:
                end_date = today.strftime("%Y-%m-%d")
            
            start_dates.append(start_date)
            end_dates.append(end_date)
        
        # 将开始日期和结束日期添加到ETF列表中
        etfs['start_date'] = start_dates
        etfs['end_date'] = end_dates

        self.total_stocks = len(etfs)
        set_progress(total=self.total_stocks)  # 更新进度状态中的总数
        safe_print(f"📋 待处理ETF数量: {self.total_stocks} 只")
        
        if self.total_stocks == 0:
            safe_print("✅ 没有ETF需要处理，无需操作。")
            return

        # 3. 切分任务
        chunks: List[pd.DataFrame] = []
        chunk_size = len(etfs) // self.current_workers
        for i in range(self.current_workers):
            start_idx = i * chunk_size
            if i == self.current_workers - 1:
                chunks.append(etfs.iloc[start_idx:])
            else:
                chunks.append(etfs.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        # 4. 启动多进程
        data_queue: Queue = Queue()
        processes: List[Process] = []

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_update, args=(i+1, chunk, data_queue, frequency, self.stop_event, 'etf'))
            p.start()
            processes.append(p)
        
        # 5. 主进程：监听队列并入库
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer: List[pd.DataFrame] = []
        
        while any(p.is_alive() for p in processes) or not data_queue.empty():
            try:
                item = data_queue.get(timeout=0.5)
                if item:
                    df, success, error_msg = item
                    self.total_count += 1
                    self.processed_count += 1
                    
                    if success and df is not None:
                        batch_buffer.append(df)
                        self.success_count += 1
                        # 添加调试信息
                        if self.total_count <= 5:  # 只显示前5次
                            safe_print(f"📥 [主进程] 接收到ETF数据，批次大小: {len(df)} 条，缓冲区: {len(batch_buffer)}/{self.current_batch_size}")
                    else:
                        self.error_count += 1
                        # 记录错误信息
                        if error_msg:
                            # 从df中提取ETF代码，如果df为None则从错误信息中提取
                            code = "未知"  
                            if df is not None and hasattr(df, 'code') and not df.empty:
                                code = df['code'].iloc[0] if 'code' in df.columns else "未知"
                            elif error_msg.startswith('sh.') or error_msg.startswith('sz.') or error_msg.startswith('bj.'):
                                code = error_msg.split(' ')[0]
                            self.log_error(code, error_msg)
                    
                    # 动态调整并发数和批处理大小
                    if self.total_count % 50 == 0:  # 每处理50只ETF调整一次
                        self._adjust_concurrency()
                        self._adjust_batch_size()
                    
                    # 错误数达到阈值时发出警告
                    if self.error_count % MAX_ERRORS_BEFORE_WARNING == 0 and self.error_count > 0:
                        safe_print(f"⚠️ 已累计 {self.error_count} 个错误，请检查网络连接和API状态")
                    
                    # 计算进度并更新状态
                    elapsed = time.time() - _progress_state['start_time'] if _progress_state['start_time'] else 1
                    speed = self.processed_count / elapsed if elapsed > 0 else 0
                    remaining = (self.total_stocks - self.processed_count) / speed if speed > 0 else 0
                    eta_str = f"{int(remaining)}秒" if remaining > 0 else "完成"
                    
                    # 更新进度状态（供前端显示）
                    set_progress(
                        processed=self.processed_count,
                        success=self.success_count,
                        error=self.error_count,
                        speed=round(speed, 1),
                        eta=eta_str,
                        message=f"{self.processed_count}/{self.total_stocks}"
                    )
                    
                    # 显示详细进度
                    if self.total_count % 10 == 0 or self.total_count == self.total_stocks:
                        progress_percent = (self.processed_count / self.total_stocks) * 100
                        safe_print(f"📊 进度: {self.processed_count}/{self.total_stocks} ({progress_percent:.1f}%) | 速度: {speed:.2f} 只/秒 | 剩余: {remaining:.0f} 秒")
                    
                    # 批量入库
                    if len(batch_buffer) >= self.current_batch_size:
                        self.db.upload_batch(batch_buffer, frequency, asset_type="etf")
                        safe_print(f"💾 批量写入完成: {len(batch_buffer)} 条记录 (表: {self.db._get_table_name(frequency, 'etf')})")
                        batch_buffer = []
            except Exception as e:
                import queue
                if not isinstance(e, queue.Empty):
                    logger.error(f"❌ 主进程处理队列数据时出错: {e}")
                    self.error_count += 1
                    self.log_error("未知", str(e))
        
        # 处理剩余数据
        if batch_buffer:
            self.db.upload_batch(batch_buffer, frequency, asset_type="etf")
            safe_print(f"💾 最终批量写入完成: {len(batch_buffer)} 条记录 (表: {self.db._get_table_name(frequency, 'etf')})")
        
        # 记录结束时间
        self.end_time = pd.Timestamp.now()
        elapsed_time = (self.end_time - self.start_time).total_seconds()
        
        # 显示统计信息
        safe_print(f"\n📋 任务统计")
        safe_print(f"✅ 成功: {self.success_count} 只")
        safe_print(f"❌ 失败: {self.error_count} 只")
        safe_print(f"📊 总处理: {self.total_count} 只")
        safe_print(f"⏰ 耗时: {elapsed_time:.2f} 秒")
        if elapsed_time > 0:
            safe_print(f"⚡ 平均速度: {self.total_count / elapsed_time:.2f} 只/秒")
        safe_print(f"📈 成功率: {(self.success_count / self.total_count * 100):.1f}%" if self.total_count > 0 else "📈 成功率: 0%")
        safe_print(f"⏰ 结束时间: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新进度状态为完成
        set_progress(
            is_running=False,
            processed=self.total_stocks,
            success=self.success_count,
            error=self.error_count,
            message='更新完成'
        )
        
        safe_print(f"\n✅ ETF增量流水线结束 (频率: {frequency})")