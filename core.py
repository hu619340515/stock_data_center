import pandas as pd
from multiprocessing import Process, Queue, current_process, Event
import logging # ✅ 单独导入标准 logging 模块
from typing import List, Tuple, Optional, Set, Any
from queue import Empty
from database import DuckDBManager
from data_source_factory import DataSourceFactory
from config import MAX_WORKERS, START_DATE_FULL, DYNAMIC_CONCURRENCY, MIN_WORKERS, MAX_WORKERS_LIMIT, ERROR_THRESHOLD, SUCCESS_THRESHOLD, BATCH_SIZE, MAX_BATCH_SIZE, MIN_BATCH_SIZE, MEMORY_THRESHOLD, USE_ARROW, COMPRESS_DATA, ERROR_LOG_FILE, MAX_ERRORS_BEFORE_WARNING, DEFAULT_DATA_SOURCE, ENABLE_DATA_SOURCE_FALLBACK, DATA_SOURCE_PRIORITY

# 配置日志
logger = logging.getLogger("Core")

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
    # 使用logging模块来避免Unicode编码问题
    logger.info(msg)

def worker_update(process_id: int, stock_list: pd.DataFrame, result_queue: Queue, frequency: str = "d", data_type: str = "stock", pause_event: Event = None, cancel_event: Event = None):
    """
    🚀 增量更新的工作进程
    
    Args:
        process_id: 进程ID
        stock_list: 股票列表
        result_queue: 结果队列
        frequency: 数据频率
    """
    from data_source_factory import DataSourceFactory
    from config import DEFAULT_DATA_SOURCE
    
    # 使用工厂创建数据源
    client = DataSourceFactory.create_data_source(DEFAULT_DATA_SOURCE)
    client.login()
    
    # 实时显示进程启动
    safe_print(f"🚀 [进程 {process_id}] 启动，任务量: {len(stock_list)} 只，数据源: {client.get_data_source_name()}，频率: {frequency}")
    
    try:
        total = len(stock_list)
        for i, (_, row) in enumerate(stock_list.iterrows()):
            if cancel_event and cancel_event.is_set():
                safe_print(f"❌ [进程 {process_id}] 检测到取消信号，终止。")
                break

            if pause_event and pause_event.is_set():
                safe_print(f"⏸️ [进程 {process_id}] 检测到暂停信号，等待中...")
                while pause_event.is_set():
                    time.sleep(0.1) # 短暂休眠，避免忙等
                safe_print(f"▶️ [进程 {process_id}] 暂停解除，继续。")

            if cancel_event and cancel_event.is_set():
                safe_print(f"❌ [进程 {process_id}] 检测到取消信号，终止。")
                break

            if pause_event and pause_event.is_set():
                safe_print(f"⏸️ [进程 {process_id}] 检测到暂停信号，等待中...")
                while pause_event.is_set():
                    time.sleep(0.1) # 短暂休眠，避免忙等
                safe_print(f"▶️ [进程 {process_id}] 暂停解除，继续。")

            current_stock_count = i + 1
            total_stocks = len(stock_list)
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
                        result_queue.put(("data", df, True, None))  # (类型, 数据, 成功标志, 错误信息)
                    else:
                        safe_print(f"ℹ️ [进程 {process_id}] {code} 无新数据")
                        result_queue.put(("data", None, True, f"{code} 无新数据"))  # (类型, 无数据, 成功标志, 信息)
                else:
                    safe_print(f"ℹ️ [进程 {process_id}] {code} 数据已最新")
                    result_queue.put((None, True, f"{code} 数据已最新"))  # (无数据, 成功标志, 信息)
                
                # 实时发送进度更新到主进程
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    result_queue.put(("progress", process_id, i + 1, total, code))
                    
            except Exception as e:
                error_msg = str(e)
                safe_print(f"❌ [进程 {process_id}] {code} 更新异常: {error_msg}")
                result_queue.put(("data", None, False, error_msg))  # (类型, 异常, 失败标志, 错误信息)
    finally:
        client.logout()
        safe_print(f"🏁 [进程 {process_id}] 任务结束")

def worker_download(process_id: int, stock_list: pd.DataFrame, start_date: str, end_date: str, result_queue: Queue, frequency: str = "d", data_type: str = "stock", pause_event: Event = None, cancel_event: Event = None):
    """
    🚀 全量下载的工作进程
    
    Args:
        process_id: 进程ID
        stock_list: 股票列表
        start_date: 开始日期
        end_date: 结束日期
        result_queue: 结果队列
        frequency: 数据频率
    """
    from data_source_factory import DataSourceFactory
    from config import DEFAULT_DATA_SOURCE
    
    # 使用工厂创建数据源
    client = DataSourceFactory.create_data_source(DEFAULT_DATA_SOURCE)
    client.login()
    
    # 实时显示进程启动
    safe_print(f"🚀 [进程 {process_id}] 启动，任务量: {len(stock_list)} 只，数据源: {client.get_data_source_name()}，频率: {frequency}")
    
    try:
        total = len(stock_list)
        for i, (_, row) in enumerate(stock_list.iterrows()):
            code = row['code']
            name = row['code_name']
            
            try:
                # 1. 直接下载完整日期范围的数据（主进程负责过滤已存在的数据）
                safe_print(f"📥 [进程 {process_id}] 下载 {code} {start_date} 至 {end_date} 的数据 (频率: {frequency})")
                df = client.get_stock_history(code, start_date, end_date, frequency)
                
                # 2. 将数据放入队列（供主进程入库和去重）
                if not df.empty:
                    safe_print(f"✅ [进程 {process_id}] {code} 下载成功，数据量: {len(df)} 条")
                    result_queue.put(("data", df, True, None))  # (类型, 数据, 成功标志, 错误信息)
                    
                    # 实时发送进度更新到主进程
                    if (i + 1) % 10 == 0 or (i + 1) == total:
                        result_queue.put(("progress", process_id, i + 1, total, code))
                else:
                    safe_print(f"ℹ️ [进程 {process_id}] {code} 无历史数据")
                    result_queue.put((None, True, f"{code} 无历史数据"))  # (无数据, 成功标志, 信息)
                    
            except Exception as e:
                error_msg = str(e)
                safe_print(f"❌ [进程 {process_id}] {code} 下载异常: {error_msg}")
                result_queue.put(("data", None, False, error_msg))  # (类型, 异常, 失败标志, 错误信息)
    finally:
        client.logout()
        safe_print(f"🏁 [进程 {process_id}] 任务结束")

class StockDataPipeline:
    def __init__(self):
        # 注意：数据库连接应该在需要时创建，避免多进程共享同一连接
        self.db: Optional[DuckDBManager] = None
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
        self.current_data_source: Optional[Any] = None
        self._init_data_source()

        self._pause_event = Event()
        self._cancel_event = Event()
        
    def _ensure_db_connected(self):
        """确保数据库连接已建立"""
        if self.db is None:
            self.db = DuckDBManager()

    def set_pause(self):
        self._pause_event.set()

    def clear_pause(self):
        self._pause_event.clear()

    def set_cancel(self):
        self._cancel_event.set()

    def _init_data_source(self) -> None:
        """
        📱 初始化数据源
        """
        self.current_data_source = self._create_data_source(DEFAULT_DATA_SOURCE)
        if self.current_data_source:
            safe_print(f"✅ 初始化数据源: {self.current_data_source.get_data_source_name()}")
        else:
            safe_print("❌ 初始化数据源失败")

    def _create_data_source(self, source_type: str) -> Optional[Any]:
        """
        📱 创建数据源实例
        
        Args:
            source_type: 数据源类型
            
        Returns:
            数据源实例
        """
        try:
            return DataSourceFactory.create_data_source(source_type)
        except Exception as e:
            logger.error(f"❌ 创建数据源 {source_type} 失败: {e}")
            return None

    def _switch_data_source(self) -> bool:
        """
        🔄 切换数据源
        
        Returns:
            是否切换成功
        """
        if not ENABLE_DATA_SOURCE_FALLBACK:
            return False
        
        current_source_name = self.current_data_source.get_data_source_name() if self.current_data_source else "None"
        safe_print(f"🔄 尝试切换数据源，当前数据源: {current_source_name}")
        
        for source_type in DATA_SOURCE_PRIORITY:
            if source_type.lower() != current_source_name.lower():
                new_source = self._create_data_source(source_type)
                if new_source:
                    # 登出当前数据源
                    if self.current_data_source:
                        try:
                            self.current_data_source.logout()
                        except:
                            pass
                    
                    self.current_data_source = new_source
                    safe_print(f"✅ 成功切换到数据源: {self.current_data_source.get_data_source_name()}")
                    return True
        
        safe_print("❌ 没有可用的备用数据源")
        return False

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

    def full_download_pipeline(self, frequency: str = "d", progress_callback: Optional[callable] = None, data_type: str = "stock", start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        """
        🚀 多进程下载 + 实时进度显示
        
        Args:
            frequency: 数据频率
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        """
        # 确保数据库连接已建立
        self._ensure_db_connected()
        
        # 记录开始时间
        self.start_time = pd.Timestamp.now()
        self.end_time = None
        self.error_count = 0
        self.success_count = 0
        self.total_count = 0
        self.processed_count = 0
        
        # 使用用户指定的时间范围或默认值
        if start_date is None:
            start_date = START_DATE_FULL
        if end_date is None:
            end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        safe_print(f"🚀 启动多进程下载 (进程数: {self.current_workers}, 频率: {frequency})...")
        safe_print(f"⏰ 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        safe_print(f"📅 下载时间范围: {start_date} 至 {end_date}")
        
        # 1. 获取股票列表
        all_stocks = self.current_data_source.get_stock_list(data_type=data_type)
        if all_stocks.empty:
            safe_print("❌ 未获取到股票列表")
            return

        self.total_stocks = len(all_stocks)
        
        # 更新股票基本信息
        safe_print("📝 更新股票基本信息...")
        for _, row in all_stocks.iterrows():
            code = row['code']
            code_name = row.get('code_name', '')
            industry = row.get('industry', None)
            market = row.get('market', None)
            list_date = row.get('list_date', None)
            self.db.upsert_stock_info(code, code_name, industry, market, list_date)
        
        # 2. 断点续传 - 不再跳过已有的股票，而是下载每只股票的缺失数据
        safe_print("📅 计算每只股票的下载日期范围...")
        
        # 为每只股票确定下载的开始和结束日期
        stocks_with_specific_dates = []

        for _, row in all_stocks.iterrows():
            code = row['code']
            last_date_in_db = self.db.get_last_date(code, frequency)
            
            if last_date_in_db:
                last_date_obj = pd.to_datetime(last_date_in_db)
                start_date_for_stock = (last_date_obj + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date_for_stock = start_date
            
            # 只有当计算出的开始日期不晚于全局结束日期时，才将股票加入待处理列表
            if start_date_for_stock <= end_date:
                row_dict = row.to_dict()
                row_dict['start_date'] = start_date_for_stock
                row_dict['end_date'] = end_date
                stocks_with_specific_dates.append(row_dict)
        
        todo_stocks = pd.DataFrame(stocks_with_specific_dates)
        
        safe_print(f"ℹ️ 初始股票总数: {len(all_stocks)} 只，实际待下载股票: {len(todo_stocks)} 只 (已过滤掉已是最新数据或无下载范围的股票)。")
        safe_print(f"📋 待处理股票数量: {len(todo_stocks)} 只")
        
        if todo_stocks.empty:
            safe_print("✅ 没有股票需要处理，无需操作。")
            return

        # 3. 切分任务
        chunks: List[pd.DataFrame] = []
        chunk_size = len(todo_stocks) // self.current_workers
        for i in range(self.current_workers):
            start_idx = i * chunk_size
            if i == self.current_workers - 1:
                chunks.append(todo_stocks.iloc[start_idx:])
            else:
                chunks.append(todo_stocks.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        # 3. 启动多进程
        data_queue: Queue = Queue()
        processes: List[Process] = []

        for i, chunk in enumerate(chunks):
            # 注意：worker_download函数内部不应该直接访问主进程的数据库连接
            # 所有数据库操作应该由主进程统一处理
            p = Process(target=worker_download, args=(i+1, chunk, start_date, end_date, data_queue, frequency, data_type, self._pause_event, self._cancel_event))
            p.start()
            processes.append(p)
        
        # 5. 主进程：监听队列并入库
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer: List[pd.DataFrame] = []
        processed_count = 0
        total_stocks = len(todo_stocks)

        while True:
                # 检查取消信号
                if self._cancel_event.is_set():
                    logger.warning("检测到取消信号，停止接收队列数据并终止子进程。")
                    for p in processes:
                        if p.is_alive():
                            p.terminate() # 强制终止子进程
                    break # 退出主循环

                # 检查所有进程是否都已结束且队列已空，如果是则正常退出
                if not any(p.is_alive() for p in processes) and data_queue.empty():
                    break # 正常退出主循环

                try:
                    message = data_queue.get(timeout=0.1) # 短暂等待以避免忙等和频繁检查
                    if message[0] == "progress":
                        # 处理进度消息
                        _type, process_id, current, total_in_process, code = message
                        # 直接使用current作为当前进程的进度，然后传递给回调
                        if progress_callback:
                            # 计算当前进程的完成比例
                            process_progress = current / total_in_process if total_in_process > 0 else 0
                            # 估算整个任务的进度
                            estimated_total_progress = (processed_count + process_progress) / total_stocks * 100
                            progress_callback(int(estimated_total_progress), 100, f"进程 {process_id} 正在下载 {code}")
                    elif message[0] == "data":
                        # 处理数据消息
                        _type, df, success, error_msg = message
                        if success and df is not None:
                            batch_buffer.append(df)
                            processed_count += 1
                            if len(batch_buffer) >= BATCH_SIZE:
                                self._insert_batch(batch_buffer, frequency)
                                batch_buffer.clear()
                            # 每次处理完一个股票后更新进度
                            if progress_callback:
                                current_progress = processed_count / total_stocks * 100
                                progress_callback(int(current_progress), 100, f"已处理 {processed_count}/{total_stocks} 只股票")
                        else:
                            # 处理失败情况
                            self.error_count += 1
                            if error_msg:
                                self.log_error("未知", error_msg)
                    elif message[0] == "log":
                        # 处理日志消息
                        _type, level, msg = message
                        self.pipeline_logger.log(level, msg)
                except Empty:
                    # 队列为空时继续循环，检查进程状态和取消信号
                    pass
                except Exception as e:
                    logger.error(f"处理队列消息时发生错误: {e}")

            # 插入剩余的批量数据
        if batch_buffer:
            self._insert_batch(batch_buffer, frequency)

        # 等待所有子进程结束 (可选，因为上面已经terminate了，但保险起见)
        for p in processes:
            p.join()

        # 6. 等待所有进程结束
        for p in processes:
            p.join()

        # 7. 写入最后剩余的数据
        if batch_buffer:
            self.db.upload_batch(batch_buffer, frequency)

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
        
        safe_print(f"\n✅ 全量流水线结束 (频率: {frequency})")
        self.db.vacuum()

    def daily_update_pipeline(self, frequency: str = "d", progress_callback: Optional[callable] = None, data_type: str = "stock", start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        """
        🚀 多进程增量更新流水线
        
        Args:
            frequency: 数据频率
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        """
        # 确保数据库连接已建立
        self._ensure_db_connected()
        
        # 记录开始时间
        self.start_time = pd.Timestamp.now()
        self.end_time = None
        self.error_count = 0
        self.success_count = 0
        self.total_count = 0
        self.processed_count = 0
        
        # 使用用户指定的时间范围或默认值
        if end_date is None:
            # 计算结束日期，考虑到数据可能还未更新
            today = pd.Timestamp.now().date()
            # 如果当前时间早于 18:00，使用昨天作为结束日期
            if pd.Timestamp.now().hour < 18:
                end_date = (today - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                end_date = today.strftime("%Y-%m-%d")
        
        safe_print(f"🔄 启动多进程增量更新 (进程数: {self.current_workers}, 频率: {frequency})...")
        safe_print(f"⏰ 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        safe_print(f"📅 更新时间范围: {start_date if start_date else '自动计算'} 至 {end_date}")
        
        # 1. 获取股票列表
        stocks = self.current_data_source.get_stock_list(data_type=data_type)
        if stocks.empty:
            safe_print("❌ 未获取到股票列表")
            return
        
        # 更新股票基本信息
        safe_print("📝 更新股票基本信息...")
        for _, row in stocks.iterrows():
            code = row['code']
            code_name = row.get('code_name', '')
            industry = row.get('industry', None)
            market = row.get('market', None)
            list_date = row.get('list_date', None)
            self.db.upsert_stock_info(code, code_name, industry, market, list_date)

        # 2. 计算每只股票的开始日期和结束日期
        safe_print("📅 计算每只股票的更新日期范围...")
        start_dates = []
        end_dates = []
        
        for _, row in stocks.iterrows():
            code = row['code']
            
            if start_date:
                # 使用用户指定的开始日期
                start_date_stock = start_date
            else:
                # 自动计算开始日期
                last_date = self.db.get_last_date(code, frequency)
                if last_date:
                    last_date_obj = pd.to_datetime(last_date)
                    # 计算开始日期（最后日期的下一天）
                    start_date_stock = (last_date_obj + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_date_stock = "1999-01-01"
            
            # 如果开始日期大于结束日期，使用开始日期作为结束日期
            if start_date_stock > end_date:
                start_date_stock = end_date
            
            start_dates.append(start_date_stock)
            end_dates.append(end_date)
        
        # 将开始日期和结束日期添加到股票列表中
        stocks['start_date'] = start_dates
        stocks['end_date'] = end_dates

        self.total_stocks = len(stocks)
        safe_print(f"📋 待处理股票数量: {self.total_stocks} 只")
        
        if self.total_stocks == 0:
            safe_print("✅ 没有股票需要处理，无需操作。")
            return

        # 2. 切分任务
        chunks: List[pd.DataFrame] = []
        chunk_size = len(stocks) // self.current_workers
        for i in range(self.current_workers):
            start_idx = i * chunk_size
            if i == self.current_workers - 1:
                chunks.append(stocks.iloc[start_idx:])
            else:
                chunks.append(stocks.iloc[start_idx : start_idx + chunk_size])
        
        chunks = [c for c in chunks if not c.empty]
        
        # 3. 启动多进程
        data_queue: Queue = Queue()
        processes: List[Process] = []

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_update, args=(i+1, chunk, data_queue, frequency, data_type, self._pause_event, self._cancel_event))
            p.start()
            processes.append(p)
        
        # 4. 主进程：监听队列并入库
        safe_print("👂 主进程正在后台接收数据并入库...")
        batch_buffer: List[pd.DataFrame] = []

        while True:
            # 检查取消信号
            if self._cancel_event.is_set():
                logger.warning("检测到取消信号，停止接收队列数据并终止子进程。")
                for p in processes:
                    if p.is_alive():
                        p.terminate() # 强制终止子进程
                break # 退出主循环

            # 检查所有进程是否都已结束且队列已空，如果是则正常退出
            if not any(p.is_alive() for p in processes) and data_queue.empty():
                break # 正常退出主循环

            try:
                    item = data_queue.get(timeout=0.1) # 短暂等待以避免忙等和频繁检查
                    if item:
                        msg_type = item[0]
                        if msg_type == "progress":
                            _type, process_id, current, total_in_process, code = item
                            if progress_callback:
                                # 计算当前进程的完成比例
                                process_progress = current / total_in_process if total_in_process > 0 else 0
                                # 估算整个任务的进度
                                estimated_total_progress = (self.processed_count + process_progress) / self.total_stocks * 100
                                progress_callback(int(estimated_total_progress), 100, f"正在处理: {code} ({current}/{total_in_process})")
                        elif msg_type == "data":
                            df, success, error_msg = item[1:]
                            self.total_count += 1
                            self.processed_count += 1

                            if success and df is not None:
                                batch_buffer.append(df)
                                self.success_count += 1
                                if self.total_count <= 5:
                                    safe_print(f"📥 [主进程] 接收到数据，批次大小: {len(df)} 条，缓冲区: {len(batch_buffer)}/{self.current_batch_size}")
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

                        if progress_callback:
                            # 计算当前进度百分比
                            current_progress = self.processed_count / self.total_stocks * 100
                            progress_callback(int(current_progress), 100, f"已处理: {self.processed_count}/{self.total_stocks}")

                        if len(batch_buffer) >= self.current_batch_size:
                            self.db.upload_batch(batch_buffer, frequency)
                            batch_buffer = []
                            safe_print(f"💾 [主进程] 完成一批入库 (批大小: {self.current_batch_size}, 频率: {frequency})")
            except queue.Empty:
                # 队列为空时继续循环，检查进程状态和取消信号
                pass
            except Exception as e:
                logger.error(f"❌ 主进程处理队列消息时出错: {e}")
                self.error_count += 1
                self.log_error("未知", str(e))
        
        # 处理剩余数据
        if batch_buffer:
            self.db.upload_batch(batch_buffer, frequency)
            safe_print(f"💾 最终批量写入完成: {len(batch_buffer)} 条记录 (表: {self.db._get_table_name(frequency)})")

        # 等待所有子进程结束
        for p in processes:
            p.join()
        
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
        
        safe_print(f"\n✅ 增量流水线结束 (频率: {frequency})")