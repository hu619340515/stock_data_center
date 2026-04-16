import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog
import threading
import queue
import logging
from datetime import datetime
import os
import webbrowser

# Assuming the stock_data_center modules are directly in the project root
from core import StockDataPipeline
from database import DuckDBManager
from config import config_loader, DATABASE_PATH, BAOSTOCK_ADJUST_FLAG, START_DATE_FULL # Import config_loader


class QueueHandler(logging.Handler):
    """Class to send logging records to a queue."""
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)

class StockDataGUI:
    def __init__(self, master):
        self.master = master
        master.title("股票数据中枢")
        master.geometry("800x600")
        master.resizable(True, True)
        master.iconbitmap(default=None)

        # 设置主题
        self.style = ttk.Style()
        try:
            # 尝试使用现代主题
            self.style.theme_use('clam')
            # 自定义样式
            self.style.configure('TButton', font=('微软雅黑', 10), padding=5)
            self.style.configure('TLabel', font=('微软雅黑', 10))
            self.style.configure('TEntry', font=('微软雅黑', 10))
            self.style.configure('TLabelframe', font=('微软雅黑', 11, 'bold'))
            self.style.configure('TLabelframe.Label', font=('微软雅黑', 11, 'bold'))
            self.style.configure('TNotebook.Tab', font=('微软雅黑', 10))
            self.style.configure('TProgressbar', background='#4CAF50')
        except:
            pass

        self.log_queue = queue.Queue()
        
        # Configure root logger to send messages to the queue
        self.queue_handler = QueueHandler(self.log_queue)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.queue_handler.setFormatter(formatter)
        
        # Get the root logger and add the queue handler
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO) # Ensure root logger captures INFO level messages
        root_logger.addHandler(self.queue_handler)

        self.gui_logger = logging.getLogger(__name__) # Logger for GUI specific messages

        self.create_widgets()
        self.master.after(100, self.process_log_queue)

        self.pipeline = StockDataPipeline() # Initialize pipeline
        self.db_manager = DuckDBManager() # Initialize DB manager

        self.is_paused = tk.BooleanVar(value=False)
        self.is_cancelled = tk.BooleanVar(value=False)

    def create_widgets(self):
        # Notebook for different tabs (Download, Export, Config, etc.)
        self.notebook = ttk.Notebook(self.master)
        self.notebook.pack(pady=10, padx=10, expand=True, fill="both")

        # --- Download Tab ---
        self.download_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.download_frame, text="数据下载")
        self._create_download_tab(self.download_frame)

        # --- Export Tab ---
        self.export_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.export_frame, text="数据导出")
        self._create_export_tab(self.export_frame)

        # --- Database Management Tab ---
        self.db_manage_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.db_manage_frame, text="数据库管理")
        self._create_db_manage_tab(self.db_manage_frame)

        # --- Configuration Tab ---
        self.config_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.config_frame, text="配置")
        self._create_config_tab(self.config_frame)

    def _create_download_tab(self, parent_frame):
        # 主框架
        main_frame = ttk.Frame(parent_frame)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # 顶部控制区域
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill="x", pady=5)

        # 数据频率选择 (多选)
        freq_frame = ttk.LabelFrame(top_frame, text="数据频率")
        freq_frame.pack(side="left", fill="x", expand=True, padx=5)
        
        freq_inner_frame = ttk.Frame(freq_frame)
        freq_inner_frame.pack(padx=10, pady=10)
        
        self.freq_daily_var = tk.BooleanVar(value=True) # Default to daily
        ttk.Checkbutton(freq_inner_frame, text="日线", variable=self.freq_daily_var).pack(side="left", padx=10)
        self.freq_weekly_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(freq_inner_frame, text="周线", variable=self.freq_weekly_var).pack(side="left", padx=10)
        self.freq_monthly_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(freq_inner_frame, text="月线", variable=self.freq_monthly_var).pack(side="left", padx=10)

        # 数据类型选择 (多选)
        type_frame = ttk.LabelFrame(top_frame, text="数据类型")
        type_frame.pack(side="left", fill="x", expand=True, padx=5)
        
        type_inner_frame = ttk.Frame(type_frame)
        type_inner_frame.pack(padx=10, pady=10)
        
        self.data_type_stock_var = tk.BooleanVar(value=True) # Default to include stocks
        ttk.Checkbutton(type_inner_frame, text="股票", variable=self.data_type_stock_var).pack(side="left", padx=10)
        self.data_type_etf_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(type_inner_frame, text="ETF", variable=self.data_type_etf_var).pack(side="left", padx=10)
        self.data_type_special_var = tk.BooleanVar(value=False) # Placeholder for future special data
        ttk.Checkbutton(type_inner_frame, text="特色数据", variable=self.data_type_special_var).pack(side="left", padx=10)

        # 时间跨度选择
        date_frame = ttk.LabelFrame(main_frame, text="时间跨度")
        date_frame.pack(fill="x", pady=5)
        
        date_inner_frame = ttk.Frame(date_frame)
        date_inner_frame.pack(padx=10, pady=10)
        
        ttk.Label(date_inner_frame, text="开始日期:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.start_date_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(date_inner_frame, textvariable=self.start_date_var, width=15).grid(row=0, column=1, padx=5, pady=5)
        ttk.Button(date_inner_frame, text="选择", command=lambda: self._show_calendar(self.start_date_var)).grid(row=0, column=2, padx=5, pady=5)

        ttk.Label(date_inner_frame, text="结束日期:").grid(row=0, column=3, padx=5, pady=5, sticky="e")
        self.end_date_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(date_inner_frame, textvariable=self.end_date_var, width=15).grid(row=0, column=4, padx=5, pady=5)
        ttk.Button(date_inner_frame, text="选择", command=lambda: self._show_calendar(self.end_date_var)).grid(row=0, column=5, padx=5, pady=5)

        # 按钮区域
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill="x", pady=5)
        
        self.full_download_button = ttk.Button(button_frame, text="全量下载", command=self.start_full_download)
        self.full_download_button.pack(side="left", expand=True, padx=5, pady=5)

        self.update_button = ttk.Button(button_frame, text="增量更新", command=self.start_incremental_update)
        self.update_button.pack(side="left", expand=True, padx=5, pady=5)

        self.pause_button = ttk.Button(button_frame, text="暂停下载", command=self._pause_download, state='disabled')
        self.pause_button.pack(side="left", expand=True, padx=5, pady=5)

        self.cancel_button = ttk.Button(button_frame, text="取消下载", command=self._cancel_download, state='disabled')
        self.cancel_button.pack(side="left", expand=True, padx=5, pady=5)

        # 进度条和状态
        progress_frame = ttk.LabelFrame(main_frame, text="下载进度")
        progress_frame.pack(fill="x", pady=5)
        
        self.download_progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", length=0, mode="determinate")
        self.download_progress_bar.pack(fill="x", padx=10, pady=5)

        self.download_status_label = ttk.Label(progress_frame, text="准备下载...", anchor="center")
        self.download_status_label.pack(fill="x", padx=10, pady=5)

        # Log Display
        log_frame = ttk.LabelFrame(main_frame, text="日志输出")
        log_frame.pack(fill="both", expand=True, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, width=80, height=20, state='disabled', font=('Consolas', 9))
        self.log_text.pack(expand=True, fill="both", padx=10, pady=10)
        
        # 设置日志文本的样式
        self.log_text.tag_configure('INFO', foreground='blue')
        self.log_text.tag_configure('ERROR', foreground='red')
        self.log_text.tag_configure('WARNING', foreground='orange')

    def _create_export_tab(self, parent_frame):
        # 主框架
        main_frame = ttk.Frame(parent_frame)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # 导出控制区域
        export_frame = ttk.LabelFrame(main_frame, text="导出控制")
        export_frame.pack(fill="x", pady=5)
        
        export_inner_frame = ttk.Frame(export_frame)
        export_inner_frame.pack(padx=10, pady=10)

        # 股票代码
        ttk.Label(export_inner_frame, text="股票代码 (空为全部):").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.export_code_var = tk.StringVar(value="")
        ttk.Entry(export_inner_frame, textvariable=self.export_code_var, width=30).grid(row=0, column=1, padx=5, pady=5, sticky="w")

        # 开始日期
        ttk.Label(export_inner_frame, text="开始日期:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        self.export_start_date_var = tk.StringVar(value="")
        ttk.Entry(export_inner_frame, textvariable=self.export_start_date_var, width=15).grid(row=1, column=1, padx=5, pady=5, sticky="w")
        ttk.Button(export_inner_frame, text="选择", command=lambda: self._show_calendar(self.export_start_date_var)).grid(row=1, column=2, padx=5, pady=5)

        # 结束日期
        ttk.Label(export_inner_frame, text="结束日期:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        self.export_end_date_var = tk.StringVar(value="")
        ttk.Entry(export_inner_frame, textvariable=self.export_end_date_var, width=15).grid(row=2, column=1, padx=5, pady=5, sticky="w")
        ttk.Button(export_inner_frame, text="选择", command=lambda: self._show_calendar(self.export_end_date_var)).grid(row=2, column=2, padx=5, pady=5)

        # 输出文件路径
        ttk.Label(export_inner_frame, text="输出文件路径:").grid(row=3, column=0, padx=5, pady=5, sticky="e")
        self.export_output_path_var = tk.StringVar(value="output.csv")
        ttk.Entry(export_inner_frame, textvariable=self.export_output_path_var, width=30).grid(row=3, column=1, padx=5, pady=5, sticky="w")
        ttk.Button(export_inner_frame, text="浏览", command=self._browse_output_file).grid(row=3, column=2, padx=5, pady=5)

        # 数据频率
        ttk.Label(export_inner_frame, text="数据频率:").grid(row=4, column=0, padx=5, pady=5, sticky="e")
        self.export_frequency_var = tk.StringVar(value="d")
        frequency_frame = ttk.Frame(export_inner_frame)
        frequency_frame.grid(row=4, column=1, columnspan=2, padx=5, pady=5, sticky="w")
        export_frequency_options = [("日线", "d"), ("周线", "w"), ("月线", "m")]
        for i, (text, value) in enumerate(export_frequency_options):
            ttk.Radiobutton(frequency_frame, text=text, variable=self.export_frequency_var, value=value).pack(side="left", padx=10)

        # 输出格式
        ttk.Label(export_inner_frame, text="输出格式:").grid(row=5, column=0, padx=5, pady=5, sticky="e")
        self.export_format_var = tk.StringVar(value="csv")
        format_frame = ttk.Frame(export_inner_frame)
        format_frame.grid(row=5, column=1, columnspan=2, padx=5, pady=5, sticky="w")
        export_format_options = [("CSV", "csv"), ("Parquet", "parquet"), ("JSON", "json")]
        for i, (text, value) in enumerate(export_format_options):
            ttk.Radiobutton(format_frame, text=text, variable=self.export_format_var, value=value).pack(side="left", padx=10)

        # 导出按钮
        ttk.Button(export_frame, text="导出数据", command=self.start_export_data).pack(fill="x", padx=10, pady=10)

        # 日志输出
        log_frame = ttk.LabelFrame(main_frame, text="导出日志")
        log_frame.pack(fill="both", expand=True, pady=5)

        self.export_log_text = scrolledtext.ScrolledText(log_frame, width=80, height=15, state='disabled', font=('Consolas', 9))
        self.export_log_text.pack(expand=True, fill="both", padx=10, pady=10)
        
        # 设置日志文本的样式
        self.export_log_text.tag_configure('INFO', foreground='blue')
        self.export_log_text.tag_configure('ERROR', foreground='red')
        self.export_log_text.tag_configure('WARNING', foreground='orange')

    def _create_db_manage_tab(self, parent_frame):
        # 主框架
        main_frame = ttk.Frame(parent_frame)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # 数据库信息区域
        db_info_frame = ttk.LabelFrame(main_frame, text="数据库信息")
        db_info_frame.pack(fill="x", pady=5)
        
        info_inner_frame = ttk.Frame(db_info_frame)
        info_inner_frame.pack(padx=10, pady=10)

        ttk.Label(info_inner_frame, text="数据库路径:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.db_path_label = ttk.Label(info_inner_frame, text=DATABASE_PATH, font=('Consolas', 9))
        self.db_path_label.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        # 数据库操作按钮
        button_frame = ttk.Frame(db_info_frame)
        button_frame.pack(fill="x", padx=10, pady=10)

        ttk.Button(button_frame, text="查看数据库状态", command=self.show_db_status).pack(fill="x", pady=5)
        ttk.Button(button_frame, text="清理所有数据 (慎重!)", command=self.clear_all_data, style='Danger.TButton').pack(fill="x", pady=5)

        # 数据库状态显示
        status_frame = ttk.LabelFrame(main_frame, text="数据库状态")
        status_frame.pack(fill="both", expand=True, pady=5)

        self.db_status_text = scrolledtext.ScrolledText(status_frame, width=80, height=15, state='disabled', font=('Consolas', 9))
        self.db_status_text.pack(expand=True, fill="both", padx=10, pady=10)
        
        # 设置样式
        self.db_status_text.tag_configure('INFO', foreground='blue')
        self.db_status_text.tag_configure('ERROR', foreground='red')
        self.db_status_text.tag_configure('WARNING', foreground='orange')

    def _create_config_tab(self, parent_frame):
        # 主框架
        main_frame = ttk.Frame(parent_frame)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # 配置区域
        config_frame = ttk.LabelFrame(main_frame, text="通用配置")
        config_frame.pack(fill="x", pady=5)
        
        config_inner_frame = ttk.Frame(config_frame)
        config_inner_frame.pack(padx=10, pady=10)

        # Baostock 复权类型
        ttk.Label(config_inner_frame, text="Baostock 复权类型:").grid(row=0, column=0, padx=5, pady=10, sticky="e")
        self.baostock_adjust_flag_var = tk.StringVar(value=BAOSTOCK_ADJUST_FLAG)
        adjust_frame = ttk.Frame(config_inner_frame)
        adjust_frame.grid(row=0, column=1, padx=5, pady=10, sticky="w")
        adjust_flag_options = [("后复权", "1"), ("前复权", "2"), ("不复权", "3")]
        for i, (text, value) in enumerate(adjust_flag_options):
            ttk.Radiobutton(adjust_frame, text=text, variable=self.baostock_adjust_flag_var, value=value).pack(side="left", padx=10)

        # 最大并发进程数
        ttk.Label(config_inner_frame, text="最大并发进程数:").grid(row=1, column=0, padx=5, pady=10, sticky="e")
        self.max_workers_var = tk.IntVar(value=config_loader.get("concurrency.max_workers", 4))
        ttk.Spinbox(config_inner_frame, from_=1, to=10, textvariable=self.max_workers_var, width=5).grid(row=1, column=1, padx=5, pady=10, sticky="w")

        # 数据源选择
        ttk.Label(config_inner_frame, text="默认数据源:").grid(row=2, column=0, padx=5, pady=10, sticky="e")
        self.default_data_source_var = tk.StringVar(value=config_loader.get("data_source.default", "baostock"))
        data_source_options = [("Baostock", "baostock"), ("其他", "other")]
        data_source_frame = ttk.Frame(config_inner_frame)
        data_source_frame.grid(row=2, column=1, padx=5, pady=10, sticky="w")
        for i, (text, value) in enumerate(data_source_options):
            ttk.Radiobutton(data_source_frame, text=text, variable=self.default_data_source_var, value=value).pack(side="left", padx=10)

        # 保存按钮
        ttk.Button(config_frame, text="保存配置", command=self.save_config).pack(fill="x", padx=10, pady=10)

        # 关于区域
        about_frame = ttk.LabelFrame(main_frame, text="关于")
        about_frame.pack(fill="both", expand=True, pady=5)
        
        about_inner_frame = ttk.Frame(about_frame)
        about_inner_frame.pack(padx=10, pady=10)

        ttk.Label(about_inner_frame, text="股票数据中枢 v1.0").pack(pady=5)
        ttk.Label(about_inner_frame, text="一个功能强大的股票数据下载和管理工具").pack(pady=5)
        ttk.Button(about_inner_frame, text="查看帮助", command=self.show_help).pack(pady=5)
        ttk.Button(about_inner_frame, text="检查更新", command=self.check_update).pack(pady=5)

    def _browse_output_file(self):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("Parquet files", "*.parquet"), ("JSON files", "*.json"), ("All files", "*.*")]
        )
        if file_path:
            self.export_output_path_var.set(file_path)

    def _show_calendar(self, date_var):
        """显示日历选择器"""
        try:
            # 尝试从tkinter.ttk导入Calendar
            from tkinter import Toplevel
            from tkinter.ttk import Calendar
            
            top = Toplevel(self.master)
            top.title("选择日期")
            
            # 创建日历控件
            cal_widget = Calendar(top, selectmode="day", year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
            cal_widget.pack(padx=10, pady=10)
            
            def select_date():
                selected_date = cal_widget.selection_get()
                date_var.set(selected_date.strftime("%Y-%m-%d"))
                top.destroy()
            
            # 确认按钮
            ttk.Button(top, text="确认", command=select_date).pack(pady=10)
        except ImportError:
            # 如果Calendar不可用，使用简单的输入对话框
            from tkinter import simpledialog
            current_date = date_var.get()
            new_date = simpledialog.askstring("输入日期", "请输入日期 (YYYY-MM-DD):", initialvalue=current_date)
            if new_date:
                try:
                    # 验证日期格式
                    datetime.strptime(new_date, "%Y-%m-%d")
                    date_var.set(new_date)
                except ValueError:
                    self.gui_logger.warning("日期格式不正确，请使用 YYYY-MM-DD 格式。")

    def process_log_queue(self):
        """Process messages from the log queue and display them in the text area."""
        while not self.log_queue.empty():
            record = self.log_queue.get()
            msg = self.queue_handler.format(record)
            self.log_text.configure(state='normal')
            # 根据日志级别设置不同的颜色
            if record.levelno == logging.ERROR:
                self.log_text.insert(tk.END, msg + '\n', 'ERROR')
            elif record.levelno == logging.WARNING:
                self.log_text.insert(tk.END, msg + '\n', 'WARNING')
            else:
                self.log_text.insert(tk.END, msg + '\n', 'INFO')
            self.log_text.configure(state='disabled')
            self.log_text.see(tk.END)
        self.master.after(100, self.process_log_queue)

    def _run_in_thread(self, target_function, *args, **kwargs):
        """Helper to run a function in a separate thread."""
        thread = threading.Thread(target=target_function, args=args, kwargs=kwargs)
        thread.daemon = True # Allow the program to exit even if threads are running
        thread.start()

    def start_full_download(self):
        self.gui_logger.info("开始全量下载...")
        self._set_buttons_state('disabled')
        self.is_paused.set(False)
        self.is_cancelled.set(False)
        self.pause_button.config(text="暂停下载")

        # 获取用户选择的时间范围
        start_date = self.start_date_var.get()
        end_date = self.end_date_var.get()

        # 验证日期格式
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            self.gui_logger.warning("日期格式不正确，请使用 YYYY-MM-DD 格式。")
            self._set_buttons_state('normal')
            return

        # 验证开始日期不晚于结束日期
        if start_date > end_date:
            self.gui_logger.warning("开始日期不能晚于结束日期。")
            self._set_buttons_state('normal')
            return

        selected_frequencies = []
        if self.freq_daily_var.get():
            selected_frequencies.append("d")
        if self.freq_weekly_var.get():
            selected_frequencies.append("w")
        if self.freq_monthly_var.get():
            selected_frequencies.append("m")

        selected_data_types = []
        if self.data_type_stock_var.get():
            selected_data_types.append("stock")
        if self.data_type_etf_var.get():
            selected_data_types.append("etf")
        if self.data_type_special_var.get():
            selected_data_types.append("special")

        if not selected_frequencies or not selected_data_types:
            self.gui_logger.warning("请至少选择一个数据频率和一个数据类型进行下载。")
            self._set_buttons_state('normal')
            return

        self.total_download_tasks = len(selected_frequencies) * len(selected_data_types)
        self.completed_download_tasks = 0
        self.download_progress_bar['value'] = 0
        self.download_status_label.config(text=f"开始 {self.total_download_tasks} 个下载任务...")

        # Use a single thread to manage multiple download tasks sequentially
        # Each _do_full_download will run its own pipeline in multiprocessing
        self._run_in_thread(self._do_multi_full_download, selected_frequencies, selected_data_types, start_date, end_date)

    def _do_multi_full_download(self, selected_frequencies, selected_data_types, start_date, end_date):
        try:
            for data_type in selected_data_types:
                if self.is_cancelled.get():
                    self.gui_logger.info("下载任务已取消。")
                    break
                for frequency in selected_frequencies:
                    if self.is_cancelled.get():
                        self.gui_logger.info("下载任务已取消。")
                        break
                    task_description = f"[{data_type.upper()}-{frequency.upper()}]"
                    self.gui_logger.info(f"开始下载任务: {task_description}")
                    self._do_full_download_single(frequency, data_type, task_description, start_date, end_date)
                    self.completed_download_tasks += 1
                    overall_progress = (self.completed_download_tasks / self.total_download_tasks) * 100
                    self.download_progress_bar['value'] = overall_progress
                    self.download_status_label.config(text=f"完成 {task_description} ({self.completed_download_tasks}/{self.total_download_tasks})")
            self.gui_logger.info("所有全量下载任务完成。")
        except Exception as e:
            self.gui_logger.error(f"多任务全量下载失败: {e}")
        finally:
            self._set_buttons_state('normal')
            self.download_progress_bar['value'] = 0 # Reset progress bar
            self.download_status_label.config(text="") # Clear status

    def _do_full_download_single(self, frequency, data_type, task_description, start_date, end_date):
        try:
            self.pipeline.full_download_pipeline(frequency=frequency, progress_callback=self._update_progress, data_type=data_type, start_date=start_date, end_date=end_date)
            self.gui_logger.info(f"任务 {task_description} 完成。")
        except Exception as e:
            self.gui_logger.error(f"任务 {task_description} 失败: {e}")

    def start_incremental_update(self):
        self.gui_logger.info("开始增量更新...")
        self._set_buttons_state('disabled')
        self.is_paused.set(False)
        self.is_cancelled.set(False)
        self.pause_button.config(text="暂停下载")

        # 获取用户选择的时间范围
        start_date = self.start_date_var.get()
        end_date = self.end_date_var.get()

        # 验证日期格式
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            self.gui_logger.warning("日期格式不正确，请使用 YYYY-MM-DD 格式。")
            self._set_buttons_state('normal')
            return

        # 验证开始日期不晚于结束日期
        if start_date > end_date:
            self.gui_logger.warning("开始日期不能晚于结束日期。")
            self._set_buttons_state('normal')
            return

        selected_frequencies = []
        if self.freq_daily_var.get():
            selected_frequencies.append("d")
        if self.freq_weekly_var.get():
            selected_frequencies.append("w")
        if self.freq_monthly_var.get():
            selected_frequencies.append("m")

        selected_data_types = []
        if self.data_type_stock_var.get():
            selected_data_types.append("stock")
        if self.data_type_etf_var.get():
            selected_data_types.append("etf")
        if self.data_type_special_var.get():
            selected_data_types.append("special")

        if not selected_frequencies or not selected_data_types:
            self.gui_logger.warning("请至少选择一个数据频率和一个数据类型进行更新。")
            self._set_buttons_state('normal')
            return

        self.total_download_tasks = len(selected_frequencies) * len(selected_data_types)
        self.completed_download_tasks = 0
        self.download_progress_bar['value'] = 0
        self.download_status_label.config(text=f"开始 {self.total_download_tasks} 个更新任务...")

        self._run_in_thread(self._do_multi_incremental_update, selected_frequencies, selected_data_types, start_date, end_date)

    def _do_multi_incremental_update(self, selected_frequencies, selected_data_types, start_date, end_date):
        try:
            for data_type in selected_data_types:
                if self.is_cancelled.get():
                    self.gui_logger.info("更新任务已取消。")
                    break
                for frequency in selected_frequencies:
                    if self.is_cancelled.get():
                        self.gui_logger.info("更新任务已取消。")
                        break
                    task_description = f"[{data_type.upper()}-{frequency.upper()}]"
                    self.gui_logger.info(f"开始更新任务: {task_description}")
                    self._do_incremental_update_single(frequency, data_type, task_description, start_date, end_date)
                    self.completed_download_tasks += 1
                    overall_progress = (self.completed_download_tasks / self.total_download_tasks) * 100
                    self.download_progress_bar['value'] = overall_progress
                    self.download_status_label.config(text=f"完成 {task_description} ({self.completed_download_tasks}/{self.total_download_tasks})")
            self.gui_logger.info("所有增量更新任务完成。")
        except Exception as e:
            self.gui_logger.error(f"多任务增量更新失败: {e}")
        finally:
            self._set_buttons_state('normal')
            self.download_progress_bar['value'] = 0 # Reset progress bar
            self.download_status_label.config(text="") # Clear status

    def _do_incremental_update_single(self, frequency, data_type, task_description, start_date, end_date):
        try:
            self.pipeline.daily_update_pipeline(frequency=frequency, progress_callback=self._update_progress, data_type=data_type, start_date=start_date, end_date=end_date)
            self.gui_logger.info(f"任务 {task_description} 完成。")
        except Exception as e:
            self.gui_logger.error(f"任务 {task_description} 失败: {e}")

    def _pause_download(self):
        if not self.is_paused.get():
            self.is_paused.set(True)
            self.pipeline.set_pause()
            self.gui_logger.info("下载已暂停。")
            self.pause_button.config(text="继续下载")
        else:
            self.is_paused.set(False)
            self.pipeline.clear_pause()
            self.gui_logger.info("下载已恢复。")
            self.pause_button.config(text="暂停下载")

    def _cancel_download(self):
        self.is_cancelled.set(True)
        self.pipeline.set_cancel()
        self.gui_logger.info("下载已取消。")
        self._set_buttons_state('normal') # Re-enable buttons immediately
        self.download_progress_bar['value'] = 0
        self.download_status_label.config(text="下载已取消")

    def start_export_data(self):
        self.gui_logger.info("开始导出数据...")
        self._set_buttons_state('disabled')
        code = self.export_code_var.get()
        start_date = self.export_start_date_var.get()
        end_date = self.export_end_date_var.get()
        output_path = self.export_output_path_var.get()
        frequency = self.export_frequency_var.get()
        format_type = self.export_format_var.get()

        if not start_date or not end_date or not output_path:
            self.gui_logger.warning("导出数据需要填写开始日期、结束日期和输出文件路径。")
            self._set_buttons_state('normal')
            return

        self._run_in_thread(self._do_export_data, code, start_date, end_date, output_path, frequency, format_type)

    def _do_export_data(self, code, start_date, end_date, output_path, frequency, format_type):
        try:
            success = self.db_manager.export_data(code, start_date, end_date, output_path, frequency, format_type)
            if success:
                self.gui_logger.info(f"✅ 数据导出成功: {output_path}")
            else:
                self.gui_logger.error("❌ 数据导出失败")
        except Exception as e:
            self.gui_logger.error(f"导出失败: {e}")
        finally:
            self._set_buttons_state('normal')

    def show_db_status(self):
        self.gui_logger.info("查询数据库状态...")
        self._run_in_thread(self._do_show_db_status)

    def _do_show_db_status(self):
        try:
            frequencies = ["d", "w", "m"]
            status_messages = []
            for freq in frequencies:
                record_count, stock_count = self.db_manager.get_table_status(freq)
                status_messages.append(f"  频率: {freq.upper()} - 总记录数: {record_count}, 股票数量: {stock_count}")
            
            self.gui_logger.info("📊 数据库状态:\n" + "\n".join(status_messages))
        except Exception as e:
            self.gui_logger.error(f"查询数据库状态失败: {e}")

    def clear_all_data(self):
        if tk.messagebox.askyesno("确认", "确定要清理所有数据库数据吗？此操作不可逆！"): # type: ignore
            self.gui_logger.info("开始清理所有数据...")
            self._set_buttons_state('disabled')
            self._run_in_thread(self._do_clear_all_data)

    def _do_clear_all_data(self):
        try:
            self.db_manager.clear_all_tables()
            self.gui_logger.info("所有数据清理完成。")
        except Exception as e:
            self.gui_logger.error(f"清理数据失败: {e}")
        finally:
            self._set_buttons_state('normal')
            self.show_db_status() # Refresh status after clearing

    def save_config(self):
        """保存配置到config.yaml文件"""
        try:
            # 获取当前配置值
            new_adjust_flag = self.baostock_adjust_flag_var.get()
            new_max_workers = self.max_workers_var.get()
            new_data_source = self.default_data_source_var.get()
            
            # 读取当前配置
            config = config_loader.config
            
            # 更新配置
            config['baostock']['adjust_flag'] = new_adjust_flag
            config['concurrency']['max_workers'] = new_max_workers
            config['data_source']['default'] = new_data_source
            
            # 保存配置到文件
            import yaml
            with open('config.yaml', 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
            
            self.gui_logger.info("配置保存成功。")
        except Exception as e:
            self.gui_logger.error(f"保存配置失败: {e}")

    def show_help(self):
        """显示帮助信息"""
        help_text = """
股票数据中枢使用帮助:

1. 数据下载:
   - 选择数据频率和数据类型
   - 设置时间跨度
   - 点击"全量下载"或"增量更新"

2. 数据导出:
   - 输入股票代码（空为全部）
   - 设置时间范围
   - 选择输出格式
   - 点击"导出数据"

3. 数据库管理:
   - 查看数据库状态
   - 清理所有数据（慎重操作）

4. 配置:
   - 设置Baostock复权类型
   - 调整最大并发进程数
   - 选择默认数据源

注意事项:
- 首次使用时建议进行全量下载
- 增量更新会自动计算需要更新的日期范围
- 数据导出支持CSV、Parquet和JSON格式
        """
        from tkinter import messagebox
        messagebox.showinfo("帮助", help_text)

    def check_update(self):
        """检查更新"""
        from tkinter import messagebox
        messagebox.showinfo("检查更新", "当前版本为 v1.0，暂无更新。")

    def _set_buttons_state(self, state):
        self.full_download_button.config(state=state)
        self.update_button.config(state=state)
        if state == 'disabled':
            # 下载开始时，保持暂停和取消按钮可用
            self.pause_button.config(state='normal')
            self.cancel_button.config(state='normal')
        else:
            # 下载结束时，禁用暂停和取消按钮
            self.pause_button.config(state=state)
            self.cancel_button.config(state=state)

    def _update_progress(self, current, total, message):
        """Update the progress bar and status label."""
        # Ensure UI updates are done on the main thread
        self.master.after(0, self.__update_progress_gui, current, total, message)

    def __update_progress_gui(self, current, total, message):
        # current now represents the overall progress percentage (0-100)
        # total is always 100 in this case
        if total == 100:
            # 直接使用current作为进度百分比
            self.download_progress_bar['value'] = current
            self.download_status_label.config(text=f"{message} (总进度: {current}%)")
        else:
            # 保持原有逻辑，处理其他情况
            current_task_contribution = 0
            if total > 0: # Avoid division by zero for current task progress
                current_task_contribution = current / total

            # Ensure self.total_download_tasks is not zero before calculating overall progress
            if self.total_download_tasks > 0:
                current_overall_progress = (self.completed_download_tasks + current_task_contribution) / self.total_download_tasks * 100
                self.download_progress_bar['value'] = current_overall_progress
                self.download_status_label.config(text=f"{message} (总进度: {int(current_overall_progress)}%)")
            else:
                # If there are no total tasks (e.g., no selections), just show 0 progress
                self.download_progress_bar['value'] = 0
                self.download_status_label.config(text=message)
        # Add other buttons as they are implemented


if __name__ == "__main__":
    root = tk.Tk()
    app = StockDataGUI(root)
    root.mainloop()
