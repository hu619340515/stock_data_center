import logging
import logging.handlers
import os
import multiprocessing
import sys
from config import LOG_DIR


def configure_console_encoding():
    """Make console output tolerant of emoji/non-ASCII logs on Windows."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


configure_console_encoding()


def setup_logger(name="StockLogger", log_file="app.log", level=logging.INFO):
    """配置命名日志器（直接写文件 + 屏幕输出，适用于单进程/CLI 场景）"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, log_file), 
        maxBytes=10*1024*1024, 
        backupCount=5,
        encoding='utf-8'
    )
    handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    for old_handler in list(logger.handlers):
        logger.removeHandler(old_handler)
    logger.addHandler(handler)
    logger.addHandler(stream_handler) # 屏幕输出
    logger.propagate = False
    
    return logger


def _make_real_handlers(log_file="app.log", level=logging.INFO):
    """构建真实的文件 + 屏幕 handler（供 QueueListener 专用）"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, log_file),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    fh.setFormatter(formatter)
    
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    
    return [fh, sh]


def create_mp_log_queue(log_file="app.log", level=logging.INFO):
    """
    创建多进程安全的日志队列，并启动 QueueListener。
    仅主进程调用一次。
    
    Returns:
        (log_queue, listener): multiprocessing.Queue 和 QueueListener。
        任务结束时需调用 listener.stop()。
    """
    handlers = _make_real_handlers(log_file, level)
    log_queue = multiprocessing.Queue(-1)
    listener = logging.handlers.QueueListener(log_queue, *handlers, respect_handler_level=True)
    listener.start()
    return log_queue, listener


def attach_queue_handler(log_queue):
    """
    将当前进程所有 logger 的 handler 替换为 QueueHandler。
    主进程和各 worker 进程在启动后调用一次。
    之后所有日志通过队列流向 QueueListener 统一写入文件。
    """
    qh = logging.handlers.QueueHandler(log_queue)
    
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(qh)
    root.setLevel(logging.INFO)
    
    for name in list(logging.root.manager.loggerDict.keys()):
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)
        lg.propagate = True
        if lg.level == logging.NOTSET:
            lg.setLevel(logging.INFO)
