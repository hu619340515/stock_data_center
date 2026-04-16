import logging
import os
import multiprocessing
from logging.handlers import RotatingFileHandler
from config import LOG_DIR

def setup_logger(name="StockLogger", log_file="app.log", level=logging.INFO):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # 为每个进程创建不同的日志文件，避免文件锁定
    process_id = multiprocessing.current_process().pid
    log_file_with_pid = f"{log_file}.{process_id}"
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler = RotatingFileHandler(
        os.path.join(LOG_DIR, log_file_with_pid), 
        maxBytes=10*1024*1024, 
        backupCount=5,
        encoding='utf-8'
    )
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if not logger.handlers:
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler()) # 屏幕输出
    
    return logger