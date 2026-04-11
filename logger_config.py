import logging
import os
from logging.handlers import RotatingFileHandler
from config import LOG_DIR

def setup_logger(name="StockLogger", log_file="app.log", level=logging.INFO):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler = RotatingFileHandler(
        os.path.join(LOG_DIR, log_file), 
        maxBytes=10*1024*1024, 
        backupCount=5,
        encoding='utf-8'
    )
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler()) # 屏幕输出
    
    return logger