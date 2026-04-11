import os
from datetime import datetime

# --- 路径配置 ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
# OUTPUT_DIR 已废弃，因为我们不再保存CSV

# --- MongoDB 配置 ---
MONGODB_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "stock_data"
COLLECTION_NAME = "stock_daily" # 统一集合

# --- Baostock 配置 ---
BAOSTOCK_ADJUST_FLAG = "2"  # 前复权
END_DATE = datetime.now().strftime("%Y-%m-%d") 
START_DATE_FULL = "1999-01-01" 

# --- 并发与性能 ---
MAX_WORKERS = 1 # 根据你的CPU核心数调整，建议 5-10
CHUNK_SIZE = 100 # 批量写入MongoDB的块大小