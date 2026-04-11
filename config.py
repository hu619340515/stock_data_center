import os
from datetime import datetime

# --- 路径配置 ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
# 🦆 DuckDB 数据库文件路径 (单文件数据库)
DATABASE_PATH = os.path.join(PROJECT_ROOT, "quant_data.db")

# --- Baostock 配置 ---
BAOSTOCK_ADJUST_FLAG = "2" # 前复权
END_DATE = datetime.now().strftime("%Y-%m-%d") 
START_DATE_FULL = "1999-01-01" 

# --- 并发与性能 ---
# DuckDB 对并发写入有限制（文件锁），建议全量下载时用 1，或者使用连接池
# 这里为了稳定，保持单线程或低线程
MAX_WORKERS = 1