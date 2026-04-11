import os
from datetime import datetime

# --- 路径配置 ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
DATABASE_PATH = os.path.join(PROJECT_ROOT, "quant_data.db")

# --- Baostock 配置 ---
BAOSTOCK_ADJUST_FLAG = "2"
START_DATE_FULL = "1999-01-01"

# --- 并发与性能 ---
# 现在可以设为 3 或 4 了！
# 建议不要超过 5，否则容易被 Baostock 封 IP
MAX_WORKERS = 10 