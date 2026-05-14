import os
from datetime import datetime
import yaml
from typing import Any, Dict

# --- 路径配置 ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

class ConfigLoader:
    """配置加载器"""
    
    def __init__(self, config_file: str = "config.yaml"):
        self.config_file = os.path.join(PROJECT_ROOT, config_file)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return self._replace_env_vars(config)
        except Exception as e:
            print(f"Warning: 加载配置文件失败: {e}")
            return self._get_default_config()
    
    def _replace_env_vars(self, config: Any) -> Any:
        """替换环境变量"""
        if isinstance(config, str):
            # 替换 ${VAR} 格式的环境变量
            import re
            pattern = r'\$\{([^}]+)\}'
            def replace_var(match):
                var_name = match.group(1)
                return os.environ.get(var_name, match.group(0))
            return re.sub(pattern, replace_var, config)
        elif isinstance(config, dict):
            return {k: self._replace_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._replace_env_vars(item) for item in config]
        else:
            return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "database": {
                "path": "quant_data.db"
            },
            "baostock": {
                "adjust_flag": "2"
            },
            "concurrency": {
                "max_workers": 4,
                "dynamic_concurrency": True,
                "min_workers": 2,
                "max_workers_limit": 5,
                "error_threshold": 3,
                "success_threshold": 95
            },
            "batch": {
                "size": 50,
                "max_size": 200,
                "min_size": 20,
                "memory_threshold": 0.8
            },
            "processing": {
                "use_arrow": False,
                "compress_data": True
            },
            "retry": {
                "max_retries": 3,
                "initial_retry_delay": 1,
                "max_retry_delay": 30,
                "retry_backoff_factor": 2
            },
            "error": {
                "log_file": "error_log.txt",
                "max_errors_before_warning": 10
            },
            "datasource": {
                "default": "baostock",
                "enable_fallback": True,
                "priority": ["baostock", "akshare"]
            },
            "data": {
                "start_date_full": "1999-01-01"
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

# 初始化配置加载器
config_loader = ConfigLoader()

# 数据库配置
DATABASE_PATH = os.path.join(PROJECT_ROOT, config_loader.get("database.path", "quant_data.db"))

# Baostock 配置
BAOSTOCK_ADJUST_FLAG = config_loader.get("baostock.adjust_flag", "2")
START_DATE_FULL = config_loader.get("data.start_date_full", "1999-01-01")

# 并发与性能
MAX_WORKERS = config_loader.get("concurrency.max_workers", 4)
DYNAMIC_CONCURRENCY = config_loader.get("concurrency.dynamic_concurrency", True)
MIN_WORKERS = config_loader.get("concurrency.min_workers", 2)
MAX_WORKERS_LIMIT = config_loader.get("concurrency.max_workers_limit", 5)
ERROR_THRESHOLD = config_loader.get("concurrency.error_threshold", 3)
SUCCESS_THRESHOLD = config_loader.get("concurrency.success_threshold", 95)

# 批量写入优化
BATCH_SIZE = config_loader.get("batch.size", 50)
MAX_BATCH_SIZE = config_loader.get("batch.max_size", 200)
MIN_BATCH_SIZE = config_loader.get("batch.min_size", 20)
MEMORY_THRESHOLD = config_loader.get("batch.memory_threshold", 0.8)

# 数据传输优化
USE_ARROW = config_loader.get("processing.use_arrow", False)
COMPRESS_DATA = config_loader.get("processing.compress_data", True)

# 重试机制配置
MAX_RETRIES = config_loader.get("retry.max_retries", 3)
INITIAL_RETRY_DELAY = config_loader.get("retry.initial_retry_delay", 1)
MAX_RETRY_DELAY = config_loader.get("retry.max_retry_delay", 30)
RETRY_BACKOFF_FACTOR = config_loader.get("retry.retry_backoff_factor", 2)

# 异常处理配置
ERROR_LOG_FILE = config_loader.get("error.log_file", "error_log.txt")
MAX_ERRORS_BEFORE_WARNING = config_loader.get("error.max_errors_before_warning", 10)

# 数据源配置
DEFAULT_DATA_SOURCE = config_loader.get("datasource.default", "baostock")
ENABLE_DATA_SOURCE_FALLBACK = config_loader.get("datasource.enable_fallback", True)
DATA_SOURCE_PRIORITY = config_loader.get("datasource.priority", ["baostock"])