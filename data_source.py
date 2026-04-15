import baostock as bs
import pandas as pd
import time
import random
from datetime import timedelta
from logger_config import setup_logger
from config import MAX_RETRIES, INITIAL_RETRY_DELAY, MAX_RETRY_DELAY, RETRY_BACKOFF_FACTOR
from data_source_interface import DataSourceInterface

logger = setup_logger("DataFetcher")

def retry_with_backoff(func):
    """
    🛡️ 带指数退避的重试装饰器
    """
    def wrapper(*args, **kwargs):
        retries = 0
        delay = INITIAL_RETRY_DELAY
        
        while retries < MAX_RETRIES:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries >= MAX_RETRIES:
                    logger.error(f"❌ 达到最大重试次数，操作失败: {e}")
                    raise
                
                # 指数退避 + 随机抖动
                jitter = random.uniform(0.5, 1.5)
                wait_time = min(delay * (RETRY_BACKOFF_FACTOR ** (retries - 1)) * jitter, MAX_RETRY_DELAY)
                
                logger.warning(f"⚠️ 操作失败，{wait_time:.2f}秒后重试 ({retries}/{MAX_RETRIES}): {e}")
                time.sleep(wait_time)
        return None
    return wrapper

class BaoStockClient(DataSourceInterface):
    def __init__(self):
        self.lg = None

    @retry_with_backoff
    def login(self):
        if self.lg is None:
            logger.info("🚀 正在登录 Baostock...")
            self.lg = bs.login()
            if self.lg.error_code != '0':
                raise Exception(f"Login Failed: {self.lg.error_msg}")
            logger.info("✅ Baostock 登录成功")

    def logout(self):
        if self.lg:
            try:
                bs.logout()
                logger.info("👋 Baostock 登出")
            except Exception as e:
                logger.warning(f"⚠️ 登出失败: {e}")

    @retry_with_backoff
    def get_stock_list(self) -> pd.DataFrame:
        self.login()
        # 使用昨天作为查询日期
        yesterday = (pd.Timestamp.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"📅 尝试获取股票列表 (查询日期: {yesterday})...")
        rs = bs.query_all_stock(day=yesterday)
        
        # 如果失败，尝试备用日期
        if rs.error_code != '0':
            logger.warning(f"⚠️ 默认日期查询失败，尝试备用日期...")
            rs = bs.query_all_stock(day="2024-12-31")
            if rs.error_code != '0':
                logger.error(f"❌ 备用日期查询失败: {rs.error_msg}")
                return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning("⚠️ 未获取到股票列表数据")
            return pd.DataFrame()
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        # 过滤A股
        df = df[df['code'].str.contains(r'sh\.6|sz\.0|sz\.3|bj\.')]
        
        logger.info(f"✅ 获取到 {len(df)} 只A股股票")
        return df

    @retry_with_backoff
    def get_stock_history(self, code: str, start_date: str, end_date: str, frequency: str = "d") -> pd.DataFrame:
        self.login()
        # 映射频率参数
        frequency_map = {
            "d": "d",   # 日线
            "w": "w",   # 周线
            "m": "m",   # 月线
            "1": "1",   # 1分钟
            "5": "5"   # 5分钟
        }
        bs_frequency = frequency_map.get(frequency, "d")
        
        # 根据频率选择不同的字段列表
        if frequency in ["w", "m"]:
            # 周线和月线支持的字段
            fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
        else:
            # 日线和分钟线支持所有字段
            fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
        
        rs = bs.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency=bs_frequency,
            adjustflag="2" 
        )
        
        if rs.error_code != '0':
            # 检查是否是登录失效错误
            if "用户未登录" in rs.error_msg:
                logger.warning(f"⚠️ 会话已过期，需要重新登录: {rs.error_msg}")
                # 重置登录状态
                self.lg = None
                # 重新登录
                self.login()
                # 重新尝试获取数据
                rs = bs.query_history_k_data_plus(
                    code,
                    fields,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=bs_frequency,
                    adjustflag="2" 
                )
                if rs.error_code != '0':
                    logger.error(f"❌ 重新登录后获取数据失败 {code}: {rs.error_msg}")
                    raise Exception(f"获取数据失败 {code}: {rs.error_msg}")
            else:
                logger.error(f"❌ 获取数据失败 {code}: {rs.error_msg}")
                raise Exception(f"获取数据失败 {code}: {rs.error_msg}")
            
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning(f"⚠️ 未获取到 {code} 的历史数据")
            return pd.DataFrame()
            
        return pd.DataFrame(data_list, columns=rs.fields)
    
    def get_data_source_name(self) -> str:
        """
        📛 获取数据源名称
        """
        return "Baostock"