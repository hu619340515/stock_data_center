import baostock as bs
import pandas as pd
from datetime import timedelta
from logger_config import setup_logger

logger = setup_logger("DataFetcher")

class BaoStockClient:
    def __init__(self):
        self.lg = None

    def login(self):
        if self.lg is None:
            logger.info("🚀 正在登录 Baostock...")
            self.lg = bs.login()
            if self.lg.error_code != '0':
                raise Exception(f"Login Failed: {self.lg.error_msg}")
            logger.info("✅ Baostock 登录成功")

    def logout(self):
        if self.lg:
            bs.logout()
            logger.info("👋 Baostock 登出")

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
                return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list: return pd.DataFrame()
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        # 过滤A股
        df = df[df['code'].str.contains(r'sh\.6|sz\.0|sz\.3|bj\.')]
        
        logger.info(f"✅ 获取到 {len(df)} 只A股股票")
        return df

    def get_stock_history(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        self.login()
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2" 
        )
        
        if rs.error_code != '0':
            logger.error(f"❌ 获取数据失败 {code}: {rs.error_msg}")
            return pd.DataFrame()
            
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list: return pd.DataFrame()
            
        return pd.DataFrame(data_list, columns=rs.fields)