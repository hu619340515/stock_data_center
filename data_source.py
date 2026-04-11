import baostock as bs
import pandas as pd
from datetime import timedelta
from logger_config import setup_logger
import traceback

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
        """
        获取A股股票列表（智能处理非交易日）
        """
        self.login()
        
        # 1. 尝试使用昨天作为查询日期（通常最稳妥）
        yesterday = (pd.Timestamp.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 2. 如果昨天是周末，可能需要往前推，但Baostock通常能处理非交易日请求返回最近数据
        # 这里我们简单尝试用昨天
        logger.info(f"📅 尝试获取股票列表 (查询日期: {yesterday})...")
        rs = bs.query_all_stock(day=yesterday)
        
        # 3. 如果失败，尝试使用代码中硬编码的备用日期
        if rs.error_code != '0':
            logger.warning(f"⚠️ 默认日期查询失败: {rs.error_msg}，尝试备用日期...")
            # 这里用一个已知的过去交易日
            rs = bs.query_all_stock(day="2024-12-31")
            if rs.error_code != '0':
                logger.error(f"❌ 备用日期也失败: {rs.error_msg}")
                return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.error("❌ 获取到的股票列表为空")
            return pd.DataFrame()
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 4. 过滤A股 (保留 sh.6, sz.0, sz.3, bj.)
        # 注意：Baostock 返回的代码格式通常是 sh.600000
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
            adjustflag="2" # 前复权
        )
        
        if rs.error_code != '0':
            logger.error(f"❌ 获取数据失败 {code}: {rs.error_msg}")
            return pd.DataFrame()
            
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            return pd.DataFrame()
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        return df