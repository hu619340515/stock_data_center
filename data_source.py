import baostock as bs
import pandas as pd
import time
import random
import re
import akshare as ak
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
        # 尝试多个日期，避免单日数据问题
        test_dates = [
            (pd.Timestamp.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            (pd.Timestamp.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
            (pd.Timestamp.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
            "2026-04-15",
            "2024-12-31"
        ]
        
        for test_date in test_dates:
            logger.info(f"📅 尝试获取股票列表 (查询日期: {test_date})...")
            rs = bs.query_all_stock(day=test_date)
            
            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                
                if len(data_list) > 0:
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    # 过滤A股
                    df = df[df['code'].str.contains(r'sh\.6|sz\.0|sz\.3|bj\.')]
                    logger.info(f"✅ 获取到 {len(df)} 只A股股票")
                    return df
                else:
                    logger.warning(f"⚠️ 日期 {test_date} 返回数据为空，尝试下一个日期")
            else:
                logger.warning(f"⚠️ 日期 {test_date} 查询失败: {rs.error_msg}，尝试下一个日期")
        
        logger.error("❌ 所有日期均无法获取股票列表数据")
        return pd.DataFrame()

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
    
    @retry_with_backoff
    def get_etf_list(self) -> pd.DataFrame:
        """
        📋 获取ETF基金列表
        """
        self.login()
        # 尝试多个日期，避免单日数据问题
        test_dates = [
            (pd.Timestamp.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            (pd.Timestamp.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
            (pd.Timestamp.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
            "2026-04-15",
            "2024-12-31"
        ]
        
        for test_date in test_dates:
            logger.info(f"📅 尝试获取ETF列表 (查询日期: {test_date})...")
            rs = bs.query_all_stock(day=test_date)
            
            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                
                if len(data_list) > 0:
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    # 过滤ETF基金
                    # 沪市ETF: sh.51/56/58开头
                    # 深市ETF: sz.15/159开头
                    df = df[df['code'].str.contains(r'sh\.5[168]|sz\.15[09]')]
                    logger.info(f"✅ 获取到 {len(df)} 只ETF基金")
                    return df
                else:
                    logger.warning(f"⚠️ 日期 {test_date} 返回数据为空，尝试下一个日期")
            else:
                logger.warning(f"⚠️ 日期 {test_date} 查询失败: {rs.error_msg}，尝试下一个日期")
        
        logger.error("❌ 所有日期均无法获取ETF列表数据")
        return pd.DataFrame()
    
    def get_data_source_name(self) -> str:
        """
        📛 获取数据源名称
        """
        return "Baostock"


class AKShareClient(DataSourceInterface):
    def __init__(self):
        # AKShare无需登录
        pass
    
    @retry_with_backoff
    def login(self):
        # AKShare不需要登录，直接返回
        logger.info("✅ AKShare无需登录，已就绪")
        return
    
    def logout(self):
        # AKShare不需要登出
        logger.info("👋 AKShare已退出")
        return
    
    @retry_with_backoff
    def get_stock_list(self) -> pd.DataFrame:
        """
        📋 获取A股股票列表
        """
        logger.info("📅 正在获取A股股票列表...")
        df = ak.stock_zh_a_spot()
        # 格式化代码，添加市场前缀
        df['code'] = df.apply(lambda x: f"sh.{x['代码']}" if x['代码'].startswith('6') else f"sz.{x['代码']}", axis=1)
        df['code_name'] = df['名称']
        # 过滤A股
        df = df[df['code'].str.contains(r'sh\.6|sz\.0|sz\.3|bj\.')]
        logger.info(f"✅ 获取到 {len(df)} 只A股股票")
        return df[['code', 'code_name']]
    
    @retry_with_backoff
    def get_etf_list(self) -> pd.DataFrame:
        """
        📋 获取ETF基金列表
        """
        logger.info("📅 正在获取ETF基金列表...")
        df = ak.fund_etf_spot_em()  # 适配新版本AKShare接口
        # 格式化代码，添加市场前缀
        df['code'] = df.apply(lambda x: f"sh.{x['代码']}" if x['代码'].startswith('5') else f"sz.{x['代码']}", axis=1)
        df['code_name'] = df['名称']
        # 过滤所有ETF（51/56/58开头沪市，15/159开头深市）
        df = df[df['code'].str.contains(r'sh\.5[168]|sz\.15[09]')]
        
        logger.info(f"✅ 获取到 {len(df)} 只ETF基金")
        return df[['code', 'code_name']]
    
    @retry_with_backoff
    def get_stock_history(self, code: str, start_date: str, end_date: str, frequency: str = "d") -> pd.DataFrame:
        """
        📈 获取股票/ETF历史数据
        """
        # 转换频率参数
        period_map = {
            "d": "daily",
            "w": "weekly",
            "m": "monthly",
            "1": "1min",
            "5": "5min"
        }
        period = period_map.get(frequency, "daily")
        
        # 去掉代码前缀
        pure_code = code.split('.')[1]
        
        # 处理日期格式
        # 股票接口要求YYYYMMDD，基金接口要求YYYY-MM-DD
        start_date_stock = start_date.replace('-', '')
        end_date_stock = end_date.replace('-', '')
        start_date_fund = start_date
        end_date_fund = end_date
        
        # 判断是股票还是ETF
        is_etf = bool(re.match(r'sh\.5[168]|sz\.15[09]', code))
        
        if is_etf:
            # ETF改用新浪财经数据源，更稳定
            logger.info(f"📥 [新浪财经] 获取ETF {code} 数据: {start_date_fund} ~ {end_date_fund}")
            # 新浪接口需要带市场前缀的代码，如sh513310
            sina_symbol = code.replace('.', '')
            df = ak.fund_etf_hist_sina(
                symbol=sina_symbol
            )
            # 新浪接口不支持日期参数，获取全部数据后过滤
            if not df.empty:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df[(df['date'] >= start_date_fund) & (df['date'] <= end_date_fund)]
        else:
            # 股票保留原有的东方财富A股接口
            logger.info(f"📥 [AKShare] 获取股票 {code} 数据: {start_date_stock} ~ {end_date_stock}")
            df = ak.stock_zh_a_hist(
                symbol=pure_code,
                period=period,
                start_date=start_date_stock,
                end_date=end_date_stock,
                adjust="hfq"
            )
        
        if df.empty:
            logger.warning(f"⚠️ 未获取到 {code} 的历史数据")
            return pd.DataFrame()
        
        # 调试信息：返回数据的日期范围
        if not df.empty and '日期' in df.columns:
            min_date = pd.to_datetime(df['日期']).min().strftime('%Y-%m-%d')
            max_date = pd.to_datetime(df['日期']).max().strftime('%Y-%m-%d')
            logger.info(f"✅ {code} 返回数据范围: {min_date} ~ {max_date}，共 {len(df)} 条")
        
        # 格式化列名，统一格式
        df['code'] = code
        
        if is_etf:
            # 新浪财经ETF字段映射
            rename_dict = {
                "date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
                "amount": "amount"
            }
            # 新浪接口没有这些字段，补充默认值
            df['preclose'] = 0.0
            df['adjustflag'] = ""
            df['turn'] = 0.0
            df['tradestatus'] = "1"
            df['pctChg'] = 0.0
            df['isST'] = "0"
        else:
            # 东方财富股票字段映射
            rename_dict = {
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "前收盘": "preclose",
                "成交量": "volume",
                "成交额": "amount",
                "调整标志": "adjustflag",
                "换手率": "turn",
                "交易状态": "tradestatus",
                "涨跌幅": "pctChg",
                "是否ST": "isST"
            }
            # 处理可能缺少的字段
            for col in ["preclose", "adjustflag", "turn", "tradestatus", "pctChg", "isST"]:
                if col not in df.columns:
                    if col in ["preclose", "turn", "pctChg"]:
                        df[col] = 0.0
                    elif col == "adjustflag":
                        df[col] = ""
                    elif col == "tradestatus":
                        df[col] = "1"
                    elif col == "isST":
                        df[col] = "0"
        
        df.rename(columns=rename_dict, inplace=True)
        logger.info(f"📤 {code} 数据列: {df.columns.tolist()}")
        return df[['code', 'date', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST']]
    
    def get_data_source_name(self) -> str:
        """
        📛 获取数据源名称
        """
        return "AKShare"