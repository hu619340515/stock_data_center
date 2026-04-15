from abc import ABC, abstractmethod
import pandas as pd

class DataSourceInterface(ABC):
    """
    📊 数据源接口
    定义所有数据源必须实现的方法
    """
    
    @abstractmethod
    def login(self):
        """
        🔐 登录数据源
        """
        pass
    
    @abstractmethod
    def logout(self):
        """
        👋 登出数据源
        """
        pass
    
    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """
        📋 获取股票列表
        返回包含股票代码和名称的DataFrame
        """
        pass
    
    @abstractmethod
    def get_stock_history(self, code: str, start_date: str, end_date: str, frequency: str = "d") -> pd.DataFrame:
        """
        📈 获取股票历史数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率 (d: 日线, w: 周线, m: 月线, 1: 1分钟, 5: 5分钟)
            
        Returns:
            包含历史数据的DataFrame
        """
        pass
    
    @abstractmethod
    def get_data_source_name(self) -> str:
        """
        📛 获取数据源名称
        """
        pass