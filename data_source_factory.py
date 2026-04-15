from data_source_interface import DataSourceInterface
from data_source import BaoStockClient

class DataSourceFactory:
    """
    🏭 数据源工厂
    根据配置创建不同的数据源实例
    """
    
    @staticmethod
    def create_data_source(source_type: str) -> DataSourceInterface:
        """
        📱 创建数据源实例
        
        Args:
            source_type: 数据源类型 (baostock, tushare, etc.)
            
        Returns:
            数据源实例
        """
        if source_type.lower() == "baostock":
            return BaoStockClient()
        # 未来可以添加其他数据源
        # elif source_type.lower() == "tushare":
        #     return TuShareClient()
        # elif source_type.lower() == "sina":
        #     return SinaClient()
        else:
            raise ValueError(f"不支持的数据源类型: {source_type}")