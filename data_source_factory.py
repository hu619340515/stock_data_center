from data_source_interface import DataSourceInterface
from data_source import QMTClient

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
            source_type: 数据源类型（仅支持 qmt/xtquant）
            
        Returns:
            数据源实例
        """
        normalized_type = source_type.lower()
        if normalized_type in ("qmt", "xtquant", "gjqmt", "guojin_qmt"):
            return QMTClient()
        else:
            raise ValueError(f"不支持的数据源类型: {source_type}。当前版本只支持 qmt/xtquant。")
