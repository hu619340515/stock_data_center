import unittest
from data_source import BaoStockClient

class TestDataSource(unittest.TestCase):
    """数据源测试"""
    
    def setUp(self):
        """设置测试环境"""
        self.client = BaoStockClient()
        self.client.login()
    
    def tearDown(self):
        """清理测试环境"""
        self.client.logout()
    
    def test_get_stock_list(self):
        """测试获取股票列表"""
        stock_list = self.client.get_stock_list()
        self.assertFalse(stock_list.empty)
        self.assertIn('code', stock_list.columns)
        self.assertIn('code_name', stock_list.columns)
    
    def test_get_stock_history(self):
        """测试获取股票历史数据"""
        # 测试获取上证指数数据
        df = self.client.get_stock_history('sh.000001', '2024-01-01', '2024-01-10')
        self.assertFalse(df.empty)
        self.assertIn('date', df.columns)
        self.assertIn('open', df.columns)
        self.assertIn('close', df.columns)
    
    def test_get_stock_history_weekly(self):
        """测试获取周线数据"""
        df = self.client.get_stock_history('sh.000001', '2024-01-01', '2024-01-31', 'w')
        self.assertFalse(df.empty)
    
    def test_get_stock_history_monthly(self):
        """测试获取月线数据"""
        df = self.client.get_stock_history('sh.000001', '2024-01-01', '2024-03-31', 'm')
        self.assertFalse(df.empty)

if __name__ == '__main__':
    unittest.main()