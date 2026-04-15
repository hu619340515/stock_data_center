import unittest
import os
import tempfile
from database import DuckDBManager

class TestDatabase(unittest.TestCase):
    """数据库操作测试"""
    
    def setUp(self):
        """设置测试环境"""
        # 创建临时数据库文件
        self.temp_db = tempfile.mktemp(suffix='.db')
        self.db = DuckDBManager(db_path=self.temp_db)
    
    def tearDown(self):
        """清理测试环境"""
        self.db.close()
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)
    
    def test_create_tables(self):
        """测试创建表"""
        # 检查表是否存在
        tables = self.db.con.execute("SHOW TABLES").df()
        self.assertIn('stock_daily', tables['name'].values)
        self.assertIn('stock_weekly', tables['name'].values)
        self.assertIn('stock_monthly', tables['name'].values)
    
    def test_upload_batch(self):
        """测试批量上传数据"""
        import pandas as pd
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.1],
            'high': [10.2, 10.3],
            'low': [9.9, 10.0],
            'close': [10.1, 10.2],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1200000],
            'amount': [10100000.0, 12240000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.12],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 0.99],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 验证数据是否正确插入
        result = self.db.con.execute("SELECT * FROM stock_daily").df()
        self.assertEqual(len(result), 2)
        self.assertEqual(result['code'].iloc[0], 'sh.600000')
    
    def test_get_last_date(self):
        """测试获取最后日期"""
        import pandas as pd
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.1],
            'high': [10.2, 10.3],
            'low': [9.9, 10.0],
            'close': [10.1, 10.2],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1200000],
            'amount': [10100000.0, 12240000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.12],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 0.99],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 获取最后日期
        last_date = self.db.get_last_date('sh.600000')
        self.assertEqual(last_date, '2024-01-02')
    
    def test_get_missing_date_ranges(self):
        """测试获取缺失日期范围"""
        import pandas as pd
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-03'],
            'open': [10.0, 10.2],
            'high': [10.2, 10.4],
            'low': [9.9, 10.1],
            'close': [10.1, 10.3],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1300000],
            'amount': [10100000.0, 13390000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.13],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 1.98],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 获取缺失日期范围
        missing_ranges = self.db.get_missing_date_ranges('sh.600000', '2024-01-01', '2024-01-03')
        self.assertEqual(len(missing_ranges), 1)
        self.assertEqual(missing_ranges[0], ('2024-01-02', '2024-01-02'))
    
    def test_export_data(self):
        """测试导出数据"""
        import pandas as pd
        import tempfile
        # 创建测试数据
        data = {
            'code': ['sh.600000', 'sh.600000'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.1],
            'high': [10.2, 10.3],
            'low': [9.9, 10.0],
            'close': [10.1, 10.2],
            'preclose': [10.0, 10.1],
            'volume': [1000000, 1200000],
            'amount': [10100000.0, 12240000.0],
            'adjustflag': ['1', '1'],
            'turn': [0.1, 0.12],
            'tradestatus': ['1', '1'],
            'pctChg': [1.0, 0.99],
            'isST': ['0', '0']
        }
        df = pd.DataFrame(data)
        
        # 上传数据
        self.db.upload_batch([df])
        
        # 导出数据
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            output_file = f.name
        
        try:
            success = self.db.export_data('sh.600000', '2024-01-01', '2024-01-02', output_file, 'd', 'csv')
            self.assertTrue(success)
            self.assertTrue(os.path.exists(output_file))
            
            # 验证导出的数据
            exported_df = pd.read_csv(output_file)
            self.assertEqual(len(exported_df), 2)
        finally:
            if os.path.exists(output_file):
                os.remove(output_file)

if __name__ == '__main__':
    unittest.main()