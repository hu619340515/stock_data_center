import unittest
import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入所有测试模块
from test_database import TestDatabase
from test_data_source import TestDataSource
from test_config import TestConfigLoader

if __name__ == '__main__':
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加所有测试用例
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDatabase))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDataSource))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConfigLoader))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 根据测试结果设置退出码
    sys.exit(not result.wasSuccessful())