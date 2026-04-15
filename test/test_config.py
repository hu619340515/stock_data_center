import unittest
import os
import tempfile
from config import ConfigLoader

class TestConfigLoader(unittest.TestCase):
    """配置加载器测试"""
    
    def test_default_config(self):
        """测试默认配置"""
        # 测试当配置文件不存在时，是否返回默认配置
        loader = ConfigLoader(config_file="non_existent_config.yaml")
        
        # 测试数据库配置
        self.assertEqual(loader.get("database.path"), "quant_data.db")
        
        # 测试并发配置
        self.assertEqual(loader.get("concurrency.max_workers"), 4)
        self.assertTrue(loader.get("concurrency.dynamic_concurrency"))
        
        # 测试批量配置
        self.assertEqual(loader.get("batch.size"), 50)
        
        # 测试数据源配置
        self.assertEqual(loader.get("datasource.default"), "baostock")
    
    def test_custom_config(self):
        """测试自定义配置"""
        # 创建临时配置文件
        config_content = """
database:
  path: "test.db"

concurrency:
  max_workers: 2
"""
        
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as f:
            f.write(config_content)
            temp_config = f.name
        
        try:
            loader = ConfigLoader(config_file=temp_config)
            self.assertEqual(loader.get("database.path"), "test.db")
            self.assertEqual(loader.get("concurrency.max_workers"), 2)
        finally:
            if os.path.exists(temp_config):
                os.remove(temp_config)
    
    def test_env_var_replacement(self):
        """测试环境变量替换"""
        # 设置环境变量
        os.environ["TEST_DB_PATH"] = "env_test.db"
        
        # 创建临时配置文件
        config_content = """
database:
  path: "${TEST_DB_PATH}"
"""
        
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as f:
            f.write(config_content)
            temp_config = f.name
        
        try:
            loader = ConfigLoader(config_file=temp_config)
            self.assertEqual(loader.get("database.path"), "env_test.db")
        finally:
            if os.path.exists(temp_config):
                os.remove(temp_config)
            del os.environ["TEST_DB_PATH"]
    
    def test_get_nonexistent_key(self):
        """测试获取不存在的键"""
        loader = ConfigLoader()
        self.assertIsNone(loader.get("nonexistent.key"))
        self.assertEqual(loader.get("nonexistent.key", "default"), "default")

if __name__ == '__main__':
    unittest.main()