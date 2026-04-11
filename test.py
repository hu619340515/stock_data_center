import pandas as pd
import time
from data_source import BaoStockClient
from database import DuckDBManager # 修改：导入 DuckDBManager
from logger_config import setup_logger

logger = setup_logger("TestRunner")

def run_mini_test():
    logger.info("🧪 开始测试：DuckDB 数据流转 (前10只股票)")
    
    # 1. 初始化组件
    client = BaoStockClient()
    db = DuckDBManager() # 修改：实例化 DuckDBManager
    
    # 2. 获取股票列表
    all_stocks = client.get_stock_list()
    if all_stocks.empty:
        logger.error("❌ 获取股票列表失败")
        return

    test_stocks = all_stocks.head(10)
    start_date = "2025-01-01" 
    end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    
    success_count = 0
    
    # 3. 循环处理
    for index, row in test_stocks.iterrows():
        code = row['code']
        name = row['code_name']
        
        try:
            logger.info(f"👉 正在处理: [{code}] {name}...")
            df = client.get_stock_history(code, start_date, end_date)
            
            if df.empty:
                continue
            
            # 4. 写入 DuckDB
            if db.upload_df(df):
                success_count += 1
                logger.info(f"✅ {code} 成功入库 {len(df)} 条")
            
            time.sleep(0.5) 
            
        except Exception as e:
            logger.error(f"❌ {code} 异常: {e}")

    logger.info(f"🏁 测试完成！成功: {success_count} / 10")
    # 可以在这里加一句 SQL 查询验证
    # res = db.con.execute("SELECT count(*) FROM stock_daily").fetchone()
    # logger.info(f"📊 当前数据库总行数: {res[0]}")

if __name__ == "__main__":
    run_mini_test()