import pandas as pd
import time
from data_source import BaoStockClient
from database import MongoManager
from logger_config import setup_logger

# 初始化日志
logger = setup_logger("TestRunner")

def run_mini_test():
    logger.info("🧪 开始测试：前10只股票的数据流转 (下载 -> 清洗 -> 入库)")
    
    # 1. 初始化组件
    client = BaoStockClient()
    mongo = MongoManager()
    
    # 2. 获取股票列表
    all_stocks = client.get_stock_list()
    
    if all_stocks.empty:
        logger.error("❌ 获取股票列表失败，测试中止。")
        return

    # 🎯 只取前 10 只股票进行测试
    test_stocks = all_stocks.head(10)
    logger.info(f"📋 已获取列表，截取前 {len(test_stocks)} 只股票进行测试。")

    # 3. 设定测试时间范围 (最近一年，为了测试速度)
    # 如果你想测试全量，可以把 start_date 改成 "1999-01-01"
    start_date = "2025-01-01" 
    end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    
    logger.info(f"⏰ 测试时间范围: {start_date} 至 {end_date}")

    success_count = 0
    
    # 4. 循环处理 (单线程，稳定)
    for index, row in test_stocks.iterrows():
        code = row['code']
        name = row['code_name']
        
        try:
            logger.info(f"👉 正在处理: [{code}] {name}...")
            
            # A. 下载数据
            df = client.get_stock_history(code, start_date, end_date)
            
            if df.empty:
                logger.warning(f"⚠️ {code} 无数据或下载失败")
                continue
            
            # B. 上传到 MongoDB
            # 注意：这里直接调用了 database.py 里的 upload_df
            # 它会先清洗数据，然后写入 stock_daily 集合
            if mongo.upload_df(df):
                success_count += 1
                logger.info(f"✅ {code} 成功入库 {len(df)} 条记录")
            else:
                logger.error(f"❌ {code} 入库失败")
            
            # C. 稍微停顿一下，防止网络请求过快被拦截
            time.sleep(0.5) 
            
        except Exception as e:
            logger.error(f"❌ {code} 发生异常: {e}")

    # 5. 测试总结
    logger.info("-" * 30)
    logger.info(f"🏁 测试完成！")
    logger.info(f"📊 成功: {success_count} / 10")
    logger.info(f"💾 请去 MongoDB 查看集合 'stock_daily' 确认数据")
    logger.info("-" * 30)

if __name__ == "__main__":
    run_mini_test()