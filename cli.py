import click
from core import StockDataPipeline
from database import DuckDBManager  # ✅ 修改：导入 DuckDBManager
from logger_config import setup_logger

logger = setup_logger("CLI")

# 初始化流水线
pipeline = StockDataPipeline()

@click.group()
def cli():
    """A股量化数据中枢 - 命令行工具"""
    pass

@cli.command()
def full():
    """全量下载：从1999年至今的所有数据"""
    try:
        pipeline.full_download_pipeline()
    except Exception as e:
        logger.error(f"全量下载失败: {e}")
    finally:
        pipeline.stock_client.logout()

@cli.command()
def update():
    """增量更新：仅更新最新日期的数据"""
    try:
        pipeline.daily_update_pipeline()
    except Exception as e:
        logger.error(f"增量更新失败: {e}")
    finally:
        pipeline.stock_client.logout()

@cli.command()
def status():
    """查看数据库状态"""
    try:
        logger.info("🔍 正在查询数据库状态...")
        # ✅ 修改：使用 DuckDB 的 SQL 语法
        # MongoDB: db.collection.countDocuments()
        # DuckDB: SELECT COUNT(*) FROM table
        res = pipeline.db.con.execute("SELECT COUNT(*) FROM stock_daily").fetchone()
        total_rows = res[0]
        
        # 查询有多少只股票
        stock_count = pipeline.db.con.execute("SELECT COUNT(DISTINCT code) FROM stock_daily").fetchone()[0]
        
        logger.info("-" * 30)
        logger.info(f"📊 数据库状态")
        logger.info(f"📈 股票数量: {stock_count}")
        logger.info(f"📝 总记录数: {total_rows}")
        logger.info("-" * 30)
        
    except Exception as e:
        logger.error(f"查询状态失败: {e}")

if __name__ == "__main__":
    cli()