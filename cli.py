import click
from core import StockDataPipeline
from logger_config import setup_logger

logger = setup_logger("CLI")

@click.group()
def cli():
    """A股量化数据中枢 - 命令行工具"""
    pass

@cli.command()
def full():
    """全量下载"""
    pipeline = StockDataPipeline() # 在这里实例化
    try:
        pipeline.full_download_pipeline()
    except Exception as e:
        logger.error(f"全量下载失败: {e}")

@cli.command()
def update():
    """增量更新"""
    pipeline = StockDataPipeline()
    try:
        pipeline.daily_update_pipeline()
    except Exception as e:
        logger.error(f"增量更新失败: {e}")

@cli.command()
def status():
    """查看状态"""
    import duckdb
    from config import DATABASE_PATH
    try:
        res = duckdb.connect(DATABASE_PATH).execute("SELECT COUNT(*) FROM stock_daily").fetchone()
        logger.info(f"📊 总记录数: {res[0]}")
    except Exception as e:
        logger.error(f"查询失败: {e}")

# ✅ 必须加上这个保护
if __name__ == "__main__":
    cli()