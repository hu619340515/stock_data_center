import click
from core import StockDataPipeline
from database import DuckDBManager
from logger_config import setup_logger

logger = setup_logger("CLI")

@click.group()
def cli():
    """A股量化数据中枢 - 命令行工具"""
    pass

@cli.command()
@click.option('--frequency', '-f', default='d', help='数据频率 (d: 日线, w: 周线, m: 月线)')
def full(frequency):
    """全量下载"""
    pipeline = StockDataPipeline() # 在这里实例化
    try:
        pipeline.full_download_pipeline(frequency=frequency)
    except Exception as e:
        logger.error(f"全量下载失败: {e}")

@cli.command()
@click.option('--frequency', '-f', default='d', help='数据频率 (d: 日线, w: 周线, m: 月线)')
def update(frequency):
    """增量更新"""
    pipeline = StockDataPipeline()
    try:
        pipeline.daily_update_pipeline(frequency=frequency)
    except Exception as e:
        logger.error(f"增量更新失败: {e}")

@cli.command()
@click.option('--frequency', '-f', default='d', help='数据频率 (d: 日线, w: 周线, m: 月线)')
def status(frequency):
    """查看状态"""
    import duckdb
    from config import DATABASE_PATH
    try:
        db = DuckDBManager()
        table_name = db._get_table_name(frequency)
        res = duckdb.connect(DATABASE_PATH).execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        stock_count = duckdb.connect(DATABASE_PATH).execute(f"SELECT COUNT(DISTINCT code) FROM {table_name}").fetchone()
        logger.info(f"📊 总记录数: {res[0]}")
        logger.info(f"📊 股票数量: {stock_count[0]}")
    except Exception as e:
        logger.error(f"查询失败: {e}")

@cli.command()
@click.option('--code', '-c', default='', help='股票代码 (空字符串表示所有股票)')
@click.option('--start-date', '-s', required=True, help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='结束日期 (YYYY-MM-DD)')
@click.option('--output', '-o', required=True, help='输出文件路径')
@click.option('--frequency', '-f', default='d', help='数据频率 (d: 日线, w: 周线, m: 月线)')
@click.option('--format', '-t', default='csv', help='输出格式 (csv, parquet, json)')
def export(code, start_date, end_date, output, frequency, format):
    """导出数据"""
    try:
        db = DuckDBManager()
        success = db.export_data(code, start_date, end_date, output, frequency, format)
        if success:
            logger.info(f"✅ 数据导出成功: {output}")
        else:
            logger.error("❌ 数据导出失败")
    except Exception as e:
        logger.error(f"导出失败: {e}")

# ✅ 必须加上这个保护
if __name__ == "__main__":
    cli()