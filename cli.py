import click
from core import StockDataPipeline
from database import MongoManager

@click.group()
def cli():
    pass

@cli.command()
def full():
    """全量流式下载（不保存CSV，直接进数据库）"""
    pipeline = StockDataPipeline()
    pipeline.full_download_pipeline()

@cli.command()
def update():
    """增量更新"""
    pipeline = StockDataPipeline()
    pipeline.daily_update_pipeline()

@cli.command()
def status():
    """查看状态"""
    mongo = MongoManager()
    count = mongo.collection.count_documents({})
    print(f"📊 总记录数: {count:,}")

if __name__ == '__main__':
    cli()