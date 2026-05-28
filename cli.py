import datetime
import socketserver

import click

from config import ETF_DB_PATH, STOCK_DB_PATH
from core import StockDataPipeline
from database import DuckDBManager
from logger_config import setup_logger

logger = setup_logger("CLI")

VALID_FREQUENCIES = {"d", "w", "m"}
VALID_ASSET_TYPES = {"stock", "etf"}


def _validate_frequency(frequency: str) -> str:
    frequency = (frequency or "d").lower()
    if frequency not in VALID_FREQUENCIES:
        raise click.BadParameter("频率只支持 d、w、m")
    return frequency


def _validate_asset_type(asset_type: str) -> str:
    asset_type = (asset_type or "stock").lower()
    if asset_type not in VALID_ASSET_TYPES:
        raise click.BadParameter("资产类型只支持 stock 或 etf")
    return asset_type


def _db_path_for(asset_type: str) -> str:
    return ETF_DB_PATH if asset_type == "etf" else STOCK_DB_PATH


def _run_stock_full(frequency: str) -> None:
    pipeline = StockDataPipeline(asset_type="stock")
    pipeline.full_download_pipeline(frequency=frequency)


def _run_stock_update(frequency: str) -> None:
    pipeline = StockDataPipeline(asset_type="stock")
    pipeline.daily_update_pipeline(frequency=frequency)


def _run_etf_full(frequency: str) -> None:
    pipeline = StockDataPipeline(asset_type="etf")
    pipeline.etf_download_pipeline(frequency=frequency)


def _run_etf_update(frequency: str) -> None:
    pipeline = StockDataPipeline(asset_type="etf")
    pipeline.etf_update_pipeline(frequency=frequency)


@click.group()
def cli():
    """A股量化数据中枢 - 命令行工具"""
    pass


@cli.command(name="full")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def full(frequency):
    """全量下载股票数据"""
    frequency = _validate_frequency(frequency)
    try:
        _run_stock_full(frequency)
    except Exception as e:
        logger.error(f"全量下载失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="download")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def download(frequency):
    """全量下载股票数据（full 的别名）"""
    frequency = _validate_frequency(frequency)
    try:
        _run_stock_full(frequency)
    except Exception as e:
        logger.error(f"全量下载失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="update")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def update(frequency):
    """增量更新股票数据"""
    frequency = _validate_frequency(frequency)
    try:
        _run_stock_update(frequency)
    except Exception as e:
        logger.error(f"增量更新失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="status")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
@click.option("--type", "-t", "asset_type", default="stock", help="资产类型 (stock: 股票, etf: ETF基金)")
def status(frequency, asset_type):
    """查看状态"""
    frequency = _validate_frequency(frequency)
    asset_type = _validate_asset_type(asset_type)
    db = None
    try:
        db = DuckDBManager(db_path=_db_path_for(asset_type))
        table_name = db._get_table_name(frequency, asset_type)
        res = db.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = db.con.execute(f"SELECT COUNT(DISTINCT code) FROM {table_name}").fetchone()
        label = "ETF" if asset_type == "etf" else "股票"
        logger.info(f"📊 {label}数据库: {_db_path_for(asset_type)}")
        logger.info(f"📊 {label}总记录数: {res[0]}")
        logger.info(f"📊 {label}数量: {count[0]}")
    except Exception as e:
        logger.error(f"查询失败: {e}")
        raise click.ClickException(str(e))
    finally:
        if db is not None:
            db.close()


@cli.command(name="etf-full")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def etf_full(frequency):
    """全量下载ETF数据"""
    frequency = _validate_frequency(frequency)
    try:
        _run_etf_full(frequency)
    except Exception as e:
        logger.error(f"ETF全量下载失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="download-etf")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def download_etf(frequency):
    """全量下载ETF数据（etf-full 的别名）"""
    frequency = _validate_frequency(frequency)
    try:
        _run_etf_full(frequency)
    except Exception as e:
        logger.error(f"ETF全量下载失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="etf-update")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def etf_update(frequency):
    """增量更新ETF数据"""
    frequency = _validate_frequency(frequency)
    try:
        _run_etf_update(frequency)
    except Exception as e:
        logger.error(f"ETF增量更新失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="update-etf")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
def update_etf(frequency):
    """增量更新ETF数据（etf-update 的别名）"""
    frequency = _validate_frequency(frequency)
    try:
        _run_etf_update(frequency)
    except Exception as e:
        logger.error(f"ETF增量更新失败: {e}")
        raise click.ClickException(str(e))


@cli.command(name="export")
@click.option("--code", "-c", default="", help="证券代码 (空字符串表示所有证券)")
@click.option("--start-date", "-s", default="1900-01-01", help="开始日期 (YYYY-MM-DD)")
@click.option("--end-date", "-e", default=None, help="结束日期 (YYYY-MM-DD)")
@click.option("--output", "-o", required=True, help="输出文件路径")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
@click.option("--format", "-m", "output_format", default="csv", help="输出格式 (csv, parquet, json)")
@click.option("--type", "-t", "asset_type", default="stock", help="资产类型 (stock: 股票, etf: ETF基金)")
def export(code, start_date, end_date, output, frequency, output_format, asset_type):
    """导出数据"""
    frequency = _validate_frequency(frequency)
    asset_type = _validate_asset_type(asset_type)
    end_date = end_date or datetime.date.today().strftime("%Y-%m-%d")
    db = None
    try:
        db = DuckDBManager(db_path=_db_path_for(asset_type))
        success = db.export_data(code, start_date, end_date, output, frequency, output_format, asset_type=asset_type)
        if success:
            logger.info(f"✅ 数据导出成功: {output}")
        else:
            raise click.ClickException("没有数据可导出或导出失败")
    except click.ClickException:
        raise
    except Exception as e:
        logger.error(f"导出失败: {e}")
        raise click.ClickException(str(e))
    finally:
        if db is not None:
            db.close()


@cli.command(name="delete")
@click.option("--code", "-c", default="", help="证券代码")
@click.option("--start-date", "-s", default="", help="开始日期 (YYYY-MM-DD)")
@click.option("--end-date", "-e", default="", help="结束日期 (YYYY-MM-DD)")
@click.option("--frequency", "-f", default="d", help="数据频率 (d: 日线, w: 周线, m: 月线)")
@click.option("--type", "-t", "asset_type", default="stock", help="资产类型 (stock: 股票, etf: ETF基金)")
@click.option("--yes", "-y", is_flag=True, help="跳过确认")
def delete(code, start_date, end_date, frequency, asset_type, yes):
    """删除指定证券或日期范围的数据"""
    frequency = _validate_frequency(frequency)
    asset_type = _validate_asset_type(asset_type)
    if not code and not start_date and not end_date:
        raise click.ClickException("必须指定 code 或 start/end 至少一个条件")

    db = None
    try:
        db = DuckDBManager(db_path=_db_path_for(asset_type))
        table_name = db._get_table_name(frequency, asset_type)
        conds = []
        params = []
        if code:
            conds.append("code = ?")
            params.append(code)
        if start_date:
            conds.append("date >= ?")
            params.append(start_date)
        if end_date:
            conds.append("date <= ?")
            params.append(end_date)
        where = "WHERE " + " AND ".join(conds)
        count = db.con.execute(f"SELECT COUNT(1) FROM {table_name} {where}", params).fetchone()[0]
        if count == 0:
            logger.info("没有匹配的数据需要删除")
            return
        if not yes:
            click.confirm(f"将从 {table_name} 删除 {count} 条记录，是否继续？", abort=True)
        db.con.execute(f"DELETE FROM {table_name} {where}", params)
        db.con.commit()
        logger.info(f"✅ 已删除 {count} 条记录")
    except click.Abort:
        logger.info("已取消删除")
    except Exception as e:
        logger.error(f"删除失败: {e}")
        raise click.ClickException(str(e))
    finally:
        if db is not None:
            db.close()


@cli.command(name="vacuum")
@click.option("--type", "-t", "asset_type", default="all", help="资产类型 (stock, etf, all)")
def vacuum(asset_type):
    """执行数据库维护"""
    asset_type = (asset_type or "all").lower()
    if asset_type not in {"stock", "etf", "all"}:
        raise click.BadParameter("资产类型只支持 stock、etf 或 all")
    targets = ["stock", "etf"] if asset_type == "all" else [asset_type]
    for target in targets:
        db = None
        try:
            db = DuckDBManager(db_path=_db_path_for(target))
            db.vacuum()
            logger.info(f"✅ {target} 数据库维护完成: {_db_path_for(target)}")
        finally:
            if db is not None:
                db.close()


@cli.command(name="start-viewer")
@click.option("--host", default="127.0.0.1", help="监听地址")
@click.option("--port", default=5678, type=int, help="监听端口")
def start_viewer(host, port):
    """启动 Web 管理界面"""
    try:
        from viewer.server import app
        socketserver.TCPServer.allow_reuse_address = True
        logger.info(f"启动 Web 管理界面: http://{host}:{port}")
        app.run(host=host, port=port, debug=False, threaded=True)
    except Exception as e:
        logger.error(f"启动 Web 管理界面失败: {e}")
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
