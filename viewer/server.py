"""
量化数据可视化管理 - Flask API Server
主要接口：
  GET  /                        → 前端首页
  GET  /api/dashboard           → 首页聚合指标、健康度和质量提醒
  GET  /api/data_quality        → 数据质量诊断
  GET  /api/task_history        → 当前任务与最近任务历史
  GET  /api/status              → 数据库连接状态
  GET  /api/overview            → 概览卡片 / 所有表行数
  GET  /api/table               → 分页表格查询（支持关键词+日期筛选）
  GET  /api/codes               → 获取指定表的 code 列表
  GET  /api/security_search     → 按代码或名称联想证券
  GET  /api/kline               → 单只证券 K 线数据
  GET  /api/stats               → 数值列统计（min/max/avg/sum）
  GET  /api/top_movers          → 涨跌幅排行榜
  GET  /api/distribution        → 成交量/价格区间分布
  GET  /api/trend               → 指数/均线走势聚合
  GET  /api/refresh_status      → 各表最新更新时间统计
  GET  /api/export              → 导出 CSV（流式下载）
  POST /api/delete              → 删除指定 code 或日期范围记录
  POST /api/delete_preview      → 删除前预览影响条数
  GET  /api/schema              → 查询表字段结构
  GET  /api/summary_by_code     → 按 code 聚合统计（均价/最大最小/总量）
  GET  /api/progress            → 获取更新进度
"""
import os
import io
import csv
import json
import datetime
import traceback
import threading
import socketserver
import sys
import time
from collections import deque
import duckdb
from flask import Flask, request, jsonify, send_from_directory, make_response, Response
from flask_cors import CORS

# ── 延迟加载 core 模块（避免启动时就导入 pandas/multiprocessing/config 等整条链）──
_CORE_MODULE = None
_MOCK_PIPELINE_CLASS = None
_MOCK_GET_PROGRESS = None
_MOCK_RESET_PROGRESS = None
_MOCK_SET_PROGRESS = None

def _get_core():
    """延迟加载 core 模块，失败时返回备用 mock"""
    global _CORE_MODULE, _MOCK_PIPELINE_CLASS, _MOCK_GET_PROGRESS, _MOCK_RESET_PROGRESS, _MOCK_SET_PROGRESS
    if _CORE_MODULE is not None or _MOCK_PIPELINE_CLASS is not None:
        return _CORE_MODULE
    
    try:
        import sys as _sys
        _PROJECT_ROOT_FOR_IMPORT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if _PROJECT_ROOT_FOR_IMPORT not in _sys.path:
            _sys.path.append(_PROJECT_ROOT_FOR_IMPORT)
        from core import StockDataPipeline, get_progress, reset_progress, set_progress
        _CORE_MODULE = (StockDataPipeline, get_progress, reset_progress, set_progress)
    except ImportError as e:
        print(f"WARNING: 'core.StockDataPipeline' not found: {e}. Using a mock class for server startup.")
        # 定义 mock 类作为备用
        class _MockPipeline:
            def __init__(self, db_path=None, use_temp_db=False, asset_type='stock'):
                self.should_stop = False
                print(f"[MOCK] Initialized StockDataPipeline with use_temp_db={use_temp_db}, asset_type={asset_type}")
            def stop(self):
                self.should_stop = True
            def cleanup_temp_db(self):
                pass
            def merge_to_main_db(self, target_db_path, tables):
                pass
            def daily_update_pipeline(self, data_type, mode=None):
                print(f"[MOCK] Running daily_update_pipeline for data_type={data_type}, mode={mode}")
                import time
                time.sleep(2)
                if 'fail' in str(data_type):
                    return False, f"Mock failure for {data_type}"
                return True, f"Mock success for {data_type} (mode: {mode})"
            def full_download_pipeline(self, data_type):
                print(f"[MOCK] Running full_download_pipeline for data_type={data_type}")
                import time
                time.sleep(2)
                return True, f"Mock success"
            def etf_download_pipeline(self, frequency):
                print(f"[MOCK] Running etf_download_pipeline for frequency={frequency}")
                import time
                time.sleep(2)
                return True, f"Mock success"
            def etf_update_pipeline(self, frequency):
                print(f"[MOCK] Running etf_update_pipeline for frequency={frequency}")
                import time
                time.sleep(2)
                return True, f"Mock success"
        _MOCK_PIPELINE_CLASS = _MockPipeline
        _MOCK_GET_PROGRESS = lambda: {'is_running': False}
        _MOCK_RESET_PROGRESS = lambda: None
        _MOCK_SET_PROGRESS = lambda **kwargs: None
        _CORE_MODULE = (_MOCK_PIPELINE_CLASS, _MOCK_GET_PROGRESS, _MOCK_RESET_PROGRESS, _MOCK_SET_PROGRESS)
    
    return _CORE_MODULE

# 后台任务状态
_task_lock = threading.Lock()
_task_running = False
_current_pipeline = None  # 保存当前正在运行的 pipeline 实例
_current_task = None
_task_history = deque(maxlen=20)
_dashboard_cache = {'key': None, 'expires_at': 0, 'data': None}

# ─────────────────────────────────────────────
# 路径配置
# ─────────────────────────────────────────────
_VIEWER_DIR   = os.path.abspath(os.path.dirname(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_VIEWER_DIR, '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.append(_PROJECT_ROOT)
from config import STOCK_DB_PATH as CONFIG_STOCK_DB_PATH, ETF_DB_PATH as CONFIG_ETF_DB_PATH, LOG_DIR

# 数据库路径配置
STOCK_DB_PATH = os.environ.get('STOCK_DB_PATH', CONFIG_STOCK_DB_PATH)
ETF_DB_PATH = os.environ.get('ETF_DB_PATH', CONFIG_ETF_DB_PATH)
DEFAULT_DB_PATH = STOCK_DB_PATH  # 默认使用股票数据库

# 优先读环境变量
DB_PATH = os.environ.get('DB_PATH', DEFAULT_DB_PATH)
APP_LOG_PATH = os.path.join(LOG_DIR, 'app.log')
ERROR_LOG_PATH = os.path.join(_PROJECT_ROOT, 'error_log.txt')

app = Flask(__name__, static_folder=_VIEWER_DIR)
CORS(app)


def _merge_pipeline_if_needed(pipeline, use_temp_db, asset_type):
    """Merge one temporary pipeline into the matching main database."""
    target_db_path = STOCK_DB_PATH if asset_type == 'stock' else ETF_DB_PATH
    print(f"🔍 检查合并条件 - use_temp_db={use_temp_db}, should_stop={pipeline.should_stop}")
    if use_temp_db and not pipeline.should_stop:
        print(f"🔄 开始合并到主数据库: {target_db_path}")
        tables = ['stock_daily', 'stock_weekly', 'stock_monthly']
        if asset_type == 'etf':
            tables = ['etf_daily', 'etf_weekly', 'etf_monthly']
        pipeline.merge_to_main_db(target_db_path, tables)
    else:
        print(f"⏭️ 跳过合并 - use_temp_db={use_temp_db}, should_stop={pipeline.should_stop}")


def _cleanup_pipeline(pipeline):
    if not pipeline:
        return
    pipeline.cleanup_temp_db()
    if not getattr(pipeline, 'temp_db_path', None):
        db = getattr(pipeline, 'db', None)
        if db:
            db.close()


def _finish_task_history(status, message=''):
    """Finalize the active task summary for the viewer task drawer."""
    global _current_task, _dashboard_cache
    if not _current_task:
        return
    _current_task['status'] = status
    _current_task['message'] = message
    _current_task['finished_at'] = datetime.datetime.now().isoformat(timespec='seconds')
    started_at = _current_task.get('started_timestamp')
    if started_at:
        _current_task['duration_seconds'] = round(time.time() - started_at, 1)
    _current_task.pop('started_timestamp', None)
    _task_history.appendleft(dict(_current_task))
    _current_task = None
    _dashboard_cache['expires_at'] = 0


def _run_task_sequence(steps, task_name='后台数据任务'):
    """Run one or more pipeline tasks serially in a background thread."""
    global _task_running, _current_pipeline, _current_task
    with _task_lock:
        if _task_running:
            return False, '已有任务正在运行，请等待完成'
        _task_running = True
        started_at = datetime.datetime.now()
        _current_task = {
            'name': task_name,
            'status': 'running',
            'started_at': started_at.isoformat(timespec='seconds'),
            'started_timestamp': started_at.timestamp(),
            'message': '任务正在运行',
        }
        _dashboard_cache['expires_at'] = 0
        reset_progress = _get_core()[2]
        reset_progress()

    def worker():
        global _task_running, _current_pipeline
        was_stopped = False
        try:
            StockDataPipeline = _get_core()[0]
            for step in steps:
                pipeline = StockDataPipeline(
                    use_temp_db=step.get('use_temp_db', False),
                    asset_type=step.get('asset_type', 'stock'),
                )
                _current_pipeline = pipeline
                step['func'](pipeline)
                _merge_pipeline_if_needed(pipeline, step.get('use_temp_db', False), step.get('asset_type', 'stock'))
                should_stop = pipeline.should_stop
                was_stopped = was_stopped or should_stop
                _cleanup_pipeline(pipeline)
                _current_pipeline = None
                if should_stop:
                    break
        except Exception as e:
            _finish_task_history('failed', str(e))
            print(f"❌ 后台任务出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print(f"🔍 finally 块执行 - _current_pipeline={_current_pipeline}")
            if _current_pipeline:
                print(f"🧹 清理数据库连接")
                _cleanup_pipeline(_current_pipeline)
                _current_pipeline = None
            with _task_lock:
                _task_running = False
            if _current_task:
                if was_stopped:
                    _finish_task_history('stopped', '任务已停止，临时数据未合并')
                else:
                    _finish_task_history('completed', '任务执行完成')
            print(f"✅ 任务完成，_task_running={_task_running}")
    
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return True, '任务已启动，请在进度条中查看'


def _run_task(func, *args, use_temp_db=False, asset_type='stock', task_name='后台数据任务'):
    """Run one pipeline task in a background thread."""
    return _run_task_sequence([{
        'func': lambda pipeline: func(pipeline, *args),
        'use_temp_db': use_temp_db,
        'asset_type': asset_type,
    }], task_name=task_name)


def _run_frequency_for_all_assets(frequency):
    """Download stock data first, then ETF data, for one frequency."""
    return _run_task_sequence([
        {
            'func': lambda pipeline: pipeline.full_download_pipeline(frequency),
            'use_temp_db': True,
            'asset_type': 'stock',
        },
        {
            'func': lambda pipeline: pipeline.etf_download_pipeline(frequency),
            'use_temp_db': True,
            'asset_type': 'etf',
        },
    ], task_name=f'股票与ETF {frequency.upper()}周期数据计算')


def _run_rps_task(asset_type='stock'):
    """Calculate stock or ETF RPS in the background without a market-data source."""
    global _task_running, _current_task
    asset_type = (asset_type or 'stock').lower()
    if asset_type not in {'stock', 'etf'}:
        return False, '不支持的 RPS 资产类型'
    asset_label = '股票' if asset_type == 'stock' else 'ETF'
    db_path = STOCK_DB_PATH if asset_type == 'stock' else ETF_DB_PATH
    with _task_lock:
        if _task_running:
            return False, '已有任务正在运行，请等待完成'
        _task_running = True
        started_at = datetime.datetime.now()
        _current_task = {
            'name': f'计算{asset_label}RPS日频因子',
            'status': 'running',
            'started_at': started_at.isoformat(timespec='seconds'),
            'started_timestamp': started_at.timestamp(),
            'message': '任务正在运行',
        }
        _dashboard_cache['expires_at'] = 0
        reset_progress = _get_core()[2]
        reset_progress()

    def worker():
        global _task_running
        db = None
        set_progress = _get_core()[3]
        try:
            from database import DuckDBManager
            set_progress(is_running=True, task_name=f'计算{asset_label}RPS日频因子', start_time=datetime.datetime.now().timestamp())
            db = DuckDBManager(db_path=db_path, asset_type=asset_type)
            count = db.calculate_rps_daily()
            set_progress(is_running=False, processed=count, total=count, success=count, message=f'{asset_label} RPS计算完成，共{count}条')
            _finish_task_history('completed', f'{asset_label} RPS计算完成，共{count}条')
        except Exception as e:
            set_progress(is_running=False, error=1, message=f'{asset_label} RPS计算失败: {e}')
            _finish_task_history('failed', f'{asset_label} RPS计算失败: {e}')
            print(f"❌ {asset_label} RPS计算失败: {e}")
            traceback.print_exc()
        finally:
            if db:
                db.close()
            with _task_lock:
                _task_running = False

    threading.Thread(target=worker, daemon=True).start()
    return True, f'{asset_label} RPS计算任务已启动，请在进度条中查看'

def _run_calendar_task():
    """Rebuild stock and ETF trade calendars from locally stored daily bars."""
    global _task_running, _current_task
    with _task_lock:
        if _task_running:
            return False, '已有任务正在运行，请等待完成'
        _task_running = True
        started_at = datetime.datetime.now()
        _current_task = {
            'name': '重建交易日历',
            'status': 'running',
            'started_at': started_at.isoformat(timespec='seconds'),
            'started_timestamp': started_at.timestamp(),
            'message': '正在根据本地日线重建交易日历',
        }
        _dashboard_cache['expires_at'] = 0
        reset_progress = _get_core()[2]
        reset_progress()

    def worker():
        global _task_running
        set_progress = _get_core()[3]
        results = []
        try:
            from database import DuckDBManager
            set_progress(is_running=True, task_name='重建交易日历', total=2, start_time=datetime.datetime.now().timestamp())
            for index, (asset_type, db_path) in enumerate([('stock', STOCK_DB_PATH), ('etf', ETF_DB_PATH)], start=1):
                db = DuckDBManager(db_path=db_path, asset_type=asset_type)
                try:
                    result = db.rebuild_trade_calendar_from_daily()
                    results.append(result)
                finally:
                    db.close()
                set_progress(processed=index, success=index, message=f"已完成 {index}/2 个资产库")
            detail = '；'.join(
                f"{item['asset_type']} {item['open_days']} 个交易日，{item['closed_days']} 个闭市日"
                for item in results
            )
            set_progress(is_running=False, processed=2, total=2, success=2, message=f'交易日历重建完成：{detail}')
            _finish_task_history('completed', f'交易日历重建完成：{detail}')
        except Exception as e:
            set_progress(is_running=False, error=1, message=f'交易日历重建失败: {e}')
            _finish_task_history('failed', f'交易日历重建失败: {e}')
            print(f"❌ 交易日历重建失败: {e}")
            traceback.print_exc()
        finally:
            with _task_lock:
                _task_running = False

    threading.Thread(target=worker, daemon=True).start()
    return True, '交易日历重建任务已启动，请在进度条中查看'

@app.route('/api/stop_task', methods=['POST'])
def api_stop_task():
    """停止当前正在运行的任务"""
    global _current_pipeline
    if _current_pipeline:
        _current_pipeline.stop()
        return jsonify({'success': True, 'message': '已发送停止信号'})
    return jsonify({'success': False, 'message': '没有正在运行的任务'})

@app.route('/api/daily_download', methods=['POST'])
def api_daily_download():
    target = request.json.get('target', 'stock')
    if target == 'etf':
        ok, msg = _run_task(lambda p: p.etf_download_pipeline('d'), use_temp_db=True, asset_type='etf', task_name='ETF日线全量下载')
    else:
        ok, msg = _run_task(lambda p: p.full_download_pipeline('d'), use_temp_db=True, asset_type='stock', task_name='股票日线全量下载')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/daily_to_latest', methods=['POST'])
def api_daily_to_latest():
    target = request.json.get('target', 'stock')
    if target == 'etf':
        ok, msg = _run_task(lambda p: p.etf_update_pipeline('d'), use_temp_db=False, asset_type='etf', task_name='ETF日线增量更新')
    else:
        ok, msg = _run_task(lambda p: p.daily_update_pipeline('d'), use_temp_db=False, asset_type='stock', task_name='股票日线增量更新')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/download_stock_all_cycles', methods=['POST'])
def api_download_stock_all_cycles():
    """下载股票的日/周/月全周期数据，自动判断全量/增量"""
    ok, msg = _run_task(lambda p: p.download_all_cycles(asset_type='stock'), use_temp_db=False, asset_type='stock', task_name='股票智能全周期更新')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/download_etf_all_cycles', methods=['POST'])
def api_download_etf_all_cycles():
    """下载ETF的日/周/月全周期数据，自动判断全量/增量"""
    ok, msg = _run_task(lambda p: p.download_all_cycles(asset_type='etf'), use_temp_db=False, asset_type='etf', task_name='ETF智能全周期更新')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/aggregate_weekly', methods=['POST'])
def api_aggregate_weekly():
    ok, msg = _run_frequency_for_all_assets('w')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/aggregate_monthly', methods=['POST'])
def api_aggregate_monthly():
    ok, msg = _run_frequency_for_all_assets('m')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/calculate_rps', methods=['POST'])
def api_calculate_rps():
    body = request.get_json(force=True, silent=True) or {}
    ok, msg = _run_rps_task(body.get('target', 'stock'))
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/rebuild_trade_calendar', methods=['POST'])
def api_rebuild_trade_calendar():
    ok, msg = _run_calendar_task()
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/etf_download', methods=['POST'])
def api_etf_download():
    ok, msg = _run_task(lambda p: p.etf_download_pipeline('d'), use_temp_db=True, asset_type='etf', task_name='ETF日线全量下载')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/etf_update_latest', methods=['POST'])
def api_etf_update_latest():
    ok, msg = _run_task(lambda p: p.etf_update_pipeline('d'), use_temp_db=True, asset_type='etf', task_name='ETF日线增量更新')
    return jsonify({'success': ok, 'message': msg})


# ─────────────────────────────────────────────
# 白名单
# ─────────────────────────────────────────────
ALLOWED_TABLES = {
    'stock_daily', 'stock_weekly', 'stock_monthly',
    'etf_daily',   'etf_weekly',   'etf_monthly',
    'stock_info', 'etf_info',
    'trade_calendar',
    'factor_rps_daily', 'factor_update_log',
    'etf_factor_rps_daily', 'etf_factor_update_log'
}

MARKET_TABLES = {
    'stock_daily', 'stock_weekly', 'stock_monthly',
    'etf_daily', 'etf_weekly', 'etf_monthly'
}
FACTOR_RPS_TABLES = {'factor_rps_daily', 'etf_factor_rps_daily'}

# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────
def get_conn(read_only=True, asset_type='stock'):
    # 根据资产类型选择数据库
    if asset_type == 'etf':
        db_path = ETF_DB_PATH
    else:
        db_path = STOCK_DB_PATH
    
    # 重试机制，最多重试3次
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            # 使用 read_only 模式避免与后台写入任务冲突
            conn = duckdb.connect(db_path, read_only=read_only)
            return conn
        except TypeError:
            return duckdb.connect(db_path)
        except duckdb.ConnectionException:
            return duckdb.connect(db_path)
        except Exception as e:
            # 如果数据库被锁定，尝试只读连接
            if read_only and attempt < max_retries -1:
                try:
                    conn = duckdb.connect(db_path, read_only=True)
                    return conn
                except Exception:
                    # 等待后重试
                    import time
                    time.sleep(retry_delay)
            elif attempt >= max_retries -1:
                raise
            else:
                # 等待后重试
                import time
                time.sleep(retry_delay)

def df_to_records(df):
    # Flask JSON encoder may emit non-standard NaN tokens; normalize to None first.
    # Cast to object first, otherwise float columns coerce None back to NaN.
    df = df.astype(object).where(df.notna(), None)
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if 'datetime' in dtype_str or 'date' in dtype_str:
            df[col] = df[col].astype(str)
    return df.to_dict(orient='records')

def read_last_lines(path, max_lines=200):
    max_lines = max(1, min(int(max_lines), 2000))
    if not os.path.exists(path):
        return []
    from collections import deque
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        return list(deque((line.rstrip('\n') for line in f), maxlen=max_lines))

def check_table(table):
    if table not in ALLOWED_TABLES:
        return None, (jsonify({'status': 'error', 'msg': f'未知表名: {table}'}), 400)
    return table, None

def asset_type_for_table(table):
    return 'etf' if table.startswith('etf_') else 'stock'

def table_columns(conn, table):
    return [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]

def info_table_for_market(table):
    return 'etf_info' if table.startswith('etf_') else 'stock_info'

def market_display_sql(conn, table):
    columns = table_columns(conn, table)
    select_parts = []
    if 'code' in columns:
        select_parts.append('d.code')
    select_parts.append("COALESCE(i.name, '') AS name")
    if 'date' in columns:
        select_parts.append('d.date')
    select_parts.extend(f'd.{col}' for col in columns if col not in {'code', 'name', 'date'})
    info_table = info_table_for_market(table)
    return ', '.join(select_parts), f"{table} d LEFT JOIN {info_table} i ON d.code = i.code"

def build_where(keyword='', start='', end='', code='', columns=None):
    """返回 (where_clause: str, params: list)，使用参数化占位符 ? 防止 SQL 注入。"""
    columns = set(columns or ['code', 'date'])
    conds  = []
    params = []
    if keyword and 'code' in columns:
        conds.append("code LIKE ?")
        params.append(f'%{keyword}%')
    if code and 'code' in columns:
        conds.append("code = ?")
        params.append(code)
    if start and 'date' in columns:
        conds.append("date >= ?")
        params.append(start)
    if end and 'date' in columns:
        conds.append("date <= ?")
        params.append(end)
    clause = ('WHERE ' + ' AND '.join(conds)) if conds else ''
    return clause, params

def build_market_where(keyword='', start='', end='', code=''):
    conds = []
    params = []
    if keyword:
        conds.append("(d.code LIKE ? OR i.name LIKE ?)")
        params.extend([f'%{keyword}%', f'%{keyword}%'])
    if code:
        conds.append("d.code = ?")
        params.append(code)
    if start:
        conds.append("d.date >= ?")
        params.append(start)
    if end:
        conds.append("d.date <= ?")
        params.append(end)
    clause = ('WHERE ' + ' AND '.join(conds)) if conds else ''
    return clause, params


TABLE_LABELS = {
    'stock_daily': '股票日线',
    'stock_weekly': '股票周线',
    'stock_monthly': '股票月线',
    'stock_info': '股票基础信息',
    'etf_daily': 'ETF日线',
    'etf_weekly': 'ETF周线',
    'etf_monthly': 'ETF月线',
    'etf_info': 'ETF基础信息',
    'trade_calendar': '交易日历',
    'factor_rps_daily': 'RPS日频因子',
    'factor_update_log': '因子更新日志',
    'etf_factor_rps_daily': 'ETF RPS日频因子',
    'etf_factor_update_log': 'ETF因子更新日志',
}


def _table_status(conn, table):
    """Return lightweight table metrics for the dashboard."""
    columns = set(table_columns(conn, table))
    count = conn.execute(f"SELECT COUNT(1) FROM {table}").fetchone()[0]
    min_date = max_date = None
    if 'date' in columns:
        row = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
        min_date = str(row[0]) if row and row[0] else None
        max_date = str(row[1]) if row and row[1] else None
    return {
        'table': table,
        'label': TABLE_LABELS.get(table, table),
        'count': int(count),
        'min_date': min_date,
        'max_date': max_date,
    }


def _dashboard_data(force_refresh=False):
    """Collect dashboard metrics once and keep the heavy reads briefly cached."""
    cache_key = (STOCK_DB_PATH, ETF_DB_PATH)
    if (
        not force_refresh
        and _dashboard_cache.get('key') == cache_key
        and _dashboard_cache.get('data') is not None
        and _dashboard_cache.get('expires_at', 0) > time.time()
    ):
        return _dashboard_cache['data']

    databases = {}
    tables = []
    table_map = {}
    market_checks = {}
    for asset_type, path in [('stock', STOCK_DB_PATH), ('etf', ETF_DB_PATH)]:
        database = {
            'asset_type': asset_type,
            'name': os.path.basename(path),
            'path': path,
            'status': 'error',
            'tables': [],
        }
        try:
            conn = get_conn(asset_type=asset_type)
            names = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
            database['status'] = 'ok'
            database['tables'] = names
            for table in names:
                if table not in ALLOWED_TABLES:
                    continue
                if asset_type == 'stock' and table.startswith('etf_'):
                    continue
                if asset_type == 'etf' and not (table.startswith('etf_') or table == 'trade_calendar'):
                    continue
                # trade_calendar exists in both databases; keep the stock copy in the main list.
                if table == 'trade_calendar' and asset_type == 'etf':
                    continue
                status = _table_status(conn, table)
                status['asset_type'] = asset_type
                tables.append(status)
                table_map[table] = status
            daily_table = f'{asset_type}_daily'
            if daily_table in names:
                columns = set(table_columns(conn, daily_table))
                if 'pctChg' in columns:
                    latest = conn.execute(f"SELECT MAX(date) FROM {daily_table}").fetchone()[0]
                    if latest:
                        row = conn.execute(
                            f"SELECT COUNT(1), "
                            f"SUM(CASE WHEN pctChg > 0 THEN 1 ELSE 0 END), "
                            f"SUM(CASE WHEN pctChg < 0 THEN 1 ELSE 0 END), "
                            f"SUM(CASE WHEN pctChg = 0 THEN 1 ELSE 0 END), "
                            f"AVG(pctChg), "
                            f"SUM(CASE WHEN turn = 0 THEN 1 ELSE 0 END), "
                            f"COUNT(turn) FROM {daily_table} WHERE date = ?",
                            [latest],
                        ).fetchone()
                        market_checks[asset_type] = {
                            'date': str(latest),
                            'total': int(row[0] or 0),
                            'rise_count': int(row[1] or 0),
                            'fall_count': int(row[2] or 0),
                            'flat_count': int(row[3] or 0),
                            'avg_pct_chg': round(float(row[4] or 0), 4),
                            'turn_zero_count': int(row[5] or 0),
                            'turn_count': int(row[6] or 0),
                        }
                        if asset_type == 'stock':
                            st_row = conn.execute(
                                "SELECT "
                                "SUM(CASE WHEN d.isST = '1' THEN 1 ELSE 0 END), "
                                "SUM(CASE WHEN "
                                "  UPPER(COALESCE(i.name, '')) LIKE 'ST%' "
                                "  OR UPPER(COALESCE(i.name, '')) LIKE '*ST%' "
                                "  OR UPPER(COALESCE(i.name, '')) LIKE 'SST%' "
                                "  OR UPPER(COALESCE(i.name, '')) LIKE 'S*ST%' "
                                "THEN 1 ELSE 0 END), "
                                "SUM(CASE WHEN "
                                "  (d.isST = '1') <> ("
                                "    UPPER(COALESCE(i.name, '')) LIKE 'ST%' "
                                "    OR UPPER(COALESCE(i.name, '')) LIKE '*ST%' "
                                "    OR UPPER(COALESCE(i.name, '')) LIKE 'SST%' "
                                "    OR UPPER(COALESCE(i.name, '')) LIKE 'S*ST%'"
                                "  ) "
                                "THEN 1 ELSE 0 END) "
                                "FROM stock_daily d "
                                "LEFT JOIN stock_info i ON d.code = i.code "
                                "WHERE d.date = ?",
                                [latest],
                            ).fetchone()
                            market_checks[asset_type].update({
                                'st_flag_count': int(st_row[0] or 0),
                                'st_name_count': int(st_row[1] or 0),
                                'st_mismatch_count': int(st_row[2] or 0),
                            })
            conn.close()
        except Exception as e:
            database['error'] = str(e)
        databases[asset_type] = database

    issues = []
    for asset_type, database in databases.items():
        if database['status'] != 'ok':
            issues.append({
                'level': 'error',
                'code': f'{asset_type}_database_unavailable',
                'title': f"{'股票' if asset_type == 'stock' else 'ETF'}数据库无法连接",
                'detail': database.get('error', '请检查数据库文件和后台任务状态。'),
            })
    for table, title in [
        ('trade_calendar', '交易日历'),
        ('factor_rps_daily', '股票 RPS日频因子'),
        ('etf_factor_rps_daily', 'ETF RPS日频因子'),
    ]:
        status = table_map.get(table)
        if not status or status['count'] == 0:
            issues.append({
                'level': 'warning' if table == 'trade_calendar' else 'info',
                'code': f'{table}_empty',
                'title': f'{title}当前为空',
                'detail': '建议补齐交易日基准。' if table == 'trade_calendar' else '可在因子中心启动首次 RPS 计算。',
            })
    stock_market = market_checks.get('stock')
    if stock_market and stock_market['total'] > 0 and stock_market['rise_count'] == 0 and stock_market['fall_count'] == 0:
        issues.append({
            'level': 'warning',
            'code': 'stock_pct_chg_all_zero',
            'title': f"股票日线涨跌幅在 {stock_market['date']} 全部为 0",
            'detail': '涨跌榜和市场宽度可能失真，建议检查数据源字段映射或重新计算。',
        })
    if stock_market and stock_market['turn_count'] > 0 and stock_market['turn_zero_count'] == stock_market['turn_count']:
        issues.append({
            'level': 'warning',
            'code': 'stock_turn_all_zero',
            'title': f"股票日线换手率在 {stock_market['date']} 全部为 0",
            'detail': '标准 QMT K 线不直接返回换手率。采集器会使用当前流通股本派生新数据，历史数据仍需专项回填。',
        })
    if stock_market and stock_market.get('st_mismatch_count', 0) > 0:
        issues.append({
            'level': 'warning',
            'code': 'stock_st_mismatch',
            'title': f"股票日线 ST 标记在 {stock_market['date']} 有 {stock_market['st_mismatch_count']} 条不一致",
            'detail': '已按股票基础信息中的 ST / *ST 名称补齐新采集数据；历史状态需要专项回填。',
        })

    score = max(0, 100 - sum(20 if item['level'] == 'error' else 8 if item['level'] == 'warning' else 3 for item in issues))
    payload = {
        'generated_at': datetime.datetime.now().isoformat(timespec='seconds'),
        'health_score': score,
        'databases': databases,
        'tables': sorted(tables, key=lambda item: item['label']),
        'summary': {
            'stock_daily_count': table_map.get('stock_daily', {}).get('count', 0),
            'etf_daily_count': table_map.get('etf_daily', {}).get('count', 0),
            'stock_count': table_map.get('stock_info', {}).get('count', 0),
            'etf_count': table_map.get('etf_info', {}).get('count', 0),
            'latest_trade_date': table_map.get('stock_daily', {}).get('max_date'),
        },
        'market_checks': market_checks,
        'issues': issues,
        'task': dict(_current_task) if _current_task else None,
    }
    if payload['task']:
        payload['task'].pop('started_timestamp', None)
    _dashboard_cache.update({'key': cache_key, 'expires_at': time.time() + 15, 'data': payload})
    return payload


# ═══════════════════════════════════════════════════════════
# 静态页面路由
# ═══════════════════════════════════════════════════════════
@app.route('/')
def root_index():
    resp = make_response(send_from_directory(app.static_folder, 'index.html'))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@app.route('/favicon.ico')
def favicon():
    return make_response('', 204)


# ═══════════════════════════════════════════════════════════
# Dashboard aggregation and quality checks
# ═══════════════════════════════════════════════════════════
@app.route('/api/dashboard')
def api_dashboard():
    conn = None
    conn = None
    try:
        force_refresh = request.args.get('refresh', '0') == '1'
        return jsonify(_dashboard_data(force_refresh=force_refresh))
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/api/data_quality')
def api_data_quality():
    try:
        dashboard = _dashboard_data(force_refresh=request.args.get('refresh', '0') == '1')
        return jsonify({
            'generated_at': dashboard['generated_at'],
            'health_score': dashboard['health_score'],
            'issues': dashboard['issues'],
            'tables': dashboard['tables'],
        })
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/api/task_history')
def api_task_history():
    limit = max(1, min(int(request.args.get('limit', 10)), 20))
    current = dict(_current_task) if _current_task else None
    if current:
        current.pop('started_timestamp', None)
    return jsonify({'current': current, 'history': list(_task_history)[:limit]})


# ═══════════════════════════════════════════════════════════
# 1. /api/status
# ═══════════════════════════════════════════════════════════
@app.route('/api/status')
def api_status():
    try:
        # 同时检查两个数据库的状态
        stock_status = {'name': 'stock_data.db', 'path': STOCK_DB_PATH, 'status': 'error', 'tables': []}
        etf_status = {'name': 'etf_data.db', 'path': ETF_DB_PATH, 'status': 'error', 'tables': []}
        
        # 检查股票数据库
        try:
            conn = get_conn(asset_type='stock')
            stock_tables = conn.execute("SHOW TABLES").fetchall()
            stock_status['status'] = 'ok'
            stock_status['tables'] = [t[0] for t in stock_tables]
            conn.close()
        except Exception as e:
            stock_status['error'] = str(e)
            # 尝试初始化
            try:
                from database import DuckDBManager
                db = DuckDBManager(db_path=STOCK_DB_PATH, asset_type='stock')
                db.close()
                stock_status['status'] = 'ok'
            except Exception as init_e:
                stock_status['init_error'] = str(init_e)
        
        # 检查ETF数据库
        try:
            conn = get_conn(asset_type='etf')
            etf_tables = conn.execute("SHOW TABLES").fetchall()
            etf_status['status'] = 'ok'
            etf_status['tables'] = [t[0] for t in etf_tables]
            conn.close()
        except Exception as e:
            etf_status['error'] = str(e)
            # 尝试初始化
            try:
                from database import DuckDBManager
                db = DuckDBManager(db_path=ETF_DB_PATH, asset_type='etf')
                db.close()
                etf_status['status'] = 'ok'
            except Exception as init_e:
                etf_status['init_error'] = str(init_e)
        
        return jsonify({
            'status': 'ok',
            'databases': {
                'stock': stock_status,
                'etf': etf_status
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'msg': str(e)
        }), 500
    finally:
        if conn is not None:
            conn.close()

# ═══════════════════════════════════════════════════════════
# 进度查询接口
# ═══════════════════════════════════════════════════════════
@app.route('/api/progress')
def api_progress():
    try:
        get_progress = _get_core()[1]
        progress = get_progress()
        progress['backend_task_running'] = _task_running
        progress['data_source'] = 'qmt'
        progress['log_path'] = APP_LOG_PATH
        progress['task'] = dict(_current_task) if _current_task else None
        if progress['task']:
            progress['task'].pop('started_timestamp', None)
        return jsonify(progress)
    except Exception as e:
        return jsonify({
            'is_running': False,
            'task_name': '',
            'total': 0,
            'processed': 0,
            'success': 0,
            'error': 0,
            'speed': 0,
            'eta': '',
            'message': '',
            'backend_task_running': _task_running,
            'data_source': 'qmt',
            'log_path': APP_LOG_PATH
        })


@app.route('/api/logs')
def api_logs():
    try:
        limit = request.args.get('limit', 200)
        include_errors = request.args.get('errors', '1') != '0'
        logs = read_last_lines(APP_LOG_PATH, limit)
        error_logs = read_last_lines(ERROR_LOG_PATH, 80) if include_errors else []
        return jsonify({
            'status': 'ok',
            'is_running': _task_running,
            'log_path': APP_LOG_PATH,
            'error_log_path': ERROR_LOG_PATH,
            'logs': logs,
            'error_logs': error_logs,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e), 'logs': [], 'error_logs': []}), 500


# ═══════════════════════════════════════════════════════════
# 2. /api/overview
# ═══════════════════════════════════════════════════════════
@app.route('/api/overview')
def api_overview():
    table = request.args.get('table', '').strip()
    try:
        # 根据表名自动选择数据库
        asset_type = asset_type_for_table(table)
        conn = get_conn(asset_type=asset_type)
        if table and table in ALLOWED_TABLES:
            columns = set(table_columns(conn, table))
            count      = conn.execute(f"SELECT COUNT(1) FROM {table}").fetchone()[0]
            code_count = conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table}").fetchone()[0] if 'code' in columns else 0
            dr         = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone() if 'date' in columns else (None, None)
            min_date   = str(dr[0]) if dr[0] else '-'
            max_date   = str(dr[1]) if dr[1] else '-'
            try:
                latest   = str(dr[1])
                if latest != '-' and 'date' in columns and 'pctChg' in columns:
                    rise_cnt = conn.execute(
                        f"SELECT COUNT(1) FROM {table} WHERE date='{latest}' AND pctChg > 0"
                    ).fetchone()[0]
                    fall_cnt = conn.execute(
                        f"SELECT COUNT(1) FROM {table} WHERE date='{latest}' AND pctChg < 0"
                    ).fetchone()[0]
                else:
                    rise_cnt = fall_cnt = 0
                cards = [
                    {'label': '总记录数',  'value': f'{count:,}',   'trend': 'neutral'},
                    {'label': '证券数量',  'value': str(code_count),'trend': 'neutral'},
                    {'label': '最早日期',  'value': min_date,       'trend': 'neutral'},
                    {'label': '最新日期',  'value': max_date,       'trend': 'rise'},
                    {'label': '最新上涨数','value': str(rise_cnt),  'trend': 'rise'},
                    {'label': '最新下跌数','value': str(fall_cnt),  'trend': 'fall'},
                ]
            except Exception:
                cards = [
                    {'label': '总记录数', 'value': f'{count:,}',   'trend': 'neutral'},
                    {'label': '证券数量', 'value': str(code_count),'trend': 'neutral'},
                    {'label': '最早日期', 'value': min_date,       'trend': 'neutral'},
                    {'label': '最新日期', 'value': max_date,       'trend': 'rise'},
                ]
            conn.close()
            return jsonify(cards)
        else:
            # 查询两个数据库的所有表
            result = []
            for atype in ['stock', 'etf']:
                try:
                    conn_atype = get_conn(asset_type=atype)
                    all_tables = conn_atype.execute("SHOW TABLES").fetchall()
                    for t in all_tables:
                        tname = t[0]
                        cnt   = conn_atype.execute(f"SELECT COUNT(1) FROM {tname}").fetchone()[0]
                        result.append({'table': tname, 'count': cnt})
                    conn_atype.close()
                except Exception:
                    pass
            return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 3. /api/table — 分页表格查询
# ═══════════════════════════════════════════════════════════
@app.route('/api/table')
def api_table():
    table     = request.args.get('table', 'stock_daily').strip()
    page      = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', 50))
    keyword   = request.args.get('keyword', '').strip()
    start     = request.args.get('start', '').strip()
    end       = request.args.get('end', '').strip()
    offset    = page * page_size

    tbl, err = check_table(table)
    if err:
        return err

    conn = None
    try:
        #  参数化 WHERE，防注入，同时避免特殊字符（如 sh.600006 中的点）破坏 SQL
        asset_type = asset_type_for_table(table)
        conn  = get_conn(asset_type=asset_type)
        if table in MARKET_TABLES:
            select_cols, from_sql = market_display_sql(conn, table)
            where, params = build_market_where(keyword=keyword, start=start, end=end)
            sql_data  = (
                f"SELECT {select_cols} FROM {from_sql} {where} "
                f"ORDER BY d.date DESC, d.code "
                f"LIMIT {page_size} OFFSET {offset}"
            )
            sql_count = f"SELECT COUNT(1) FROM {from_sql} {where}"
        elif table in FACTOR_RPS_TABLES:
            info_table = 'etf_info' if table.startswith('etf_') else 'stock_info'
            from_sql = f"{table} d LEFT JOIN {info_table} i ON d.code = i.code"
            where, params = build_market_where(keyword=keyword, start=start, end=end)
            factor_columns = set(table_columns(conn, table))
            ret_5_expr = "d.ret_5, " if "ret_5" in factor_columns else "NULL AS ret_5, "
            rps_5_expr = "d.rps_5, " if "rps_5" in factor_columns else "NULL AS rps_5, "
            sql_data = (
                "SELECT d.code, COALESCE(i.name, '') AS name, d.date, "
                f"{ret_5_expr}d.ret_20, d.ret_50, d.ret_120, d.ret_250, "
                f"{rps_5_expr}d.rps_20, d.rps_50, d.rps_120, d.rps_250, d.universe, d.factor_version, d.updated_at "
                f"FROM {from_sql} {where} "
                "ORDER BY d.date DESC, d.code "
                f"LIMIT {page_size} OFFSET {offset}"
            )
            sql_count = f"SELECT COUNT(1) FROM {from_sql} {where}"
        else:
            columns = table_columns(conn, table)
            where, params = build_where(keyword=keyword, start=start, end=end, columns=columns)
            order_by = "date DESC, code" if "date" in columns and "code" in columns else columns[0]
            sql_data  = (
                f"SELECT * FROM {table} {where} "
                f"ORDER BY {order_by} "
                f"LIMIT {page_size} OFFSET {offset}"
            )
            sql_count = f"SELECT COUNT(1) FROM {table} {where}"

        # 根据表名自动选择数据库
        # ② 用参数列表执行，DuckDB 支持 ? 占位符
        df    = conn.execute(sql_data,  params).fetchdf()
        total = conn.execute(sql_count, params).fetchone()[0]

        return jsonify({
            'columns': df.columns.tolist(),
            'rows':    df_to_records(df),
            'total':   int(total),
        })

    except Exception as e:
        # ③ 打印完整堆栈，方便服务端日志定位问题
        tb = traceback.format_exc()
        print(f"[ERROR] /api/table table={table} keyword={keyword!r} "
              f"start={start!r} end={end!r}\n{tb}")
        return jsonify({
            'status': 'error',
            'msg':    str(e),
            'detail': tb,          # 开发期暴露，上线后可删
        }), 500
    finally:
        if conn is not None:
            conn.close()

# ═══════════════════════════════════════════════════════════
# 4. /api/codes
# ═══════════════════════════════════════════════════════════
@app.route('/api/codes')
def api_codes():
    table = request.args.get('table', 'stock_daily')
    tbl, err = check_table(table)
    if err: return err
    try:
        asset_type = asset_type_for_table(table)
        conn  = get_conn(asset_type=asset_type)
        if 'code' not in table_columns(conn, table):
            conn.close()
            return jsonify([])
        codes = conn.execute(f"SELECT DISTINCT code FROM {table} ORDER BY code").fetchall()
        conn.close()
        return jsonify([c[0] for c in codes])
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/api/security_search')
def api_security_search():
    asset_type = request.args.get('asset_type', 'stock').strip()
    keyword = request.args.get('keyword', '').strip()
    limit = max(1, min(int(request.args.get('limit', 20)), 100))
    if asset_type not in {'stock', 'etf'}:
        return jsonify({'status': 'error', 'msg': f'不支持的资产类型: {asset_type}'}), 400
    table = 'stock_info' if asset_type == 'stock' else 'etf_info'
    try:
        conn = get_conn(asset_type=asset_type)
        if keyword:
            rows = conn.execute(
                f"SELECT code, name FROM {table} "
                f"WHERE code LIKE ? OR name LIKE ? ORDER BY code LIMIT ?",
                [f'%{keyword}%', f'%{keyword}%', limit],
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT code, name FROM {table} ORDER BY code LIMIT ?",
                [limit],
            ).fetchall()
        conn.close()
        return jsonify([{'code': row[0], 'name': row[1] or ''} for row in rows])
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 5. /api/kline
# ═══════════════════════════════════════════════════════════
@app.route('/api/kline')
def api_kline():
    table = request.args.get('table', 'stock_daily')
    code  = request.args.get('code', '').strip()
    start = request.args.get('start', '').strip()
    end   = request.args.get('end', '').strip()
    tbl, err = check_table(table)
    if err: return err
    if table not in MARKET_TABLES:
        return jsonify({'status': 'error', 'msg': '该接口仅支持行情表'}), 400
    if not code:
        return jsonify({'status': 'error', 'msg': '缺少 code 参数'}), 400
    try:
        asset_type = asset_type_for_table(table)
        conn = get_conn(asset_type=asset_type)
        columns = table_columns(conn, table)
        where, params = build_where(code=code, start=start, end=end, columns=columns)
        info_table = info_table_for_market(table)
        name_row = conn.execute(
            f"SELECT name FROM {info_table} WHERE code = ?",
            [code]
        ).fetchone()
        security_name = name_row[0] if name_row else ''
        # 尝试含 amount 字段；若不存在则降级
        try:
            sql = (f"SELECT CAST(date AS VARCHAR) as date, open, high, low, close, "
                   f"volume, amount FROM {table} {where} ORDER BY date")
            df  = conn.execute(sql, params).fetchdf()
        except Exception:
            try:
                sql = (f"SELECT CAST(date AS VARCHAR) as date, open, high, low, close, "
                       f"volume FROM {table} {where} ORDER BY date")
                df  = conn.execute(sql, params).fetchdf()
                df['amount'] = None
            except Exception:
                sql = (f"SELECT CAST(date AS VARCHAR) as date, open, high, low, close "
                       f"FROM {table} {where} ORDER BY date")
                df  = conn.execute(sql, params).fetchdf()
                df['volume'] = None
                df['amount'] = None
        conn.close()

        if df.empty:
            return jsonify({'code': code, 'name': security_name, 'dates': [], 'ohlc': [], 'volumes': [], 'amounts': [], 'mas': {}})

        # 转换为 float，避免 NaN/Inf 序列化失败
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: round(float(x), 4) if x is not None and str(x) not in ('nan','None','') else None)

        dates   = df['date'].tolist()
        # ECharts candlestick 格式：[open, close, low, high]
        ohlc    = [[row['open'], row['close'], row['low'], row['high']] for _, row in df.iterrows()]
        volumes = df['volume'].tolist() if 'volume' in df.columns else []
        amounts = df['amount'].tolist() if 'amount' in df.columns else []

        # 计算均线（MA5 MA10 MA20 MA60）
        closes = [r[1] for r in ohlc]  # close 在 index 1
        mas = {}
        for n in [5, 10, 20, 60]:
            ma_vals = []
            for i in range(len(closes)):
                if i < n - 1 or closes[i] is None:
                    ma_vals.append(None)
                else:
                    window = [c for c in closes[i-n+1:i+1] if c is not None]
                    ma_vals.append(round(sum(window)/len(window), 4) if window else None)
            mas[str(n)] = ma_vals

        return jsonify({
            'code':    code,
            'name':    security_name,
            'dates':   dates,
            'ohlc':    ohlc,
            'volumes': volumes,
            'amounts': amounts,
            'mas':     mas
        })
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ERROR] /api/kline\n{tb}")
        return jsonify({'status': 'error', 'msg': str(e), 'detail': tb}), 500


# ═══════════════════════════════════════════════════════════
# 6. /api/stats
# ═══════════════════════════════════════════════════════════
@app.route('/api/stats')
def api_stats():
    table = request.args.get('table', 'stock_daily')
    tbl, err = check_table(table)
    if err: return err
    try:
        asset_type = asset_type_for_table(table)
        conn   = get_conn(asset_type=asset_type)
        schema = conn.execute(f"DESCRIBE {table}").fetchall()
        num_types = ('INTEGER','DOUBLE','FLOAT','BIGINT','DECIMAL','REAL','HUGEINT','UBIGINT')
        num_cols  = [col[0] for col in schema if col[1].upper().split('(')[0] in num_types]
        stats = {}
        for col in num_cols:
            res = conn.execute(
                f"SELECT MIN({col}), MAX({col}), AVG({col}), SUM({col}) FROM {table}"
            ).fetchone()
            stats[col] = {
                'min': round(float(res[0]), 4) if res[0] is not None else None,
                'max': round(float(res[1]), 4) if res[1] is not None else None,
                'avg': round(float(res[2]), 4) if res[2] is not None else None,
                'sum': round(float(res[3]), 4) if res[3] is not None else None,
            }
        conn.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 7. /api/top_movers — 涨跌幅排行榜
# ═══════════════════════════════════════════════════════════
@app.route('/api/top_movers')
def api_top_movers():
    table     = request.args.get('table', 'stock_daily')
    date      = request.args.get('date', '').strip()
    limit     = min(int(request.args.get('limit', 20)), 100)
    direction = request.args.get('direction', 'rise')

    tbl, err = check_table(table)
    if err: return err
    if table not in MARKET_TABLES:
        return jsonify({'status': 'error', 'msg': '该接口仅支持行情表'}), 400

    try:
        asset_type = asset_type_for_table(table)
        conn = get_conn(asset_type=asset_type)
        if not date:
            row  = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
            date = str(row[0]) if row and row[0] else ''
        if not date:
            conn.close()
            return jsonify({'status': 'error', 'msg': '表中无数据'}), 404

        order = 'DESC' if direction == 'rise' else 'ASC'
        info_table = info_table_for_market(table)
        try:
            sql = (f"SELECT d.code, COALESCE(i.name, '') AS name, d.open, d.high, d.low, "
                   f"d.close, d.volume, d.pctChg "
                   f"FROM {table} d LEFT JOIN {info_table} i ON d.code = i.code "
                   f"WHERE d.date = ? "
                   f"ORDER BY pctChg {order} LIMIT {limit}")
            df = conn.execute(sql, [date]).fetchdf()
        except Exception:
            sql = (f"SELECT d.code, COALESCE(i.name, '') AS name, d.open, d.high, d.low, "
                   f"d.close, d.volume, "
                   f"ROUND((d.close-d.open)/NULLIF(d.open,0)*100, 2) AS pctChg "
                   f"FROM {table} d LEFT JOIN {info_table} i ON d.code = i.code "
                   f"WHERE d.date = ? "
                   f"ORDER BY pctChg {order} LIMIT {limit}")
            df = conn.execute(sql, [date]).fetchdf()
        conn.close()
        return jsonify({'date': date, 'direction': direction, 'data': df_to_records(df)})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 8. /api/distribution — 成交量/价格区间分布
# ═══════════════════════════════════════════════════════════
@app.route('/api/distribution')
def api_distribution():
    table = request.args.get('table', 'stock_daily')
    field = request.args.get('field', 'close')
    bins  = min(int(request.args.get('bins', 10)), 50)
    date  = request.args.get('date', '').strip()
    start = request.args.get('start', '').strip()
    end   = request.args.get('end', '').strip()

    tbl, err = check_table(table)
    if err: return err
    if table not in MARKET_TABLES:
        return jsonify({'status': 'error', 'msg': '该接口仅支持行情表'}), 400
    ALLOWED_FIELDS = {'open','high','low','close','volume','pctChg','amount','turnover_rate'}
    if field not in ALLOWED_FIELDS:
        return jsonify({'status': 'error', 'msg': f'不支持的字段: {field}'}), 400

    try:
        import numpy as np
        where, params = build_where(start=start, end=end)
        if date:
            where  = (where + ' AND date = ?') if where else 'WHERE date = ?'
            params = params + [date]
        asset_type = asset_type_for_table(table)
        conn = get_conn(asset_type=asset_type)
        if where:
            sql = f"SELECT {field} FROM {table} {where} AND {field} IS NOT NULL"
        else:
            sql = f"SELECT {field} FROM {table} WHERE {field} IS NOT NULL"
        df = conn.execute(sql, params).fetchdf()
        conn.close()

        if df.empty:
            return jsonify({'bins': [], 'counts': []})

        vals = df[field].dropna().astype(float)

        if field == 'pctChg':
            bands = [
                ('≤-20%', vals <= -20),
                ('-20~-10%', (vals > -20) & (vals <= -10)),
                ('-10~-7%', (vals > -10) & (vals <= -7)),
                ('-7~-5%', (vals > -7) & (vals <= -5)),
                ('-5~-3%', (vals > -5) & (vals <= -3)),
                ('-3~-1%', (vals > -3) & (vals <= -1)),
                ('-1~0%', (vals > -1) & (vals < 0)),
                ('0%', vals == 0),
                ('0~1%', (vals > 0) & (vals <= 1)),
                ('1~3%', (vals > 1) & (vals <= 3)),
                ('3~5%', (vals > 3) & (vals <= 5)),
                ('5~7%', (vals > 5) & (vals <= 7)),
                ('7~10%', (vals > 7) & (vals < 10)),
                ('10~20%', (vals >= 10) & (vals < 20)),
                ('≥20%', vals >= 20),
            ]
            result = {
                'field': field,
                'bins': [label for label, _ in bands],
                'counts': [int(mask.sum()) for _, mask in bands],
                'min': round(float(vals.min()), 4),
                'max': round(float(vals.max()), 4),
                'mean': round(float(vals.mean()), 4),
                'rise_count': int((vals > 0).sum()),
                'fall_count': int((vals < 0).sum()),
                'flat_count': int((vals == 0).sum()),
                'avg_pct_chg': round(float(vals.mean()), 4),
            }
        else:
            counts, edges = np.histogram(vals, bins=bins)
            result = {
                'field':  field,
                'bins':   [f'{edges[i]:.2f}~{edges[i+1]:.2f}' for i in range(len(edges)-1)],
                'counts': counts.tolist(),
                'min':    round(float(vals.min()), 4),
                'max':    round(float(vals.max()), 4),
                'mean':   round(float(vals.mean()), 4),
            }

        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 9. /api/trend — 走势聚合
# ═══════════════════════════════════════════════════════════
@app.route('/api/trend')
def api_trend():
    table = request.args.get('table', 'stock_daily')
    code  = request.args.get('code', '').strip()
    start = request.args.get('start', '').strip()
    end   = request.args.get('end', '').strip()
    field = request.args.get('field', 'close')
    limit = min(int(request.args.get('limit', 500)), 2000)
    allowed_fields = {'open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn'}

    tbl, err = check_table(table)
    if err: return err
    if table not in MARKET_TABLES:
        return jsonify({'status': 'error', 'msg': '该接口仅支持行情表'}), 400
    if field not in allowed_fields:
        return jsonify({'status': 'error', 'msg': f'不支持的字段: {field}'}), 400

    try:
        asset_type = asset_type_for_table(table)
        conn  = get_conn(asset_type=asset_type)
        columns = table_columns(conn, table)
        if field not in columns:
            conn.close()
            return jsonify({'status': 'error', 'msg': f'字段不存在: {field}'}), 400
        where, params = build_where(code=code, start=start, end=end, columns=columns)
        sql   = (f"SELECT CAST(date AS VARCHAR) as date, "
                 f"ROUND(AVG({field}),4) as avg_val, "
                 f"ROUND(MAX({field}),4) as max_val, "
                 f"ROUND(MIN({field}),4) as min_val, "
                 f"SUM(volume) as total_volume "
                 f"FROM {table} {where} "
                 f"GROUP BY date ORDER BY date DESC LIMIT {limit}")
        df = conn.execute(sql, params).fetchdf()
        security_name = ''
        if code:
            info_table = info_table_for_market(table)
            name_row = conn.execute(
                f"SELECT name FROM {info_table} WHERE code = ?",
                [code]
            ).fetchone()
            security_name = name_row[0] if name_row else ''
        conn.close()
        df = df.iloc[::-1].reset_index(drop=True)
        return jsonify({'field': field, 'code': code, 'name': security_name, 'data': df_to_records(df)})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 10. /api/refresh_status
# ═══════════════════════════════════════════════════════════
@app.route('/api/refresh_status')
def api_refresh_status():
    try:
        result = []
        for atype in ['stock', 'etf']:
            try:
                conn = get_conn(asset_type=atype)
                tables = conn.execute("SHOW TABLES").fetchall()
                for t in tables:
                    tname = t[0]
                    try:
                        columns = set(table_columns(conn, tname))
                        if 'date' in columns:
                            row = conn.execute(
                                f"SELECT MAX(date), MIN(date), COUNT(1) FROM {tname}"
                            ).fetchone()
                        else:
                            count = conn.execute(f"SELECT COUNT(1) FROM {tname}").fetchone()[0]
                            row = (None, None, count)
                        result.append({
                            'table':    tname,
                            'max_date': str(row[0]) if row[0] else None,
                            'min_date': str(row[1]) if row[1] else None,
                            'count':    int(row[2]),
                        })
                    except Exception:
                        result.append({'table': tname, 'max_date': None, 'min_date': None, 'count': 0})
                conn.close()
            except Exception:
                pass
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 11. /api/export — 导出 CSV
# ═══════════════════════════════════════════════════════════
@app.route('/api/export')
def api_export():
    table   = request.args.get('table', 'stock_daily')
    keyword = request.args.get('keyword', '').strip()
    start   = request.args.get('start', '').strip()
    end     = request.args.get('end', '').strip()
    code    = request.args.get('code', '').strip()
    limit   = min(int(request.args.get('limit', 50000)), 200000)

    tbl, err = check_table(table)
    if err: return err

    try:
        asset_type = asset_type_for_table(table)
        conn  = get_conn(asset_type=asset_type)
        if table in MARKET_TABLES:
            select_cols, from_sql = market_display_sql(conn, table)
            where, params = build_market_where(keyword=keyword, start=start, end=end, code=code)
            sql = f"SELECT {select_cols} FROM {from_sql} {where} ORDER BY d.date DESC, d.code LIMIT {limit}"
        else:
            columns = table_columns(conn, table)
            where, params = build_where(keyword=keyword, start=start, end=end, code=code, columns=columns)
            order_by = "date DESC, code" if "date" in columns and "code" in columns else columns[0]
            sql = f"SELECT * FROM {table} {where} ORDER BY {order_by} LIMIT {limit}"
        df    = conn.execute(sql, params).fetchdf()
        conn.close()
        for col in df.columns:
            if 'date' in str(df[col].dtype) or 'datetime' in str(df[col].dtype):
                df[col] = df[col].astype(str)
        buf      = io.StringIO()
        df.to_csv(buf, index=False, encoding='utf-8-sig')
        buf.seek(0)
        filename = f"{table}_{datetime.date.today()}.csv"
        return Response(
            buf.getvalue().encode('utf-8-sig'),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 12. /api/delete — 删除记录（POST）
# ═══════════════════════════════════════════════════════════
@app.route('/api/repair_latest_derived_fields', methods=['POST'])
def api_repair_latest_derived_fields():
    body = request.get_json(force=True, silent=True) or {}
    asset_type = body.get('asset_type', 'stock').strip()
    frequency = body.get('frequency', 'd').strip()
    confirm = body.get('confirm', False)

    if not confirm:
        return jsonify({'status': 'error', 'msg': '请传入 confirm:true 以确认修复'}), 400
    if asset_type not in {'stock', 'etf'}:
        return jsonify({'status': 'error', 'msg': '不支持的资产类型'}), 400
    if frequency not in {'d', 'w', 'm'}:
        return jsonify({'status': 'error', 'msg': '不支持的数据周期'}), 400

    db = None
    try:
        from database import DuckDBManager
        db_path = STOCK_DB_PATH if asset_type == 'stock' else ETF_DB_PATH
        db = DuckDBManager(db_path=db_path, asset_type=asset_type)
        result = db.repair_latest_derived_fields(frequency=frequency)
        _dashboard_cache['expires_at'] = 0
        return jsonify({'status': 'ok', **result})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500
    finally:
        if db is not None:
            db.close()


@app.route('/api/delete_preview', methods=['POST'])
def api_delete_preview():
    body  = request.get_json(force=True, silent=True) or {}
    table = body.get('table', '')
    code  = body.get('code', '').strip()
    start = body.get('start', '').strip()
    end   = body.get('end', '').strip()

    tbl, err = check_table(table)
    if err: return err
    if not code and not start and not end:
        return jsonify({'status': 'error', 'msg': '必须指定 code 或 start/end 至少一个条件'}), 400
    try:
        asset_type = asset_type_for_table(table)
        conn = get_conn(asset_type=asset_type)
        columns = table_columns(conn, table)
        where, params = build_where(code=code, start=start, end=end, columns=columns)
        if not where:
            conn.close()
            return jsonify({'status': 'error', 'msg': '条件为空，拒绝全表删除'}), 400
        count = conn.execute(f"SELECT COUNT(1) FROM {table} {where}", params).fetchone()[0]
        conn.close()
        return jsonify({'status': 'ok', 'count': int(count)})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/api/delete', methods=['POST'])
def api_delete():
    body    = request.get_json(force=True, silent=True) or {}
    table   = body.get('table', '')
    code    = body.get('code', '').strip()
    start   = body.get('start', '').strip()
    end     = body.get('end', '').strip()
    confirm = body.get('confirm', False)

    tbl, err = check_table(table)
    if err: return err
    if not confirm:
        return jsonify({'status': 'error', 'msg': '请传入 confirm:true 以确认删除'}), 400
    if not code and not start and not end:
        return jsonify({'status': 'error', 'msg': '必须指定 code 或 start/end 至少一个条件'}), 400

    try:
        asset_type = asset_type_for_table(table)
        conn  = get_conn(read_only=False, asset_type=asset_type)
        columns = table_columns(conn, table)
        where, params = build_where(code=code, start=start, end=end, columns=columns)
        if not where:
            conn.close()
            return jsonify({'status': 'error', 'msg': '条件为空，拒绝全表删除'}), 400
        count = conn.execute(f"SELECT COUNT(1) FROM {table} {where}", params).fetchone()[0]
        conn.execute(f"DELETE FROM {table} {where}", params)
        conn.commit()
        conn.close()
        return jsonify({'status': 'ok', 'deleted': int(count)})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 13. /api/schema
# ═══════════════════════════════════════════════════════════
@app.route('/api/schema')
def api_schema():
    table = request.args.get('table', 'stock_daily')
    tbl, err = check_table(table)
    if err: return err
    try:
        asset_type = asset_type_for_table(table)
        conn   = get_conn(asset_type=asset_type)
        schema = conn.execute(f"DESCRIBE {table}").fetchall()
        conn.close()
        return jsonify([{'column': row[0], 'type': row[1]} for row in schema])
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 14. /api/summary_by_code
# ═══════════════════════════════════════════════════════════
@app.route('/api/summary_by_code')
def api_summary_by_code():
    table = request.args.get('table', 'stock_daily')
    start = request.args.get('start', '').strip()
    end   = request.args.get('end', '').strip()
    limit = min(int(request.args.get('limit', 50)), 500)

    tbl, err = check_table(table)
    if err: return err
    if table not in MARKET_TABLES:
        return jsonify({'status': 'error', 'msg': '该接口仅支持行情表'}), 400

    try:
        asset_type = asset_type_for_table(table)
        conn  = get_conn(asset_type=asset_type)
        where, params = build_market_where(start=start, end=end)
        info_table = info_table_for_market(table)
        sql   = (f"SELECT d.code, COALESCE(i.name, '') AS name, COUNT(1) as days, "
                 f"ROUND(AVG(d.close),4) as avg_close, "
                 f"MAX(d.high) as max_high, MIN(d.low) as min_low, "
                 f"SUM(d.volume) as total_volume "
                 f"FROM {table} d LEFT JOIN {info_table} i ON d.code = i.code {where} "
                 f"GROUP BY d.code, i.name ORDER BY total_volume DESC LIMIT {limit}")
        df = conn.execute(sql, params).fetchdf()
        conn.close()
        return jsonify(df_to_records(df))
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 启动入口
# ═══════════════════════════════════════════════════════════
if __name__ == '__main__':
    socketserver.TCPServer.allow_reuse_address = True
    print(f"[Server] DB_PATH   = {DB_PATH}")
    print(f"[Server] DB exists = {os.path.exists(DB_PATH)}")
    app.run(host='127.0.0.1', port=5678, debug=False, threaded=True)
