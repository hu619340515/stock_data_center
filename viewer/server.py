"""
量化数据可视化管理 - Flask API Server
接口清单（共 14 个）：
  GET  /                        → 前端首页
  GET  /api/status              → 数据库连接状态
  GET  /api/overview            → 概览卡片 / 所有表行数
  GET  /api/table               → 分页表格查询（支持关键词+日期筛选）
  GET  /api/codes               → 获取指定表的 code 列表
  GET  /api/kline               → 单只证券 K 线数据
  GET  /api/stats               → 数值列统计（min/max/avg/sum）
  GET  /api/top_movers          → 涨跌幅排行榜
  GET  /api/distribution        → 成交量/价格区间分布
  GET  /api/trend               → 指数/均线走势聚合
  GET  /api/refresh_status      → 各表最新更新时间统计
  GET  /api/export              → 导出 CSV（流式下载）
  POST /api/delete              → 删除指定 code 或日期范围记录
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
import numpy as np
import duckdb
from flask import Flask, request, jsonify, send_from_directory, make_response, Response
from flask_cors import CORS

# 尝试导入数据管道，如果失败则使用 Mock 类
try:
    # 假设 core.py 在项目根目录，与 viewer/ 平级
    import sys
    _PROJECT_ROOT_FOR_IMPORT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if _PROJECT_ROOT_FOR_IMPORT not in sys.path:
        sys.path.append(_PROJECT_ROOT_FOR_IMPORT)
    from core import StockDataPipeline, get_progress, reset_progress
except ImportError as e:
    print(f"WARNING: 'core.StockDataPipeline' not found: {e}. Using a mock class for server startup.")
    # 定义一个 mock 类, 避免因缺少 core.py 而启动失败
    class StockDataPipeline:
        def __init__(self, db_path):
            print(f"[MOCK] Initialized StockDataPipeline with db_path: {db_path}")
        def daily_update_pipeline(self, data_type, mode=None):
            print(f"[MOCK] Running daily_update_pipeline for data_type={data_type}, mode={mode}")
            import time
            time.sleep(2) # 模拟耗时
            if 'fail' in str(data_type):
                return False, f"Mock failure for {data_type}"
            return True, f"Mock success for {data_type} (mode: {mode})"
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
    
    def get_progress():
        return {'is_running': False}
    
    def reset_progress():
        pass

# 后台任务状态
_task_lock = threading.Lock()
_task_running = False
_current_pipeline = None  # 保存当前正在运行的 pipeline 实例

# ─────────────────────────────────────────────
# 路径配置
# ─────────────────────────────────────────────
_VIEWER_DIR   = os.path.abspath(os.path.dirname(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_VIEWER_DIR, '..'))

# 优先读环境变量，其次自动定位项目根目录的 quant_data.db
DB_PATH = os.environ.get(
    'DB_PATH',
    os.path.join(_PROJECT_ROOT, 'quant_data.db')
)

app = Flask(__name__, static_folder=_VIEWER_DIR)
CORS(app)


def _run_task(func, *args, use_temp_db=False, asset_type='stock'):
    """在后台线程中执行任务，使用临时数据库避免锁定"""
    global _task_running, _current_pipeline
    with _task_lock:
        if _task_running:
            return False, '已有任务正在运行，请等待完成'
        _task_running = True
        reset_progress()
    
    def worker():
        global _task_running, _current_pipeline
        try:
            pipeline = StockDataPipeline(use_temp_db=use_temp_db)
            _current_pipeline = pipeline
            func(pipeline, *args)
            
            # 任务完成，合并到主数据库
            if use_temp_db and not pipeline.should_stop:
                tables = None
                if asset_type == 'stock':
                    tables = ['stock_daily', 'stock_weekly', 'stock_monthly']
                elif asset_type == 'etf':
                    tables = ['etf_daily', 'etf_weekly', 'etf_monthly']
                pipeline.merge_to_main_db(DB_PATH, tables)
        except Exception as e:
            print(f"后台任务出错: {e}")
            traceback.print_exc()
        finally:
            if _current_pipeline:
                _current_pipeline.cleanup_temp_db()
                _current_pipeline = None
            with _task_lock:
                _task_running = False
    
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return True, '任务已启动，请在进度条中查看'

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
    ok, msg = _run_task(lambda p: p.full_download_pipeline(f'{target}_d'), use_temp_db=True, asset_type=target)
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/daily_to_latest', methods=['POST'])
def api_daily_to_latest():
    target = request.json.get('target', 'stock')
    ok, msg = _run_task(lambda p: p.daily_update_pipeline(f'{target}_d'), use_temp_db=True, asset_type=target)
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/aggregate_weekly', methods=['POST'])
def api_aggregate_weekly():
    target = request.json.get('target', 'stock')
    ok, msg = _run_task(lambda p: p.full_download_pipeline(f'{target}_w'), use_temp_db=True, asset_type=target)
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/aggregate_monthly', methods=['POST'])
def api_aggregate_monthly():
    target = request.json.get('target', 'stock')
    ok, msg = _run_task(lambda p: p.full_download_pipeline(f'{target}_m'), use_temp_db=True, asset_type=target)
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/etf_download', methods=['POST'])
def api_etf_download():
    ok, msg = _run_task(lambda p: p.etf_download_pipeline('d'), use_temp_db=True, asset_type='etf')
    return jsonify({'success': ok, 'message': msg})

@app.route('/api/etf_update_latest', methods=['POST'])
def api_etf_update_latest():
    ok, msg = _run_task(lambda p: p.etf_update_pipeline('d'), use_temp_db=True, asset_type='etf')
    return jsonify({'success': ok, 'message': msg})


# ─────────────────────────────────────────────
# 白名单
# ─────────────────────────────────────────────
ALLOWED_TABLES = {
    'stock_daily', 'stock_weekly', 'stock_monthly',
    'etf_daily',   'etf_weekly',   'etf_monthly'
}

# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────
def get_conn(read_only=True):
    try:
        # 使用 read_only 模式避免与后台写入任务冲突
        conn = duckdb.connect(DB_PATH, read_only=read_only)
        return conn
    except TypeError:
        return duckdb.connect(DB_PATH)
    except Exception:
        # 如果数据库被锁定，尝试只读连接
        if read_only:
            try:
                return duckdb.connect(DB_PATH, read_only=True)
            except Exception:
                raise
        raise

def df_to_records(df):
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if 'datetime' in dtype_str or 'date' in dtype_str or dtype_str == 'object':
            df[col] = df[col].astype(str)
    return df.to_dict(orient='records')

def check_table(table):
    if table not in ALLOWED_TABLES:
        return None, (jsonify({'status': 'error', 'msg': f'未知表名: {table}'}), 400)
    return table, None

def build_where(keyword='', start='', end='', code=''):
    """返回 (where_clause: str, params: list)，使用参数化占位符 ? 防止 SQL 注入。"""
    conds  = []
    params = []
    if keyword:
        conds.append("code LIKE ?")
        params.append(f'%{keyword}%')
    if code:
        conds.append("code = ?")
        params.append(code)
    if start:
        conds.append("date >= ?")
        params.append(start)
    if end:
        conds.append("date <= ?")
        params.append(end)
    clause = ('WHERE ' + ' AND '.join(conds)) if conds else ''
    return clause, params


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
# 1. /api/status
# ═══════════════════════════════════════════════════════════
@app.route('/api/status')
def api_status():
    try:
        conn = get_conn()
        tables = conn.execute("SHOW TABLES").fetchall()
        table_list = [t[0] for t in tables]
        conn.close()
        return jsonify({
            'status': 'ok',
            'db_path': DB_PATH,
            'db_exists': os.path.exists(DB_PATH),
            'tables': table_list
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'msg': str(e),
            'db_path': DB_PATH,
            'db_exists': os.path.exists(DB_PATH)
        }), 500


# ═══════════════════════════════════════════════════════════
# 进度查询接口
# ═══════════════════════════════════════════════════════════
@app.route('/api/progress')
def api_progress():
    try:
        progress = get_progress()
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
            'message': ''
        })


# ═══════════════════════════════════════════════════════════
# 2. /api/overview
# ═══════════════════════════════════════════════════════════
@app.route('/api/overview')
def api_overview():
    table = request.args.get('table', '').strip()
    try:
        conn = get_conn()
        if table and table in ALLOWED_TABLES:
            count      = conn.execute(f"SELECT COUNT(1) FROM {table}").fetchone()[0]
            code_count = conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table}").fetchone()[0]
            dr         = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
            min_date   = str(dr[0]) if dr[0] else '-'
            max_date   = str(dr[1]) if dr[1] else '-'
            try:
                latest   = str(dr[1])
                rise_cnt = conn.execute(
                    f"SELECT COUNT(1) FROM {table} WHERE date='{latest}' AND pctChg > 0"
                ).fetchone()[0]
                fall_cnt = conn.execute(
                    f"SELECT COUNT(1) FROM {table} WHERE date='{latest}' AND pctChg < 0"
                ).fetchone()[0]
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
            all_tables = conn.execute("SHOW TABLES").fetchall()
            result = []
            for t in all_tables:
                tname = t[0]
                cnt   = conn.execute(f"SELECT COUNT(1) FROM {tname}").fetchone()[0]
                result.append({'table': tname, 'count': cnt})
            conn.close()
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

    try:
        # ① 参数化 WHERE，防注入，同时避免特殊字符（如 sh.600006 中的点）破坏 SQL
        where, params = build_where(keyword=keyword, start=start, end=end)

        sql_data  = (
            f"SELECT * FROM {table} {where} "
            f"ORDER BY date DESC, code "
            f"LIMIT {page_size} OFFSET {offset}"
        )
        sql_count = f"SELECT COUNT(1) FROM {table} {where}"

        conn  = get_conn()
        # ② 用参数列表执行，DuckDB 支持 ? 占位符
        df    = conn.execute(sql_data,  params).fetchdf()
        total = conn.execute(sql_count, params).fetchone()[0]
        conn.close()

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


# ═══════════════════════════════════════════════════════════
# 4. /api/codes
# ═══════════════════════════════════════════════════════════
@app.route('/api/codes')
def api_codes():
    table = request.args.get('table', 'stock_daily')
    tbl, err = check_table(table)
    if err: return err
    try:
        conn  = get_conn()
        codes = conn.execute(f"SELECT DISTINCT code FROM {table} ORDER BY code").fetchall()
        conn.close()
        return jsonify([c[0] for c in codes])
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
    if not code:
        return jsonify({'status': 'error', 'msg': '缺少 code 参数'}), 400
    try:
        where, params = build_where(code=code, start=start, end=end)
        conn = get_conn()
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
            return jsonify({'dates': [], 'ohlc': [], 'volumes': [], 'amounts': [], 'mas': {}})

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
        conn   = get_conn()
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

    try:
        conn = get_conn()
        if not date:
            row  = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
            date = str(row[0]) if row and row[0] else ''
        if not date:
            conn.close()
            return jsonify({'status': 'error', 'msg': '表中无数据'}), 404

        order = 'DESC' if direction == 'rise' else 'ASC'
        try:
            sql = (f"SELECT code, open, high, low, close, volume, pctChg "
                   f"FROM {table} WHERE date='{date}' "
                   f"ORDER BY pctChg {order} LIMIT {limit}")
            df = conn.execute(sql).fetchdf()
        except Exception:
            sql = (f"SELECT code, open, high, low, close, volume, "
                   f"ROUND((close-open)/NULLIF(open,0)*100, 2) AS pctChg "
                   f"FROM {table} WHERE date='{date}' "
                   f"ORDER BY pctChg {order} LIMIT {limit}")
            df = conn.execute(sql).fetchdf()
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
    ALLOWED_FIELDS = {'open','high','low','close','volume','pctChg','amount','turnover_rate'}
    if field not in ALLOWED_FIELDS:
        return jsonify({'status': 'error', 'msg': f'不支持的字段: {field}'}), 400

    try:
        where, params = build_where(start=start, end=end)
        if date:
            where  = (where + ' AND date = ?') if where else 'WHERE date = ?'
            params = params + [date]
        conn = get_conn()
        if where:
            sql = f"SELECT {field} FROM {table} {where} AND {field} IS NOT NULL"
        else:
            sql = f"SELECT {field} FROM {table} WHERE {field} IS NOT NULL"
        df = conn.execute(sql, params).fetchdf()
        conn.close()

        if df.empty:
            return jsonify({'bins': [], 'counts': []})

        vals = df[field].dropna().astype(float)
        counts, edges = np.histogram(vals, bins=bins)

        result = {
            'field':  field,
            'bins':   [f'{edges[i]:.2f}~{edges[i+1]:.2f}' for i in range(len(edges)-1)],
            'counts': counts.tolist(),
            'min':    round(float(vals.min()), 4),
            'max':    round(float(vals.max()), 4),
            'mean':   round(float(vals.mean()), 4),
        }

        # 当查询 pctChg 时，额外返回涨/跌/平家数与平均涨跌幅，供合并 K 线卡片使用
        if field == 'pctChg':
            result['rise_count'] = int((vals > 0).sum())
            result['fall_count'] = int((vals < 0).sum())
            result['flat_count'] = int((vals == 0).sum())
            result['avg_pct_chg'] = round(float(vals.mean()), 4)

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

    tbl, err = check_table(table)
    if err: return err

    try:
        where, params = build_where(code=code, start=start, end=end)
        conn  = get_conn()
        sql   = (f"SELECT CAST(date AS VARCHAR) as date, "
                 f"ROUND(AVG({field}),4) as avg_val, "
                 f"ROUND(MAX({field}),4) as max_val, "
                 f"ROUND(MIN({field}),4) as min_val, "
                 f"SUM(volume) as total_volume "
                 f"FROM {table} {where} "
                 f"GROUP BY date ORDER BY date DESC LIMIT {limit}")
        df = conn.execute(sql, params).fetchdf()
        conn.close()
        df = df.iloc[::-1].reset_index(drop=True)
        return jsonify({'field': field, 'code': code, 'data': df_to_records(df)})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 10. /api/refresh_status
# ═══════════════════════════════════════════════════════════
@app.route('/api/refresh_status')
def api_refresh_status():
    try:
        conn   = get_conn()
        tables = conn.execute("SHOW TABLES").fetchall()
        result = []
        for t in tables:
            tname = t[0]
            try:
                row = conn.execute(
                    f"SELECT MAX(date), MIN(date), COUNT(1) FROM {tname}"
                ).fetchone()
                result.append({
                    'table':    tname,
                    'max_date': str(row[0]) if row[0] else None,
                    'min_date': str(row[1]) if row[1] else None,
                    'count':    int(row[2]),
                })
            except Exception:
                result.append({'table': tname, 'max_date': None, 'min_date': None, 'count': 0})
        conn.close()
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
        where, params = build_where(keyword=keyword, start=start, end=end, code=code)
        sql   = f"SELECT * FROM {table} {where} ORDER BY date DESC, code LIMIT {limit}"
        conn  = get_conn()
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
        where = build_where(code=code, start=start, end=end)
        if not where:
            return jsonify({'status': 'error', 'msg': '条件为空，拒绝全表删除'}), 400
        conn  = get_conn(read_only=False)
        count = conn.execute(f"SELECT COUNT(1) FROM {table} {where}").fetchone()[0]
        conn.execute(f"DELETE FROM {table} {where}")
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
        conn   = get_conn()
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

    try:
        where = build_where(start=start, end=end)
        conn  = get_conn()
        sql   = (f"SELECT code, COUNT(1) as days, "
                 f"ROUND(AVG(close),4) as avg_close, "
                 f"MAX(high) as max_high, MIN(low) as min_low, "
                 f"SUM(volume) as total_volume "
                 f"FROM {table} {where} "
                 f"GROUP BY code ORDER BY total_volume DESC LIMIT {limit}")
        df = conn.execute(sql).fetchdf()
        conn.close()
        return jsonify(df_to_records(df))
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# 启动入口
# ═══════════════════════════════════════════════════════════
if __name__ == '__main__':
    print(f"[Server] DB_PATH   = {DB_PATH}")
    print(f"[Server] DB exists = {os.path.exists(DB_PATH)}")
    app.run(host='0.0.0.0', port=5678, debug=True)
