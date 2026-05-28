import pandas as pd
import time
import random
import re
import os
from logger_config import setup_logger
from config import (
    MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    RETRY_BACKOFF_FACTOR,
    QMT_IP,
    QMT_PORT,
    QMT_DATA_DIR,
    QMT_DIVIDEND_TYPE,
    QMT_DOWNLOAD_BEFORE_QUERY,
    QMT_SYNC_SECTOR_DATA,
    QMT_CODE_LIST_DATA_DIR,
    QMT_STOCK_SECTORS,
    QMT_ETF_SECTORS,
)
from data_source_interface import DataSourceInterface

logger = setup_logger("DataFetcher")

_XTDATA = None

def _xtdata():
    global _XTDATA
    if _XTDATA is None:
        try:
            from xtquant import xtdata as _mod
        except ImportError as e:
            raise ImportError(
                "未安装 xtquant，无法使用国金QMT数据源。请先安装/配置国金QMT或miniQMT的Python SDK，并确保QMT客户端已启动。"
            ) from e
        _XTDATA = _mod
    return _XTDATA

def retry_with_backoff(func):
    """
    🛡️ 带指数退避的重试装饰器
    """
    def wrapper(*args, **kwargs):
        retries = 0
        delay = INITIAL_RETRY_DELAY
        
        while retries < MAX_RETRIES:
            try:
                return func(*args, **kwargs)
            except ImportError as e:
                logger.error(f"❌ 数据源依赖缺失: {e}")
                raise
            except Exception as e:
                if "未找到处理函数" in str(e) or "ErrorID\" : 200005" in str(e) or "ErrorID\":200005" in str(e):
                    logger.error(f"❌ 数据源服务不支持当前接口: {e}")
                    raise
                retries += 1
                if retries >= MAX_RETRIES:
                    logger.error(f"❌ 达到最大重试次数，操作失败: {e}")
                    raise
                
                # 指数退避 + 随机抖动
                jitter = random.uniform(0.5, 1.5)
                wait_time = min(delay * (RETRY_BACKOFF_FACTOR ** (retries - 1)) * jitter, MAX_RETRY_DELAY)
                
                logger.warning(f"⚠️ 操作失败，{wait_time:.2f}秒后重试 ({retries}/{MAX_RETRIES}): {e}")
                time.sleep(wait_time)
        return None
    return wrapper


class QMTClient(DataSourceInterface):
    """
    国金QMT/xtquant 行情数据源。

    约定：
    - 项目内部代码格式保持 sh.600000 / sz.000001 / bj.xxxxxx。
    - QMT接口代码格式使用 600000.SH / 000001.SZ / xxxxxx.BJ。
    - 历史行情先下载到QMT本地缓存，再从缓存读取。
    """

    TARGET_COLUMNS = [
        'code', 'date', 'open', 'high', 'low', 'close', 'preclose',
        'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'
    ]

    @retry_with_backoff
    def login(self):
        xtdata = _xtdata()
        if QMT_DATA_DIR:
            try:
                xtdata.data_dir = QMT_DATA_DIR
            except Exception as e:
                logger.warning(f"⚠️ 设置QMT数据目录失败，将使用默认目录: {e}")
        if hasattr(xtdata, "connect"):
            result = xtdata.connect(QMT_IP, int(QMT_PORT) if QMT_PORT else None)
            logger.info(f"✅ QMT行情服务连接完成: {result}")
        else:
            logger.info("✅ QMT xtdata 已加载")

    def logout(self):
        logger.info("👋 QMT数据源无需显式登出")

    @staticmethod
    def _to_qmt_code(code: str) -> str:
        code = str(code).strip()
        if "." not in code:
            return code
        left, right = code.split(".", 1)
        if left.lower() in {"sh", "sz", "bj"}:
            return f"{right}.{left.upper()}"
        return code

    @staticmethod
    def _from_qmt_code(code: str) -> str:
        code = str(code).strip()
        if "." not in code:
            return code
        left, right = code.split(".", 1)
        market = right.lower()
        if market in {"sh", "sz", "bj"}:
            return f"{market}.{left}"
        return code

    @staticmethod
    def _is_stock_code(code: str) -> bool:
        return bool(re.match(r'^(sh\.6|sz\.0|sz\.3|bj\.)', code))

    @staticmethod
    def _is_etf_code(code: str) -> bool:
        return bool(re.match(r'^(sh\.5[168]|sz\.15[09])', code))

    @staticmethod
    def _period(frequency: str) -> str:
        return {
            "d": "1d",
            "w": "1w",
            "m": "1mon",
            "1": "1m",
            "5": "5m",
        }.get(frequency, "1d")

    @staticmethod
    def _date_arg(date_text: str) -> str:
        return str(date_text).replace("-", "")[:8]

    @staticmethod
    def _adjustflag() -> str:
        return {
            "front": "2",
            "front_ratio": "2",
            "back": "1",
            "back_ratio": "1",
            "none": "3",
        }.get(str(QMT_DIVIDEND_TYPE).lower(), "2")

    def _get_name(self, qmt_code: str) -> str:
        xtdata = _xtdata()
        try:
            detail = xtdata.get_instrument_detail(qmt_code)
            if isinstance(detail, dict):
                return (
                    detail.get("InstrumentName")
                    or detail.get("instrument_name")
                    or detail.get("name")
                    or detail.get("Name")
                    or ""
                )
        except Exception:
            return ""
        return ""

    def _code_list_dirs(self) -> list:
        xtdata = _xtdata()
        candidates = []
        for path in [QMT_CODE_LIST_DATA_DIR, QMT_DATA_DIR]:
            if path:
                candidates.append(path)
        try:
            data_dir = xtdata.get_data_dir()
            if data_dir:
                candidates.append(data_dir)
                # MiniQMT常返回 .../userdata_mini/datadir；国金完整客户端还会有兄弟目录 datadir。
                root = os.path.abspath(os.path.join(data_dir, os.pardir, os.pardir))
                candidates.append(os.path.join(root, "datadir"))
        except Exception:
            pass

        seen = set()
        result = []
        for path in candidates:
            path = os.path.abspath(os.path.normpath(path))
            if path not in seen and os.path.isdir(path):
                seen.add(path)
                result.append(path)
        return result

    def _get_codes_from_local_files(self, filter_func) -> pd.DataFrame:
        rows = []
        for data_dir in self._code_list_dirs():
            for market, prefix in [("SH", "sh"), ("SZ", "sz"), ("BJ", "bj")]:
                market_dir = os.path.join(data_dir, market, "86400")
                if not os.path.isdir(market_dir):
                    continue
                for entry in os.scandir(market_dir):
                    if not entry.is_file() or not entry.name.upper().endswith(".DAT"):
                        continue
                    raw_code = os.path.splitext(entry.name)[0]
                    if not re.match(r"^\d{6}$", raw_code):
                        continue
                    code = f"{prefix}.{raw_code}"
                    if filter_func(code):
                        rows.append({"code": code, "code_name": ""})

        df = pd.DataFrame(rows).drop_duplicates(subset=["code"]) if rows else pd.DataFrame(columns=["code", "code_name"])
        return df.sort_values("code").reset_index(drop=True) if not df.empty else df

    def _get_sector_codes(self, sectors: list, filter_func) -> pd.DataFrame:
        self.login()
        xtdata = _xtdata()
        rows = []
        for sector in sectors:
            if QMT_SYNC_SECTOR_DATA:
                try:
                    if hasattr(xtdata, "download_sector_data"):
                        xtdata.download_sector_data()
                except Exception as e:
                    logger.warning(f"⚠️ QMT板块数据同步失败，继续尝试读取本地板块: {e}")
            try:
                qmt_codes = xtdata.get_stock_list_in_sector(sector) or []
                logger.info(f"📋 QMT板块 {sector} 返回 {len(qmt_codes)} 个代码")
            except Exception as e:
                logger.warning(f"⚠️ 读取QMT板块 {sector} 失败: {e}")
                continue
            for qmt_code in qmt_codes:
                code = self._from_qmt_code(qmt_code)
                if filter_func(code):
                    rows.append({
                        "code": code,
                        "code_name": self._get_name(qmt_code),
                    })

        df = pd.DataFrame(rows).drop_duplicates(subset=["code"]) if rows else pd.DataFrame(columns=["code", "code_name"])
        if df.empty:
            df = self._get_codes_from_local_files(filter_func)
            logger.info(f"📁 QMT本地datadir推导出 {len(df)} 个代码")
        return df.sort_values("code").reset_index(drop=True) if not df.empty else df

    @retry_with_backoff
    def get_stock_list(self) -> pd.DataFrame:
        df = self._get_sector_codes(QMT_STOCK_SECTORS, self._is_stock_code)
        logger.info(f"✅ QMT获取到 {len(df)} 只A股股票")
        return df

    @retry_with_backoff
    def get_etf_list(self) -> pd.DataFrame:
        df = self._get_sector_codes(QMT_ETF_SECTORS, self._is_etf_code)
        logger.info(f"✅ QMT获取到 {len(df)} 只ETF基金")
        return df

    def _extract_market_df(self, market_data, qmt_code: str) -> pd.DataFrame:
        if market_data is None:
            return pd.DataFrame()
        if isinstance(market_data, pd.DataFrame):
            return market_data.copy()
        if isinstance(market_data, dict):
            if qmt_code in market_data and isinstance(market_data[qmt_code], pd.DataFrame):
                return market_data[qmt_code].copy()

            # 兼容 get_market_data 返回的 field -> DataFrame/Series 结构。
            field_frames = {}
            for field, value in market_data.items():
                if isinstance(value, pd.DataFrame):
                    if qmt_code in value.columns:
                        field_frames[field] = value[qmt_code]
                    elif len(value.columns) == 1:
                        field_frames[field] = value.iloc[:, 0]
                elif isinstance(value, pd.Series):
                    field_frames[field] = value
            if field_frames:
                return pd.DataFrame(field_frames)
        return pd.DataFrame()

    @staticmethod
    def _normalize_date_series(df: pd.DataFrame) -> pd.Series:
        if "date" in df.columns:
            raw = df["date"]
        elif "time" in df.columns:
            raw = df["time"]
        elif "stime" in df.columns:
            raw = df["stime"]
        elif "index" in df.columns:
            raw = df["index"]
        else:
            raw = pd.Series(df.index, index=df.index)

        if pd.api.types.is_datetime64_any_dtype(raw):
            return pd.to_datetime(raw).dt.strftime("%Y-%m-%d")

        raw_str = raw.astype(str).str.replace(r"\.0$", "", regex=True)
        compact = raw_str.str.replace(r"\D", "", regex=True)
        # QMT常见time可能是YYYYMMDD、YYYYMMDDHHMMSS或毫秒时间戳。
        if compact.str.len().isin([8, 14]).any():
            parsed = pd.to_datetime(compact.str.slice(0, 8), format="%Y%m%d", errors="coerce")
            if parsed.notna().any():
                return parsed.dt.strftime("%Y-%m-%d")
        numeric = pd.to_numeric(raw, errors="coerce")
        if numeric.notna().any() and numeric.dropna().astype("int64").astype(str).str.len().max() >= 12:
            return pd.to_datetime(numeric, unit="ms", errors="coerce").dt.strftime("%Y-%m-%d")
        return pd.to_datetime(raw_str, errors="coerce").dt.strftime("%Y-%m-%d")

    def _standardize_history_df(self, raw_df: pd.DataFrame, code: str) -> pd.DataFrame:
        if raw_df.empty:
            return pd.DataFrame(columns=self.TARGET_COLUMNS)

        df = raw_df.copy().reset_index()
        rename_map = {
            "vol": "volume",
            "turnover": "amount",
            "amount": "amount",
            "lastClose": "preclose",
            "preClose": "preclose",
            "pre_close": "preclose",
        }
        df.rename(columns=rename_map, inplace=True)

        df["date"] = self._normalize_date_series(df)
        df = df.dropna(subset=["date"])
        df["code"] = code

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "preclose" not in df.columns:
            df["preclose"] = df["close"].shift(1).fillna(0)
        df["preclose"] = pd.to_numeric(df["preclose"], errors="coerce").fillna(0)

        if "pctChg" not in df.columns:
            df["pctChg"] = ((df["close"] - df["preclose"]) / df["preclose"].replace(0, pd.NA) * 100).fillna(0)
        df["pctChg"] = pd.to_numeric(df["pctChg"], errors="coerce").fillna(0)

        df["adjustflag"] = self._adjustflag()
        df["turn"] = pd.to_numeric(df["turn"], errors="coerce").fillna(0) if "turn" in df.columns else 0.0
        df["tradestatus"] = df["tradestatus"].astype(str) if "tradestatus" in df.columns else "1"
        df["isST"] = df["isST"].astype(str) if "isST" in df.columns else "0"

        return df[self.TARGET_COLUMNS]

    @retry_with_backoff
    def get_stock_history(self, code: str, start_date: str, end_date: str, frequency: str = "d") -> pd.DataFrame:
        self.login()
        xtdata = _xtdata()
        qmt_code = self._to_qmt_code(code)
        period = self._period(frequency)
        start = self._date_arg(start_date)
        end = self._date_arg(end_date)

        logger.info(f"📥 [QMT] 获取 {code}({qmt_code}) {period} 数据: {start} ~ {end}")
        if QMT_DOWNLOAD_BEFORE_QUERY and hasattr(xtdata, "download_history_data"):
            xtdata.download_history_data(qmt_code, period=period, start_time=start, end_time=end)

        fields = ["open", "high", "low", "close", "volume", "amount"]
        if hasattr(xtdata, "get_market_data_ex"):
            market_data = xtdata.get_market_data_ex(
                fields,
                [qmt_code],
                period=period,
                start_time=start,
                end_time=end,
                count=-1,
                dividend_type=QMT_DIVIDEND_TYPE,
                fill_data=True,
            )
        else:
            market_data = xtdata.get_market_data(
                fields,
                [qmt_code],
                period=period,
                start_time=start,
                end_time=end,
                count=-1,
                dividend_type=QMT_DIVIDEND_TYPE,
                fill_data=True,
            )

        raw_df = self._extract_market_df(market_data, qmt_code)
        df = self._standardize_history_df(raw_df, code)
        if df.empty:
            logger.warning(f"⚠️ QMT未获取到 {code} 的历史数据")
        else:
            logger.info(f"✅ QMT返回 {code} 数据范围: {df['date'].min()} ~ {df['date'].max()}，共 {len(df)} 条")
        return df

    def get_data_source_name(self) -> str:
        return "QMT/xtquant"
