import os
from typing import List

import duckdb
import pandas as pd

from config import STOCK_DB_PATH
from logger_config import setup_logger

logger = setup_logger("Database")

MARKET_COLUMNS = [
    "code", "date", "open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST",
]

MARKET_TABLES = {
    "stock": {"d": "stock_daily", "w": "stock_weekly", "m": "stock_monthly"},
    "etf": {"d": "etf_daily", "w": "etf_weekly", "m": "etf_monthly"},
}

INFO_TABLES = {
    "stock": "stock_info",
    "etf": "etf_info",
}

VALID_ASSET_TYPES = {"stock", "etf"}
RPS_PERIODS = (20, 50, 120, 250)


def safe_print(msg):
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(str(msg).encode("utf-8", errors="replace").decode("utf-8", errors="replace"), flush=True)


class DuckDBManager:
    def __init__(self, db_path=None, asset_type: str = "stock"):
        db_path = db_path or STOCK_DB_PATH
        asset_type = (asset_type or "stock").lower()
        if asset_type not in VALID_ASSET_TYPES:
            raise ValueError(f"unsupported asset_type: {asset_type}")

        self.db_path = db_path
        self.asset_type = asset_type
        self.con = duckdb.connect(db_path)
        self._create_table()
        logger.info(f"DuckDB initialized: {db_path} ({asset_type})")

    def _create_table(self):
        self._drop_foreign_asset_tables()
        self._create_asset_info_table()
        self._create_market_tables()
        self._create_common_tables()
        if self.asset_type == "stock":
            self._create_factor_tables()
        self._migrate_legacy_market_name_columns()
        self._drop_foreign_asset_tables()
        self.con.commit()

    def _create_market_tables(self):
        for table_name in MARKET_TABLES[self.asset_type].values():
            self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                code VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                preclose DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                adjustflag VARCHAR,
                turn DOUBLE,
                tradestatus VARCHAR,
                pctChg DOUBLE,
                isST VARCHAR,
                PRIMARY KEY (code, date)
            )
            """)

        try:
            for table_name in MARKET_TABLES[self.asset_type].values():
                self._create_market_indexes(table_name)
        except Exception as e:
            logger.warning(f"Index creation notice: {e}")

    def _create_market_indexes(self, table_name: str):
        self.con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_code_date ON {table_name} (code, date)")
        self.con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date)")

    def _create_asset_info_table(self):
        table_name = INFO_TABLES[self.asset_type]
        self.con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

    def _create_common_tables(self):
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS trade_calendar (
            date DATE PRIMARY KEY,
            is_open BOOLEAN,
            prev_trade_date DATE,
            next_trade_date DATE,
            source VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

    def _create_factor_tables(self):
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS factor_rps_daily (
            code VARCHAR,
            date DATE,
            ret_20 DOUBLE,
            ret_50 DOUBLE,
            ret_120 DOUBLE,
            ret_250 DOUBLE,
            rps_20 DOUBLE,
            rps_50 DOUBLE,
            rps_120 DOUBLE,
            rps_250 DOUBLE,
            universe VARCHAR,
            factor_version VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, date)
        )
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS factor_update_log (
            factor_name VARCHAR,
            universe VARCHAR,
            factor_version VARCHAR,
            start_date DATE,
            end_date DATE,
            status VARCHAR,
            message VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        try:
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_factor_rps_daily_code_date ON factor_rps_daily (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_factor_rps_daily_date ON factor_rps_daily (date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_factor_rps_daily_date_rps120 ON factor_rps_daily (date, rps_120)")
        except Exception as e:
            logger.warning(f"Factor index creation notice: {e}")

    def _drop_foreign_asset_tables(self):
        foreign_asset = "etf" if self.asset_type == "stock" else "stock"
        foreign_tables = list(MARKET_TABLES[foreign_asset].values()) + [INFO_TABLES[foreign_asset]]
        if self.asset_type == "etf":
            foreign_tables.extend(["factor_rps_daily", "factor_update_log"])
        for table_name in foreign_tables:
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}")

    def calculate_rps_daily(self, universe: str = "all_stocks", factor_version: str = "rps_v1") -> int:
        """Recalculate daily stock RPS factors from the full daily close history."""
        if self.asset_type != "stock":
            raise ValueError("RPS factors can only be calculated from the stock database")

        date_range = self.con.execute("SELECT MIN(date), MAX(date) FROM stock_daily").fetchone()
        start_date, end_date = date_range if date_range else (None, None)
        if not start_date or not end_date:
            raise ValueError("stock_daily has no data")

        lag_columns = ",\n                ".join(
            f"close / NULLIF(LAG(close, {period}) OVER (PARTITION BY code ORDER BY date), 0) - 1 AS ret_{period}"
            for period in RPS_PERIODS
        )
        rank_columns = ",\n                ".join(
            f"""CASE
                    WHEN ret_{period} IS NULL THEN NULL
                    WHEN COUNT(ret_{period}) OVER (PARTITION BY date) <= 1 THEN 100.0
                    ELSE (RANK() OVER (PARTITION BY date ORDER BY ret_{period} NULLS LAST) - 1) * 100.0
                         / (COUNT(ret_{period}) OVER (PARTITION BY date) - 1)
                END AS rps_{period}"""
            for period in RPS_PERIODS
        )
        ret_names = ", ".join(f"ret_{period}" for period in RPS_PERIODS)
        rps_names = ", ".join(f"rps_{period}" for period in RPS_PERIODS)

        try:
            self.con.execute("BEGIN TRANSACTION")
            self.con.execute("DELETE FROM factor_rps_daily")
            self.con.execute(f"""
                INSERT INTO factor_rps_daily (
                    code, date, {ret_names}, {rps_names},
                    universe, factor_version, updated_at
                )
                WITH returns AS (
                    SELECT
                        code,
                        date,
                        {lag_columns}
                    FROM stock_daily
                ),
                ranked AS (
                    SELECT
                        code,
                        date,
                        {ret_names},
                        {rank_columns}
                    FROM returns
                )
                SELECT
                    code,
                    date,
                    {ret_names},
                    {rps_names},
                    ?,
                    ?,
                    CURRENT_TIMESTAMP
                FROM ranked
            """, [universe, factor_version])
            count = self.con.execute("SELECT COUNT(*) FROM factor_rps_daily").fetchone()[0]
            self.con.execute("""
                INSERT INTO factor_update_log (
                    factor_name, universe, factor_version, start_date, end_date,
                    status, message, updated_at
                )
                VALUES ('rps_daily', ?, ?, ?, ?, 'success', ?, CURRENT_TIMESTAMP)
            """, [universe, factor_version, start_date, end_date, f"calculated {count} rows"])
            self.con.execute("COMMIT")
            return count
        except Exception as e:
            self.con.execute("ROLLBACK")
            self.con.execute("""
                INSERT INTO factor_update_log (
                    factor_name, universe, factor_version, start_date, end_date,
                    status, message, updated_at
                )
                VALUES ('rps_daily', ?, ?, ?, ?, 'failed', ?, CURRENT_TIMESTAMP)
            """, [universe, factor_version, start_date, end_date, str(e)])
            raise

    def _table_exists(self, table_name: str) -> bool:
        return self.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0] > 0

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        if not self._table_exists(table_name):
            return False
        return self.con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            """,
            [table_name, column_name],
        ).fetchone()[0] > 0

    def _table_columns(self, table_name: str) -> List[str]:
        return [row[0] for row in self.con.execute(f"DESCRIBE {table_name}").fetchall()]

    def _migrate_legacy_market_name_columns(self):
        info_table = INFO_TABLES[self.asset_type]
        for table_name in MARKET_TABLES[self.asset_type].values():
            if not self._column_exists(table_name, "name"):
                continue
            try:
                self.con.execute(f"""
                INSERT OR REPLACE INTO {info_table} (code, name, update_time)
                SELECT code, MAX(name), CURRENT_TIMESTAMP
                FROM {table_name}
                WHERE name IS NOT NULL AND name <> ''
                GROUP BY code
                """)
                try:
                    self.con.execute(f"ALTER TABLE {table_name} DROP COLUMN name")
                except Exception:
                    self._rebuild_market_table_without_name(table_name)
                self._create_market_indexes(table_name)
                logger.info(f"Migrated and dropped legacy column: {table_name}.name")
            except Exception as e:
                logger.warning(f"Failed to migrate {table_name}.name: {e}")

    def _rebuild_market_table_without_name(self, table_name: str):
        temp_table = f"{table_name}__no_name"
        columns = ", ".join(MARKET_COLUMNS)
        self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        self.con.execute(f"""
        CREATE TABLE {temp_table} (
            code VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            preclose DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            adjustflag VARCHAR,
            turn DOUBLE,
            tradestatus VARCHAR,
            pctChg DOUBLE,
            isST VARCHAR,
            PRIMARY KEY (code, date)
        )
        """)
        self.con.execute(f"INSERT OR REPLACE INTO {temp_table} ({columns}) SELECT {columns} FROM {table_name}")
        self.con.execute(f"DROP TABLE {table_name}")
        self.con.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

    def _ensure_asset_type(self, asset_type: str) -> bool:
        asset_type = (asset_type or "stock").lower()
        if asset_type != self.asset_type:
            logger.error(f"Asset mismatch: manager={self.asset_type}, requested={asset_type}")
            return False
        return True

    def upload_df(self, df: pd.DataFrame, frequency: str = "d", asset_type: str = "stock") -> bool:
        if df.empty or not self._ensure_asset_type(asset_type):
            return False
        try:
            df_clean = self._clean_data(df)
            table_name = self._get_table_name(frequency, asset_type)
            columns = ", ".join(df_clean.columns.tolist())
            self.con.execute(f"INSERT OR REPLACE INTO {table_name} ({columns}) SELECT {columns} FROM df_clean")
            self.con.commit()
            return True
        except Exception as e:
            logger.error(f"Write failed: {e}")
            return False

    def upload_batch(self, df_list: list, frequency: str = "d", asset_type: str = "stock") -> int:
        if not df_list:
            safe_print(f"upload_batch empty list - frequency={frequency}, asset_type={asset_type}")
            return 0
        if not self._ensure_asset_type(asset_type):
            return 0

        try:
            aligned_dfs = []
            for df in df_list:
                if df is not None and not df.empty:
                    aligned_dfs.append(self._clean_data(df))

            combined_df = pd.concat(aligned_dfs, ignore_index=True) if aligned_dfs else pd.DataFrame()
            if combined_df.empty:
                safe_print(f"upload_batch empty frame - frequency={frequency}, asset_type={asset_type}")
                return 0

            df_clean = self._clean_data(combined_df)
            count = len(df_clean)
            table_name = self._get_table_name(frequency, asset_type)
            safe_print(f"upload_batch - writing {count} rows to {table_name}")

            columns = ", ".join(df_clean.columns.tolist())
            if count > 1000:
                temp_table = f"temp_{table_name}"
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
                self.con.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0")
                self.con.execute(f"INSERT INTO {temp_table} ({columns}) SELECT {columns} FROM df_clean")
                self.con.execute(f"INSERT OR REPLACE INTO {table_name} ({columns}) SELECT {columns} FROM {temp_table}")
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
            else:
                self.con.execute(f"INSERT OR REPLACE INTO {table_name} ({columns}) SELECT {columns} FROM df_clean")

            self.con.commit()
            logger.info(f"Batch write completed: {count} rows ({table_name})")
            return count
        except Exception as e:
            logger.error(f"Batch write failed: {e}")
            return 0

    def _get_table_name(self, frequency: str, asset_type: str = "stock") -> str:
        asset_type = (asset_type or "stock").lower()
        if asset_type not in MARKET_TABLES:
            asset_type = "stock"
        return MARKET_TABLES[asset_type].get(frequency, MARKET_TABLES[asset_type]["d"])

    def get_finished_stocks(self, frequency: str = "d", asset_type: str = "stock") -> set:
        if not self._ensure_asset_type(asset_type):
            return set()
        try:
            table_name = self._get_table_name(frequency, asset_type)
            res = self.con.execute(f"SELECT DISTINCT code FROM {table_name}").fetchall()
            return {row[0] for row in res}
        except Exception as e:
            logger.error(f"Query existing {asset_type} failed: {e}")
            return set()

    def get_stock_date_range(self, code: str, frequency: str = "d") -> tuple:
        try:
            table_name = self._get_table_name(frequency, self.asset_type)
            res = self.con.execute(
                f"SELECT MIN(date), MAX(date) FROM {table_name} WHERE code = ?",
                [code],
            ).fetchone()
            return res if res else (None, None)
        except Exception as e:
            logger.error(f"Query date range failed: {e}")
            return (None, None)

    def get_missing_date_ranges(self, code: str, start_date: str, end_date: str, frequency: str = "d") -> list:
        try:
            start = pd.to_datetime(start_date).date()
            end = pd.to_datetime(end_date).date()
            table_name = self._get_table_name(frequency, self.asset_type)
            res = self.con.execute(
                f"SELECT date FROM {table_name} WHERE code = ? AND date BETWEEN ? AND ? ORDER BY date",
                [code, start_date, end_date],
            ).fetchall()

            existing_dates = {pd.to_datetime(row[0]).date() for row in res}
            if frequency == "d":
                all_dates = pd.date_range(start=start, end=end).date.tolist()
            elif frequency == "w":
                all_dates = pd.date_range(start=start, end=end, freq="W-FRI").date.tolist()
            elif frequency == "m":
                all_dates = pd.date_range(start=start, end=end, freq="M").date.tolist()
            else:
                all_dates = pd.date_range(start=start, end=end).date.tolist()

            missing_dates = [date for date in all_dates if date not in existing_dates]
            if not missing_dates:
                return []

            ranges = []
            current_start = missing_dates[0]
            current_end = missing_dates[0]
            for date in missing_dates[1:]:
                if (date - current_end).days == 1:
                    current_end = date
                else:
                    ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
                    current_start = date
                    current_end = date

            ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
            return ranges
        except Exception as e:
            logger.error(f"Query missing date ranges failed: {e}")
            return [(start_date, end_date)]

    def get_last_date(self, stock_code: str = None, frequency: str = "d", asset_type: str = "stock"):
        if not self._ensure_asset_type(asset_type):
            return None
        try:
            table_name = self._get_table_name(frequency, asset_type)
            if stock_code is None:
                res = self.con.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
            else:
                res = self.con.execute(
                    f"SELECT MAX(date) FROM {table_name} WHERE code = ?",
                    [stock_code],
                ).fetchone()
            if res and res[0]:
                return res[0] if isinstance(res[0], str) else res[0].strftime("%Y-%m-%d")
            return None
        except Exception as e:
            logger.error(f"Query {asset_type} last date failed: {e}")
            return None

    def vacuum(self):
        logger.info("Running DuckDB checkpoint...")
        try:
            self.con.execute("FORCE CHECKPOINT")
            logger.info("Database checkpoint completed")
        except Exception as e:
            logger.error(f"Database maintenance failed: {e}")

    def save_asset_info(self, df: pd.DataFrame, asset_type: str = "stock") -> bool:
        if df.empty or not self._ensure_asset_type(asset_type):
            return False
        if "code" not in df.columns:
            return False

        try:
            table_name = INFO_TABLES[asset_type]
            df_save = df.copy()
            if "code_name" in df_save.columns:
                df_save = df_save[["code", "code_name"]].rename(columns={"code_name": "name"})
            elif "name" in df_save.columns:
                df_save = df_save[["code", "name"]]
            else:
                return False
            df_save["update_time"] = pd.Timestamp.now()
            self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df_save")
            self.con.commit()
            logger.info(f"Saved {len(df_save)} {asset_type} info rows")
            return True
        except Exception as e:
            logger.error(f"Save {asset_type} info failed: {e}")
            return False

    def merge_from_db(self, source_db_path: str, tables: list = None) -> bool:
        try:
            if not os.path.exists(source_db_path):
                logger.error(f"Source database does not exist: {source_db_path}")
                return False

            safe_print(f"Start merge from {source_db_path}, tables={tables}")
            source_con = duckdb.connect(source_db_path, read_only=True)
            source_table_names = [t[0] for t in source_con.execute("SHOW TABLES").fetchall()]
            target_table_names = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            tables = tables or source_table_names

            merged_count = 0
            for table in tables:
                try:
                    if table not in source_table_names or table not in target_table_names:
                        safe_print(f"Skip merge table {table}: missing in source or target")
                        continue

                    source_data = source_con.execute(f"SELECT * FROM {table}").fetchdf()
                    if source_data.empty:
                        continue

                    if table in MARKET_TABLES[self.asset_type].values():
                        source_data_clean = self._clean_data(source_data)
                    else:
                        target_columns = self._table_columns(table)
                        source_data_clean = source_data[[col for col in target_columns if col in source_data.columns]].copy()
                        for col in target_columns:
                            if col not in source_data_clean.columns:
                                if col == "updated_at":
                                    source_data_clean[col] = pd.Timestamp.now()
                                elif col == "update_time":
                                    source_data_clean[col] = pd.Timestamp.now()
                                else:
                                    source_data_clean[col] = None
                        source_data_clean = source_data_clean[target_columns]

                    columns = ", ".join(source_data_clean.columns)
                    self.con.execute(f"INSERT OR REPLACE INTO {table} ({columns}) SELECT {columns} FROM source_data_clean")
                    merged_count += len(source_data_clean)
                    safe_print(f"Merged {table}: {len(source_data_clean)} rows")
                except Exception as e:
                    safe_print(f"Merge table {table} failed: {e}")
                    logger.warning(f"Merge table {table} failed: {e}")

            source_con.close()
            self.con.commit()
            safe_print(f"Database merge completed: {merged_count} rows")
            return True
        except Exception as e:
            logger.error(f"Database merge failed: {e}")
            return False

    def export_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        output_file: str,
        frequency: str = "d",
        format: str = "csv",
        asset_type: str = "stock",
    ) -> bool:
        if not self._ensure_asset_type(asset_type):
            return False
        try:
            table_name = self._get_table_name(frequency, asset_type)
            if code:
                query = f"SELECT * FROM {table_name} WHERE code = ? AND date BETWEEN ? AND ? ORDER BY date"
                params = [code, start_date, end_date]
            else:
                query = f"SELECT * FROM {table_name} WHERE date BETWEEN ? AND ? ORDER BY code, date"
                params = [start_date, end_date]

            df = self.con.execute(query, params).df()
            if df.empty:
                logger.warning("No data to export")
                return False

            if format.lower() == "csv":
                df.to_csv(output_file, index=False, encoding="utf-8")
            elif format.lower() == "parquet":
                df.to_parquet(output_file, index=False)
            elif format.lower() == "json":
                df.to_json(output_file, orient="records", force_ascii=False)
            else:
                logger.error(f"Unsupported export format: {format}")
                return False
            return True
        except Exception as e:
            logger.error(f"Export data failed: {e}")
            return False

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()

        for col in MARKET_COLUMNS:
            if col not in df_copy.columns:
                if col in {"open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pctChg"}:
                    df_copy[col] = 0.0
                elif col == "tradestatus":
                    df_copy[col] = "1"
                elif col == "isST":
                    df_copy[col] = "0"
                else:
                    df_copy[col] = ""

        if "date" in df_copy.columns:
            df_copy["date"] = pd.to_datetime(df_copy["date"], errors="coerce")
            df_copy = df_copy[df_copy["date"].notna()]
            if not df_copy.empty:
                df_copy["date"] = df_copy["date"].dt.date

        numeric_cols = ["open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pctChg"]
        for col in numeric_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce").fillna(0)

        return df_copy[MARKET_COLUMNS]

    def close(self):
        try:
            self.con.close()
        except Exception:
            pass
