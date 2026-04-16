import duckdb
import pandas as pd
import os
from typing import Tuple
from config import DATABASE_PATH
from logger_config import setup_logger

logger = setup_logger("Database")

class DuckDBManager:
    def __init__(self, db_path=None):
        db_path = db_path or DATABASE_PATH
        self.db_path = db_path
        self.con = None
        self._connect()
        self._create_table()
        logger.info(f"✅ DuckDB 初始化完成 (文件: {db_path})")
    
    def _connect(self):
        """建立数据库连接"""
        try:
            self.con = duckdb.connect(self.db_path)
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def _reconnect(self):
        """重新连接数据库"""
        logger.info("尝试重新连接数据库...")
        try:
            if self.con:
                try:
                    self.con.close()
                except:
                    pass
            self._connect()
            logger.info("数据库重新连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库重新连接失败: {e}")
            return False

    def _create_table(self):
        """
        创建股票数据表
        支持不同时间粒度的数据
        """
        # 创建股票基本信息表
        stock_info_sql = """
        CREATE TABLE IF NOT EXISTS stock_info (
            code VARCHAR PRIMARY KEY,
            code_name VARCHAR,
            industry VARCHAR,
            market VARCHAR,
            list_date DATE,
            is_active BOOLEAN DEFAULT TRUE,
            last_update DATE
        )
        """
        self.con.execute(stock_info_sql)
        
        # 创建日线表
        daily_sql = """
        CREATE TABLE IF NOT EXISTS stock_daily (
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
            PRIMARY KEY (code, date),
            FOREIGN KEY (code) REFERENCES stock_info(code)
        )
        """
        self.con.execute(daily_sql)
        
        # 创建周线表
        weekly_sql = """
        CREATE TABLE IF NOT EXISTS stock_weekly (
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
            PRIMARY KEY (code, date),
            FOREIGN KEY (code) REFERENCES stock_info(code)
        )
        """
        self.con.execute(weekly_sql)
        
        # 创建月线表
        monthly_sql = """
        CREATE TABLE IF NOT EXISTS stock_monthly (
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
            PRIMARY KEY (code, date),
            FOREIGN KEY (code) REFERENCES stock_info(code)
        )
        """
        self.con.execute(monthly_sql)
        
        # ✅ 优化：手动创建索引以加速查询
        # 如果索引已存在，IGNORE 会避免报错
        try:
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_daily_code_date ON stock_daily (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_weekly_code_date ON stock_weekly (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_monthly_code_date ON stock_monthly (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_stock_info_industry ON stock_info (industry)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_stock_info_market ON stock_info (market)")
        except Exception as e:
            logger.warning(f"索引创建提示: {e}")

    def upload_df(self, df: pd.DataFrame, frequency: str = "d") -> bool:
        """单只股票写入（支持不同时间粒度）"""
        if df.empty: return False
        try:
            df_clean = self._clean_data(df)
            table_name = self._get_table_name(frequency)
            self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df_clean")
            return True
        except Exception as e:
            logger.error(f"❌ 写入失败: {e}")
            return False

    def upload_batch(self, df_list: list, frequency: str = "d") -> int:
        """
        🚀 批量写入优化（支持不同时间粒度）
        """
        if not df_list: return 0
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                combined_df = pd.concat(df_list, ignore_index=True)
                if combined_df.empty: return 0

                df_clean = self._clean_data(combined_df)
                count = len(df_clean)
                table_name = self._get_table_name(frequency)
                
                # 优化：使用DuckDB的COPY命令进行更高效的批量导入
                # 对于大型DataFrame，COPY命令比INSERT更高效
                if count > 1000:
                    # 对于大型数据，使用COPY命令
                    temp_table = f"temp_{table_name}"
                    self.con.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0")
                    self.con.execute(f"INSERT INTO {temp_table} SELECT * FROM df_clean")
                    self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {temp_table}")
                    self.con.execute(f"DROP TABLE {temp_table}")
                else:
                    # 对于小型数据，使用常规INSERT
                    self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df_clean")
                
                logger.info(f"💾 批量写入完成: {count} 条记录 (表: {table_name})")
                return count
                
            except Exception as e:
                logger.error(f"❌ 批量写入失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # 尝试重新连接
                    if self._reconnect():
                        continue
                return 0
    
    def _get_table_name(self, frequency: str) -> str:
        """
        根据时间粒度获取表名
        """
        frequency_map = {
            "d": "stock_daily",
            "w": "stock_weekly",
            "m": "stock_monthly"
        }
        return frequency_map.get(frequency, "stock_daily")

    def get_finished_stocks(self, frequency: str = "d") -> set:
        """
        🛡️ 断点续传支持 - 获取已完成的股票代码
        """
        try:
            table_name = self._get_table_name(frequency)
            res = self.con.execute(f"SELECT DISTINCT code FROM {table_name}").fetchall()
            return {row[0] for row in res}
        except Exception as e:
            logger.error(f"查询已存在股票失败: {e}")
            return set()

    def get_stock_date_range(self, code: str, frequency: str = "d") -> tuple:
        """
        📅 获取股票的日期范围
        返回 (最早日期, 最晚日期)
        """
        try:
            table_name = self._get_table_name(frequency)
            res = self.con.execute(
                f"SELECT MIN(date), MAX(date) FROM {table_name} WHERE code = ?",
                [code]
            ).fetchone()
            return res if res else (None, None)
        except Exception as e:
            logger.error(f"查询股票日期范围失败: {e}")
            return (None, None)

    def get_missing_date_ranges(self, code: str, start_date: str, end_date: str, frequency: str = "d") -> list:
        """
        🔍 获取缺失的日期范围
        返回需要下载的日期范围列表
        """
        try:
            # 转换为日期对象
            start = pd.to_datetime(start_date).date()
            end = pd.to_datetime(end_date).date()
            
            # 获取数据库中已有的日期
            table_name = self._get_table_name(frequency)
            res = self.con.execute(
                f"SELECT date FROM {table_name} WHERE code = ? AND date BETWEEN ? AND ? ORDER BY date",
                [code, start_date, end_date]
            ).fetchall()
            
            existing_dates = {pd.to_datetime(row[0]).date() for row in res}
            
            # 生成完整的日期序列
            if frequency == "d":
                all_dates = pd.date_range(start=start, end=end).date.tolist()
            elif frequency == "w":
                # 周线数据，按周生成日期
                all_dates = pd.date_range(start=start, end=end, freq='W-FRI').date.tolist()
            elif frequency == "m":
                # 月线数据，按月生成日期
                all_dates = pd.date_range(start=start, end=end, freq='M').date.tolist()
            else:
                # 默认日线
                all_dates = pd.date_range(start=start, end=end).date.tolist()
            
            # 找出缺失的日期
            missing_dates = [date for date in all_dates if date not in existing_dates]
            
            # 将缺失的日期合并为连续的范围
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
            logger.error(f"🔍 获取缺失日期范围失败 {code}: {e}")
            return []

    def get_table_status(self, frequency: str):
        """
        获取指定频率数据表的总记录数和股票数量
        返回 (total_records, distinct_stocks)
        """
        table_name = self._get_table_name(frequency)
        try:
            record_count_res = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            stock_count_res = self.con.execute(f"SELECT COUNT(DISTINCT code) FROM {table_name}").fetchone()
            record_count = record_count_res[0] if record_count_res else 0
            stock_count = stock_count_res[0] if stock_count_res else 0
            return record_count, stock_count
        except Exception as e:
            logger.warning(f"获取表 {table_name} 状态失败: {e}")
            return 0, 0

    def drop_table(self, frequency: str):
        """
        删除指定频率的数据表
        """
        table_name = self._get_table_name(frequency)
        try:
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"已删除表: {table_name}")
        except Exception as e:
            logger.error(f"删除表 {table_name} 失败: {e}")

    def clear_all_tables(self):
        """
        删除所有数据表
        """
        # 先删除有外键依赖的表
        frequencies = ["d", "w", "m"]
        for freq in frequencies:
            self.drop_table(freq)
        # 再删除股票基本信息表
        try:
            self.con.execute("DROP TABLE IF EXISTS stock_info")
            logger.info("已删除表: stock_info")
        except Exception as e:
            logger.error(f"删除表 stock_info 失败: {e}")
        logger.info("所有数据表已清理。")

    def upsert_stock_info(self, code: str, code_name: str, industry: str = None, market: str = None, list_date: str = None):
        """
        插入或更新股票基本信息
        
        Args:
            code: 股票代码
            code_name: 股票名称
            industry: 所属行业
            market: 所属市场
            list_date: 上市日期
        """
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # 检查股票是否已存在
                exists = self.con.execute("SELECT 1 FROM stock_info WHERE code = ?", [code]).fetchone() is not None
                
                if exists:
                    # 更新现有股票信息
                    self.con.execute("""
                        UPDATE stock_info 
                        SET code_name = ?, industry = ?, market = ?, list_date = ?, last_update = CURRENT_DATE
                        WHERE code = ?
                    """, [code_name, industry, market, list_date, code])
                else:
                    # 插入新股票信息
                    self.con.execute("""
                        INSERT INTO stock_info (code, code_name, industry, market, list_date, last_update)
                        VALUES (?, ?, ?, ?, ?, CURRENT_DATE)
                    """, [code, code_name, industry, market, list_date])
                return True
            except Exception as e:
                logger.error(f"更新股票信息失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # 尝试重新连接
                    if self._reconnect():
                        continue
                return False

    def get_stock_info(self, code: str):
        """
        获取股票基本信息
        
        Args:
            code: 股票代码
            
        Returns:
            股票基本信息字典
        """
        try:
            res = self.con.execute("SELECT * FROM stock_info WHERE code = ?", [code]).fetchone()
            if res:
                return {
                    'code': res[0],
                    'code_name': res[1],
                    'industry': res[2],
                    'market': res[3],
                    'list_date': res[4],
                    'is_active': res[5],
                    'last_update': res[6]
                }
            return None
        except Exception as e:
            logger.error(f"获取股票信息失败: {e}")
            return None

    def get_all_stocks(self):
        """
        获取所有股票列表
        
        Returns:
            股票列表
        """
        try:
            res = self.con.execute("SELECT code, code_name FROM stock_info ORDER BY code").fetchall()
            return res
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []


    def get_last_date(self, stock_code: str, frequency: str = "d"):
        """获取某只股票在数据库中的最后交易日"""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                table_name = self._get_table_name(frequency)
                res = self.con.execute(
                    f"SELECT MAX(date) FROM {table_name} WHERE code = ?", 
                    [stock_code]
                ).fetchone()
                if res and res[0]:
                    # 确保返回字符串格式
                    if isinstance(res[0], str):
                        return res[0]
                    else:
                        return res[0].strftime("%Y-%m-%d")
                return None
            except Exception as e:
                logger.error(f"查询最后日期失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # 尝试重新连接
                    if self._reconnect():
                        continue
                return None

    def vacuum(self):
        """压缩数据库，释放空间"""
        logger.info("🧹 正在执行数据库维护...")
        try:
            self.con.execute("FORCE CHECKPOINT")
            logger.info("✅ 数据库维护完成")
        except Exception as e:
            logger.error(f"维护失败: {e}")
            
    def export_data(self, code: str, start_date: str, end_date: str, output_file: str, frequency: str = "d", format: str = "csv") -> bool:
        """
        📤 导出数据
        
        Args:
            code: 股票代码 (空字符串表示所有股票)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            output_file: 输出文件路径
            frequency: 数据频率 (d: 日线, w: 周线, m: 月线)
            format: 输出格式 (csv, parquet, json)
            
        Returns:
            是否导出成功
        """
        try:
            table_name = self._get_table_name(frequency)
            
            # 构建查询语句
            if code:
                query = f"SELECT * FROM {table_name} WHERE code = ? AND date BETWEEN ? AND ? ORDER BY date"
                params = [code, start_date, end_date]
            else:
                query = f"SELECT * FROM {table_name} WHERE date BETWEEN ? AND ? ORDER BY code, date"
                params = [start_date, end_date]
            
            # 执行查询
            df = self.con.execute(query, params).df()
            
            if df.empty:
                logger.warning("⚠️ 没有数据可导出")
                return False
            
            # 导出数据
            if format.lower() == "csv":
                df.to_csv(output_file, index=False, encoding='utf-8')
            elif format.lower() == "parquet":
                df.to_parquet(output_file, index=False)
            elif format.lower() == "json":
                df.to_json(output_file, orient='records', force_ascii=False)
            else:
                logger.error(f"❌ 不支持的导出格式: {format}")
                return False
            
            logger.info(f"✅ 数据导出成功: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 导出数据失败: {e}")
            return False

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗：类型转换 + 列顺序重排"""
        df_copy = df.copy()
        if 'date' in df_copy.columns:
            df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
        
        target_columns = [
            'code', 'date', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'
        ]
        return df_copy[target_columns]

    def close(self):
        self.con.close()