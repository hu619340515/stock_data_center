import duckdb
import pandas as pd
import os
from config import DATABASE_PATH
from logger_config import setup_logger

logger = setup_logger("Database")

def safe_print(msg):
    """安全打印函数"""
    print(msg)

class DuckDBManager:
    def __init__(self, db_path=None):
        db_path = db_path or DATABASE_PATH
        self.con = duckdb.connect(db_path)
        self._create_table()
        logger.info(f"✅ DuckDB 初始化完成 (文件: {db_path})")

    def _create_table(self):
        """
        创建股票数据表
        支持不同时间粒度的数据
        """
        # 创建日线表
        daily_sql = """
        CREATE TABLE IF NOT EXISTS stock_daily (
            code VARCHAR,
            name VARCHAR,
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
        """
        self.con.execute(daily_sql)
        # 兼容已有表，添加name字段（如果不存在）
        self.con.execute("ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS name VARCHAR")
        
        # 创建周线表
        weekly_sql = """
        CREATE TABLE IF NOT EXISTS stock_weekly (
            code VARCHAR,
            name VARCHAR,
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
        """
        self.con.execute(weekly_sql)
        # 兼容已有表，添加name字段（如果不存在）
        self.con.execute("ALTER TABLE stock_weekly ADD COLUMN IF NOT EXISTS name VARCHAR")
        
        # 创建月线表
        monthly_sql = """
        CREATE TABLE IF NOT EXISTS stock_monthly (
            code VARCHAR,
            name VARCHAR,
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
        """
        self.con.execute(monthly_sql)
        # 兼容已有表，添加name字段（如果不存在）
        self.con.execute("ALTER TABLE stock_monthly ADD COLUMN IF NOT EXISTS name VARCHAR")
        
        # 创建股票基本信息表
        stock_info_sql = """
        CREATE TABLE IF NOT EXISTS stock_info (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.con.execute(stock_info_sql)
        
        # 创建ETF基本信息表
        etf_info_sql = """
        CREATE TABLE IF NOT EXISTS etf_info (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.con.execute(etf_info_sql)
        
        # ✅ 优化：手动创建索引以加速查询
        # 如果索引已存在，IGNORE 会避免报错
        # 创建ETF日线表
        etf_daily_sql = """
        CREATE TABLE IF NOT EXISTS etf_daily (
            code VARCHAR,
            name VARCHAR,
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
        """
        self.con.execute(etf_daily_sql)
        # 兼容已有表，添加name字段（如果不存在）
        self.con.execute("ALTER TABLE etf_daily ADD COLUMN IF NOT EXISTS name VARCHAR")
        
        # 创建ETF周线表
        etf_weekly_sql = """
        CREATE TABLE IF NOT EXISTS etf_weekly (
            code VARCHAR,
            name VARCHAR,
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
        """
        self.con.execute(etf_weekly_sql)
        # 兼容已有表，添加name字段（如果不存在）
        self.con.execute("ALTER TABLE etf_weekly ADD COLUMN IF NOT EXISTS name VARCHAR")
        
        # 创建ETF月线表
        etf_monthly_sql = """
        CREATE TABLE IF NOT EXISTS etf_monthly (
            code VARCHAR,
            name VARCHAR,
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
        """
        self.con.execute(etf_monthly_sql)
        # 兼容已有表，添加name字段（如果不存在）
        self.con.execute("ALTER TABLE etf_monthly ADD COLUMN IF NOT EXISTS name VARCHAR")
        
        # ✅ 优化：手动创建索引以加速查询
        # 如果索引已存在，IGNORE 会避免报错
        try:
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_daily_code_date ON stock_daily (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_weekly_code_date ON stock_weekly (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_monthly_code_date ON stock_monthly (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_etf_daily_code_date ON etf_daily (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_etf_weekly_code_date ON etf_weekly (code, date)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_etf_monthly_code_date ON etf_monthly (code, date)")
        except Exception as e:
            logger.warning(f"索引创建提示: {e}")

    def upload_df(self, df: pd.DataFrame, frequency: str = "d", asset_type: str = "stock") -> bool:
        """单只股票/ETF写入（支持不同时间粒度）"""
        if df.empty: return False
        try:
            df_clean = self._clean_data(df)
            table_name = self._get_table_name(frequency, asset_type)
            self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df_clean")
            return True
        except Exception as e:
            logger.error(f"❌ 写入失败: {e}")
            return False

    def upload_batch(self, df_list: list, frequency: str = "d", asset_type: str = "stock") -> int:
        """
        🚀 批量写入优化（支持不同时间粒度和资产类型）
        """
        if not df_list: 
            safe_print(f"⚠️ upload_batch 空列表 - frequency={frequency}, asset_type={asset_type}")
            return 0
        
        try:
            # 检查所有DataFrame的列顺序是否一致
            target_columns = [
                'code', 'name', 'date', 'open', 'high', 'low', 'close', 'preclose', 
                'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'
            ]
            
            # 强制每个DataFrame的列顺序一致
            aligned_dfs = []
            for i, df in enumerate(df_list):
                if df is not None and not df.empty:
                    # 检查列顺序是否正确
                    if list(df.columns) != target_columns:
                        logger.warning(f"⚠️ DataFrame {i} 列顺序不一致，正在调整: {df.columns.tolist()}")
                    # 确保所有必需的列都存在
                    for col in target_columns:
                        if col not in df.columns:
                            if col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']:
                                df[col] = 0.0
                            elif col in ['adjustflag']:
                                df[col] = ""
                            elif col in ['tradestatus']:
                                df[col] = "1"
                            elif col in ['isST']:
                                df[col] = "0"
                            elif col in ['code', 'name', 'date']:
                                df[col] = ""
                    # 强制列顺序
                    aligned_df = df[target_columns]
                    aligned_dfs.append(aligned_df)
            
            combined_df = pd.concat(aligned_dfs, ignore_index=True) if aligned_dfs else pd.DataFrame()
            if combined_df.empty: 
                safe_print(f"⚠️ upload_batch 空数据框 - frequency={frequency}, asset_type={asset_type}")
                return 0

            # 检查合并后的列顺序
            if list(combined_df.columns) != target_columns:
                logger.error(f"❌ 合并后列顺序错误: {combined_df.columns.tolist()}")
                # 强制重新排序
                combined_df = combined_df[target_columns]

            df_clean = self._clean_data(combined_df)
            count = len(df_clean)
            table_name = self._get_table_name(frequency, asset_type)
            safe_print(f"📥 upload_batch - 准备写入 {count} 条记录到 {table_name}")
            
            # 调试：检查df_clean的前几行数据
            if not df_clean.empty:
                logger.debug(f"📊 df_clean前3行数据:")
                logger.debug(f"列顺序: {df_clean.columns.tolist()}")
                for i in range(min(3, len(df_clean))):
                    row_dict = df_clean.iloc[i].to_dict()
                    logger.debug(f"行{i}: {row_dict}")
            
            # 优化：使用DuckDB的COPY命令进行更高效的批量导入
            # 对于大型DataFrame，COPY命令比INSERT更高效
            if count > 1000:
                # 对于大型数据，使用COPY命令
                temp_table = f"temp_{table_name}"
                # 先删除可能存在的残留临时表，避免冲突
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
                self.con.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0")
                # 显式指定列名
                columns_str = ', '.join(df_clean.columns.tolist())
                self.con.execute(f"INSERT INTO {temp_table} ({columns_str}) SELECT {columns_str} FROM df_clean")
                self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {temp_table}")
                self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
            else:
                # 对于小型数据，使用常规INSERT
                # 显式指定列名
                columns_str = ', '.join(df_clean.columns.tolist())
                self.con.execute(f"INSERT OR REPLACE INTO {table_name} ({columns_str}) SELECT {columns_str} FROM df_clean")
            
            # 显式提交事务，确保数据持久化到磁盘
            self.con.commit()
            
            logger.info(f"💾 批量写入完成: {count} 条记录 (表: {table_name})")
            return count
            
        except Exception as e:
            logger.error(f"❌ 批量写入失败: {e}")
            return 0
    
    def _get_table_name(self, frequency: str, asset_type: str = "stock") -> str:
        """
        根据时间粒度和资产类型获取表名
        """
        if asset_type == "etf":
            frequency_map = {
                "d": "etf_daily",
                "w": "etf_weekly",
                "m": "etf_monthly"
            }
            return frequency_map.get(frequency, "etf_daily")
        else:
            frequency_map = {
                "d": "stock_daily",
                "w": "stock_weekly",
                "m": "stock_monthly"
            }
            return frequency_map.get(frequency, "stock_daily")

    def get_finished_stocks(self, frequency: str = "d", asset_type: str = "stock") -> set:
        """
        🛡️ 断点续传支持 - 获取已完成的股票/ETF代码
        """
        try:
            table_name = self._get_table_name(frequency, asset_type)
            res = self.con.execute(f"SELECT DISTINCT code FROM {table_name}").fetchall()
            return {row[0] for row in res}
        except Exception as e:
            logger.error(f"查询已存在{asset_type}失败: {e}")
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
            logger.error(f"查询缺失日期范围失败: {e}")
            return [(start_date, end_date)]

    def get_last_date(self, stock_code: str = None, frequency: str = "d", asset_type: str = "stock"):
        """获取某只股票/ETF在数据库中的最后交易日，如果stock_code为None则获取整个表的最新日期"""
        try:
            table_name = self._get_table_name(frequency, asset_type)
            if stock_code is None:
                res = self.con.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
            else:
                res = self.con.execute(
                    f"SELECT MAX(date) FROM {table_name} WHERE code = ?", 
                    [stock_code]
                ).fetchone()
            if res[0]:
                # 确保返回字符串格式
                if isinstance(res[0], str):
                    return res[0]
                else:
                    return res[0].strftime("%Y-%m-%d")
            return None
        except Exception as e:
            logger.error(f"查询{asset_type}最后日期失败: {e}")
            return None

    def vacuum(self):
        """压缩数据库，释放空间"""
        logger.info("🧹 正在执行数据库维护...")
        try:
            self.con.execute("FORCE CHECKPOINT")
            logger.info("✅ 数据库维护完成")
        except Exception as e:
            logger.error(f"维护失败: {e}")
    
    def save_asset_info(self, df: pd.DataFrame, asset_type: str = "stock") -> bool:
        """
        保存股票/ETF基本信息（代码和名称）
        """
        if df.empty or 'code' not in df.columns or 'code_name' not in df.columns:
            return False
        
        try:
            table_name = "stock_info" if asset_type == "stock" else "etf_info"
            # 准备数据
            df_save = df[['code', 'code_name']].copy()
            df_save = df_save.rename(columns={'code_name': 'name'})
            df_save['update_time'] = pd.Timestamp.now()
            
            # 插入或替换
            self.con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df_save")
            logger.info(f"✅ 保存{len(df_save)}条{asset_type}基本信息成功")
            return True
        except Exception as e:
            logger.error(f"❌ 保存{asset_type}基本信息失败: {e}")
            return False
            
    def merge_from_db(self, source_db_path: str, tables: list = None) -> bool:
        """
        🔗 从源数据库合并数据到当前数据库
        
        Args:
            source_db_path: 源数据库路径
            tables: 要合并的表列表，None表示合并所有表
            
        Returns:
            是否合并成功
        """
        try:
            if not os.path.exists(source_db_path):
                logger.error(f"源数据库不存在: {source_db_path}")
                return False
            
            safe_print(f"🔍 开始合并 - 源数据库: {source_db_path}, 要合并的表: {tables}")
            
            # 连接源数据库（只读）
            source_con = duckdb.connect(source_db_path, read_only=True)
            
            # 获取源数据库中所有表
            all_source_tables = source_con.execute("SHOW TABLES").fetchall()
            source_table_names = [t[0] for t in all_source_tables]
            safe_print(f"📋 源数据库中的表: {source_table_names}")
            
            # 获取要合并的表
            if tables is None:
                tables = source_table_names
            
            safe_print(f"🔄 准备合并的表: {tables}")
            
            merged_count = 0
            for table in tables:
                try:
                    # 检查源表是否存在
                    if table not in source_table_names:
                        safe_print(f"⚠️ 源表 {table} 不存在于源数据库中")
                        continue
                    
                    # 获取源表数据
                    source_data = source_con.execute(f"SELECT * FROM {table}").fetchdf()
                    safe_print(f"📊 表 {table} 的数据量: {len(source_data)} 条")
                    
                    if source_data.empty:
                        continue
                    
                    # 检查目标表是否存在
                    target_tables = self.con.execute("SHOW TABLES").fetchall()
                    target_table_names = [t[0] for t in target_tables]
                    if table not in target_table_names:
                        safe_print(f"⚠️ 目标表 {table} 不存在于目标数据库中")
                        continue
                    
                    # 数据清洗和类型转换，确保和目标表类型一致
                    try:
                        source_data_clean = self._clean_data(source_data)
                        safe_print(f"✅ 数据清洗完成，列数: {len(source_data_clean.columns)}")
                    except Exception as e:
                        safe_print(f"❌ 数据清洗失败: {e}，尝试直接插入")
                        source_data_clean = source_data
                    
                    # 插入或替换到当前数据库，显式指定列名，按列名匹配避免错位
                    columns = ', '.join(source_data_clean.columns)
                    self.con.execute(f"INSERT OR REPLACE INTO {table} ({columns}) SELECT {columns} FROM source_data_clean")
                    merged_count += len(source_data)
                    safe_print(f"✅ 合并表 {table}: {len(source_data)} 条记录")
                except Exception as e:
                    safe_print(f"❌ 合并表 {table} 失败: {e}")
                    logger.warning(f"合并表 {table} 失败: {e}")
            
            source_con.close()
            safe_print(f"✅ 数据库合并完成，共合并 {merged_count} 条记录")
            logger.info(f"✅ 数据库合并完成，共合并 {merged_count} 条记录")
            return True
            
        except Exception as e:
            safe_print(f"❌ 数据库合并失败: {e}")
            logger.error(f"数据库合并失败: {e}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        try:
            self.con.close()
        except Exception:
            pass
            
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
        
        # 确保所有必需的列都存在
        required_columns = [
            'code', 'name', 'date', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'
        ]
        
        # 添加缺失的列
        for col in required_columns:
            if col not in df_copy.columns:
                if col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']:
                    df_copy[col] = 0.0
                elif col in ['adjustflag']:
                    df_copy[col] = ""
                elif col in ['tradestatus']:
                    df_copy[col] = "1"
                elif col in ['isST']:
                    df_copy[col] = "0"
                elif col in ['code', 'name', 'date']:
                    df_copy[col] = ""
        
        # 日期字段处理：确保是日期格式
        if 'date' in df_copy.columns:
            # 检查date列的数据类型
            logger.debug(f"📊 date列原始类型: {df_copy['date'].dtype}")
            # 先尝试转换，无效的日期会变成NaT
            df_copy['date'] = pd.to_datetime(df_copy['date'], errors='coerce')
            # 过滤无效日期的行
            valid_mask = df_copy['date'].notna()
            invalid_count = len(df_copy) - valid_mask.sum()
            if invalid_count > 0:
                logger.warning(f"⚠️ 过滤 {invalid_count} 条无效日期数据")
            df_copy = df_copy[valid_mask]
            if not df_copy.empty:
                df_copy['date'] = df_copy['date'].dt.date
                logger.debug(f"📊 date列转换后类型: {df_copy['date'].dtype}")
        
        # 数字字段处理
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df_copy.columns:
                logger.debug(f"📊 {col}列原始类型: {df_copy[col].dtype}")
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
                logger.debug(f"📊 {col}列转换后类型: {df_copy[col].dtype}")
        
        # 检查列顺序
        if list(df_copy.columns) != required_columns:
            logger.warning(f"⚠️ _clean_data 列顺序不一致: {df_copy.columns.tolist()}")
        
        # 强制按照目标列顺序重排
        result = df_copy[required_columns]
        
        # 验证最终列顺序
        if list(result.columns) != required_columns:
            logger.error(f"❌ _clean_data 最终列顺序错误: {result.columns.tolist()}")
        
        return result

    def close(self):
        self.con.close()