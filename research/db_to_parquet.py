import sqlite3
import pandas as pd
import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq

# 配置 logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sqlite_to_parquet(db_path, table_name, output_path, partition_cols=None, chunksize=100000, order_by=None):
    """
    将 SQLite 数据库中的表导出为 Parquet 文件。
    支持按指定列（一个或多个）进行分区导出。
    支持分批读取以避免 OOM。
    支持指定排序字段。
    
    :param db_path: SQLite 数据库文件路径
    :param table_name: 要导出的表名
    :param output_path: 输出路径 (如果分区，则为目录；如果不分区，则为文件路径)
    :param partition_cols: 用于分区的列名列表 (例如 ['symbol', 'date']) 或逗号分隔字符串
    :param chunksize: 每次读取的行数，默认为 100,000
    :param order_by: 排序字段，列表或逗号分隔字符串 (例如 'ts' 或 ['symbol', 'ts'])
    """
    # 检查数据库文件是否存在
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    # 连接到 SQLite 数据库
    conn = sqlite3.connect(db_path)
    
    try:
        # 处理排序参数
        order_clause = ""
        if order_by:
            if isinstance(order_by, str):
                order_by = [c.strip() for c in order_by.split(',')]
            if order_by:
                order_clause = f" ORDER BY {', '.join(order_by)}"

        if partition_cols:
            # 处理分区列参数，支持列表或逗号分隔字符串
            if isinstance(partition_cols, str):
                partition_cols = [c.strip() for c in partition_cols.split(',')]
            
            logging.info(f"Starting partitioned dump by {partition_cols}...")
            
            # 1. 获取所有不重复的分区键值组合
            cols_str = ", ".join(partition_cols)
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT {cols_str} FROM {table_name}")
            distinct_combinations = cursor.fetchall()
            logging.info(f"Found {len(distinct_combinations)} distinct combinations.")

            # 确保输出目录存在
            os.makedirs(output_path, exist_ok=True)

            total_rows = 0
            for combo in distinct_combinations:
                # combo 是一个元组，例如 ('BTCUSDT', '2023-01-01')
                # 如果只有一个字段，它也是元组 ('BTCUSDT',)
                if not isinstance(combo, tuple):
                    combo = (combo,)
                
                # 2. 构建查询条件和路径
                conditions = []
                params = []
                path_parts = []
                
                for col, val in zip(partition_cols, combo):
                    conditions.append(f"{col} = ?")
                    params.append(val)
                    # 构建 Hive 风格路径部分: col=val
                    path_parts.append(f"{col}={val}")
                
                where_clause = " AND ".join(conditions)
                # 组合查询语句，包含排序
                query = f"SELECT * FROM {table_name} WHERE {where_clause}{order_clause}"
                
                # 4. 构建分区路径
                # output_path/col1=val1/col2=val2/data.parquet
                partition_dir = os.path.join(output_path, *path_parts)
                os.makedirs(partition_dir, exist_ok=True)
                file_path = os.path.join(partition_dir, "data.parquet")
                
                # 分批读取并写入
                writer = None
                part_rows = 0
                for chunk in pd.read_sql_query(query, conn, params=params, chunksize=chunksize):
                    if chunk.empty:
                        continue
                        
                    table = pa.Table.from_pandas(chunk)
                    if writer is None:
                        writer = pq.ParquetWriter(file_path, table.schema)
                    writer.write_table(table)
                    part_rows += len(chunk)
                
                if writer:
                    writer.close()
                
                total_rows += part_rows
                logging.info(f"  -> Dumped {path_parts}: {part_rows} rows")
            
            logging.info(f"Partitioned dump finished. Total rows: {total_rows}")

        else:
            # --- 全量导出模式 ---
            logging.info(f"Starting full dump of table '{table_name}'...")
            
            # 确保父目录存在
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            
            query = f"SELECT * FROM {table_name}{order_clause}"
            
            writer = None
            total_rows = 0
            # 分批读取
            for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
                if chunk.empty:
                    continue
                
                table = pa.Table.from_pandas(chunk)
                # 初始化 writer (仅在第一个 chunk)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema)
                
                writer.write_table(table)
                total_rows += len(chunk)
                logging.info(f"  -> Processed chunk: {len(chunk)} rows")
            
            if writer:
                writer.close()
            
            logging.info(f"Successfully dumped table '{table_name}' to {output_path}")
            logging.info(f"Total rows processed: {total_rows}")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Dump SQLite table to Parquet file")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the SQLite database file")
    parser.add_argument("--table_name", type=str, required=True, help="Name of the table to export")
    parser.add_argument("--output_path", type=str, required=True, help="Path to the output Parquet file or directory")
    parser.add_argument("--partition_cols", type=str, help="Comma-separated columns to partition by (e.g. 'symbol,date'). If set, output_path must be a directory.")
    parser.add_argument("--chunksize", type=int, default=100000, help="Rows per chunk to read/write")
    parser.add_argument("--order_by", type=str, help="Comma-separated columns to sort by (e.g. 'ts')")
    
    args = parser.parse_args()
    if args.db_path is None or args.table_name is None or args.output_path is None:
        parser.print_help()
        sys.exit(1)

    sqlite_to_parquet(args.db_path, args.table_name, args.output_path, args.partition_cols, args.chunksize, args.order_by)