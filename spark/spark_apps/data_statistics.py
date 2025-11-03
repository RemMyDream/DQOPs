import sys
from pyspark.sql import SparkSession
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), "../..")) 
from utils.postgresql_client import PostgresSQLClient
from sqlalchemy import create_engine
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
import logging
from sqlalchemy import text
import yaml
from minio import Minio

# Logger
def create_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(name)
    return logger

def load_cfg(cfg_file):
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)
    return cfg

logger = create_logger(name = "Ingest_to_bronze_layer")

def psql_client(database:str, host:str, user: str = "postgres", password:str = "postgres") -> PostgresSQLClient: 
    try:
        pc = PostgresSQLClient(
            database=database,
            user=user,
            password=password,
            host=host
        )
        logger.info("Create PostgreSQLClient successfully")
        return pc
    except Exception as e:
        logger.error(f"Error when creating PostgreSQLClient: {e}")
        raise

# Demo ingest data from BE to Postgre
def drop_table(pc):
    query0 = "DROP TABLE IF EXISTS public.orders"
    pc.execute_query(query0)

def create_table(pc):
    pc.execute_query("""CREATE TABLE IF NOT EXISTS public.orders (
    order_id       INT NOT NULL,                      
    user_id        INT NOT NULL,                         
    status         VARCHAR(20),
    gender         VARCHAR(5),    
    created_at     TIMESTAMP NOT NULL,           
    returned_at    TIMESTAMP NULL,   
    shipped_at     TIMESTAMP NULL,  
    delivered_at   TIMESTAMP NULL, 
    num_of_item    INT NOT NULL CHECK (num_of_item > 0))""")

def insert_table(pc:PostgresSQLClient):
    df = pd.read_csv(r"C:\Users\Chien\Documents\Project VDT\orders.csv", sep = ',')

    with pc.engine.begin() as conn:
        df.to_sql(
            name="orders",
            con=conn,
            schema="public", 
            if_exists='append', 
            index=False
        )

def check_table_demo(pc):
    query1 = "SELECT * FROM public.orders LIMIT 200"
    query2 = "SELECT COUNT(*) FROM public.orders"
    with pc.engine.connect() as conn:
        df1 = pd.read_sql_query(query1, conn)
        print(df1)
        df2 = pd.read_sql_query(query2, conn)
        print(df2)

# Metadata
def read_table_info(table_name, pc, schema = "public"):
    with pc.engine.connect() as conn:
        df = pd.read_sql_query(f"""
            SELECT 
                table_catalog,
                table_schema,
                table_name,
                column_name, 
                data_type, 
                character_maximum_length,
                is_nullable,
                column_default,
                is_updatable
            FROM information_schema.columns
            WHERE table_name = '{table_name}' AND table_schema = '{schema}'
            ORDER BY ordinal_position;
        """, con=conn)
        return df

def read_constraint_info(table_name, pc, schema = "public"):
    with engine.connect() as conn:
        df = pd.read_sql_query(f"""SELECT 
                                constraint_catalog,
                                constraint_schema,
                                constraint_name,
                                column_name
            FROM information_schema.key_column_usage
            WHERE table_name = '{table_name}' AND table_schema = '{schema}'
            ORDER BY ordinal_position;
        """, con=conn)
        return df

pc = psql_client("orders", host = "localhost")
# drop_table(pc)
# create_table(pc)
# insert_table(pc)

pc = psql_client("orders", host = "localhost")

def get_table_statistics(pc: PostgresSQLClient, table_name: str, schema='public'):
    query = f'SELECT COUNT(1) FROM "{schema}"."{table_name}";'
    total_row = pc.execute_query(query).scalar()
    total_col = pc.execute_query(f"""SELECT COUNT(*) AS column_count
                                 FROM information_schema.columns
                                 WHERE table_schema = '{schema}'
                                 AND table_name = '{table_name}';""").scalar()
    return total_row, total_col

def get_null_properties(
    pc: 'PostgresSQLClient',
    table_name: str,
    schema: str = 'public',
    columns: list[str] | None = None
):
    
    all_cols = pc.get_columns(table_name=table_name, schema=schema)
    if not all_cols:
        return pd.DataFrame()

    if columns is not None:
        cols = [c for c in columns if c in all_cols]
        if not cols:
            raise ValueError(f"Columns '{cols}' does not exist in table '{table_name}'.")
    else:
        cols = all_cols

    null_counts_expr = ", ".join([
        f"COUNT(*) FILTER (WHERE \"{c}\" IS NULL) AS \"{c}\""
        for c in cols
    ])
    query = f"""
        SELECT COUNT(*) AS total_rows, {null_counts_expr}
        FROM "{schema}"."{table_name}";
    """

    result = pc.execute_query(query).fetchone()
    total_rows = result[0]
    null_counts = result[1:]

    data = []
    for col, null_count in zip(cols, null_counts):
        null_count = null_count or 0
        null_percent = null_count / total_rows if total_rows > 0 else 0
        not_null_count = total_rows - null_count
        not_null_percent = 1 - null_percent

        data.append({
            'column': col,
            'nulls_count': null_count,
            'nulls_percent': null_percent,
            'not_nulls_count': not_null_count,
            'not_nulls_percent': not_null_percent
        })
    
    return pd.DataFrame(data)

def get_uniqueness_properties(
    pc: 'PostgresSQLClient',
    table_name: str,
    schema: str = 'public',
    columns: list[str] | None = None
):
    all_cols = pc.get_columns(table_name=table_name, schema=schema)
    if not all_cols:
        return pd.DataFrame() 

    if columns is not None:
        cols = [c for c in columns if c in all_cols]
        if not cols:
            raise ValueError(f"Columns '{cols}' does not exist in table '{table_name}'.")
    else:
        cols = all_cols

    distinct_counts_expr = ", ".join([f"COUNT(DISTINCT \"{c}\") AS \"{c}\"" for c in cols])
    query = f"""
        SELECT COUNT(*) AS total_rows, {distinct_counts_expr}
        FROM "{schema}"."{table_name}";
    """
    result = pc.execute_query(query).fetchone()  
    total_rows = result[0]
    distinct_counts = result[1:]

    data = []
    for col, distinct_count in zip(cols, distinct_counts):
        distinct_percent = distinct_count / total_rows
        not_distinct_count = total_rows - distinct_count
        not_distinct_percent = 1 - distinct_percent

        data.append({
            'column': col,
            'distinct_count': distinct_count,
            'distinct_percent': distinct_percent,
            'not_distinct_count': not_distinct_count,
            'not_distinct_percent': not_distinct_percent
        })
    
    distinct_properties = pd.DataFrame(data)
    return distinct_properties

def get_descriptive_statistics(
    pc: 'PostgresSQLClient',
    table_name: str,
    schema: str = 'public',
    columns: list[str] | None = None
):    
    all_cols_info = pc.get_columns_with_types(table_name=table_name, schema=schema)
    cols_info = {}
    if not all_cols_info:
        return pd.DataFrame()
    
    if columns is not None:
        cols = [c for c in columns if c in all_cols_info.keys()]
        if not cols:
            raise ValueError(f"Columns '{cols}' does not exist in table '{table_name}'.")
        for col in cols:
            cols_info[col] = all_cols_info[col]
    else:
        cols_info = all_cols_info

    numeric_types = ['smallint','integer','bigint','decimal','numeric','real','double precision']
    text_types = ['character varying','varchar','character','char','text']
    date_types = ['timestamp without time zone', 'timestamp with time zone', 'date', 'time without time zone', 'time with time zone']
    
    numeric_cols = [c for c, t in cols_info.items() if t in numeric_types]
    text_cols = [c for c, t in cols_info.items() if t in text_types]
    date_cols = [c for c, t in cols_info.items() if t in date_types]

    if not numeric_cols and not text_cols:
        raise ValueError("No numeric or text or time columns found in the table.")
    
    expr_list = []

    # Numerics statistics
    for col in numeric_cols:
        expr_list.append(
            f"MIN(\"{col}\") AS \"{col}_min\", "
            f"MAX(\"{col}\") AS \"{col}_max\", "
            f"AVG(\"{col}\") AS \"{col}_mean\", "
            f"percentile_cont(0.5) WITHIN GROUP (ORDER BY \"{col}\") AS \"{col}_median\""
        )

    # Non-numerics statistics
    for col in text_cols:
        expr_list.append(
            f"MIN(\"{col}\") AS \"{col}_min\", "
            f"MAX(\"{col}\") AS \"{col}_max\", "
            f"MIN(LENGTH(\"{col}\")) AS \"{col}_min_length\", "
            f"MAX(LENGTH(\"{col}\")) AS  \"{col}_max_length\", "
            f"AVG(LENGTH(\"{col}\")) AS \"{col}_mean_length\", "
            f"MIN(array_length(string_to_array(\"{col}\",' '),1)) AS \"{col}_min_word_count\", "
            f"MAX(array_length(string_to_array(\"{col}\",' '),1)) AS \"{col}_max_word_count\""
        )
    # Time
    for col in date_cols:
        expr_list.append(
            f"MIN(\"{col}\") AS \"{col}_min\", "
            f"MAX(\"{col}\") AS \"{col}_max\""
        )

    query = f"SELECT {', '.join(expr_list)} FROM \"{schema}\".\"{table_name}\";"
    result = pc.execute_query(query)

    columns = [desc[0] for desc in result.cursor.description]
    values = result.fetchone()
    df_result = pd.DataFrame([values], columns = columns)
    data = []

    # numeric
    for col in numeric_cols:
        data.append({
                'column': col,
                'min': round(df_result[f"{col}_min"][0],2),
                'max': round(df_result[f"{col}_max"][0],2),
                'mean': round(df_result[f"{col}_mean"][0],2),
                'median': round(df_result[f"{col}_median"][0],2)
            }
        )

    # date
    for col in date_cols:
        data.append({
                'column': col,
                'min': df_result[f"{col}_min"][0],
                'max': df_result[f"{col}_max"][0],
            }
        )

    # text
    for col in text_cols:
        data.append({
                'column': col,
                'min': df_result[f"{col}_min"][0],
                'max': df_result[f"{col}_max"][0],
                'min_length': df_result[f"{col}_min_length"][0],
                'max_length': df_result[f"{col}_max_length"][0],
                'mean_length': round(df_result[f"{col}_mean_length"][0],2),
                'min_word_count': df_result[f"{col}_min_word_count"][0],
                'max_word_count': df_result[f"{col}_max_word_count"][0],
            }
        )

    range_properties = pd.DataFrame(data)
    return range_properties

def get_common_value(
        pc: PostgresSQLClient,
        table_name: str,
        schema: str = 'public',
        column: str | None = None,
        top_n: int = 10): 
    
    all_cols = pc.get_columns(table_name=table_name, schema=schema)
    if column not in all_cols:
        raise ValueError(f"Column '{column}' does not exist in table '{table_name}'.")
    query = f"""
        SELECT "{column}" AS value, COUNT(*) AS freq
        FROM "{schema}"."{table_name}"
        WHERE "{column}" IS NOT NULL
        GROUP BY "{column}"
        ORDER BY freq DESC
        LIMIT {top_n};
    """
    res = pc.execute_query(query).fetchall()
    df = pd.DataFrame(res, columns=['Values', "Frequency"])
    return df

# Validity Check

def profile_text_found_in_set_percent(
        pc: PostgresSQLClient,
        table_name: str,
        schema: str = 'public',
        column: str | None = None,
):
    """A column-level check that calculates the percentage of rows for which the tested text column contains a value from a set of expected values. 
    Columns with null values are also counted as a passing value (the sensor assumes that a 'null' is also an expected and accepted value). 
    The check raises a data quality issue when the percentage of rows with a not null column value 
    that is not expected (not one of the values in the expected_values set) is below the expected threshold"""

    all_cols = pc.get_columns(table_name = table_name, schema = schema)
    if column not in all_cols:
        raise ValueError(f"Column '{column}' does not exist in table '{table_name}'.")
    

table_name = 'orders'
df = get_common_value(pc, "orders", "public", "created_at")
print(df)
