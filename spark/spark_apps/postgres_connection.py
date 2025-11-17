import sys
from pyspark.sql import SparkSession
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), "../..")) 
from utils.postgresql_client import PostgresSQLClient
from sqlalchemy import create_engine
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from delta.tables import DeltaTable
import logging
from sqlalchemy import text
import yaml
from jinja2 import Environment, FileSystemLoader
from minio import Minio
from delta.tables import DeltaTable
import hashlib

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

def create_spark_connection(app_name, access_key, secret_key, endpoint):
    """
    Initialize or reuse an existing Spark Session with provided configurations.
    
    :param app_name: Name of the Spark application.
    :param access_key: Access key for Minio.
    :param secret_key: Secret key for Minio.
    :param endpoint: Endpoint of Minio.
    :return: Spark session object or None if there's an error.
    """
    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark is not None:
            print("Reusing existing Spark session.")
            return existing_spark
        
        spark_conn = (
            SparkSession.builder
                .appName(app_name)
                .config("spark.hadoop.fs.s3a.access.key", access_key)
                .config("spark.hadoop.fs.s3a.secret.key", secret_key)
                .config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
                .enableHiveSupport() \
                .getOrCreate()
        )
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        print("Spark Session initialized successfully!")
        return spark_conn

    except Exception as e:
        print(f"Error when creating spark connection: {e}")
        return None

def create_minio_connection(access_key: str, secret_key: str, endpoint: str):
    try:
        client = Minio(
            endpoint = endpoint,
            access_key = access_key,
            secret_key = secret_key,
            secure=False
        )
        logger.info("Create minio connection successfully!")
        return client
    except Exception as e:
        logger.error(f"Error when creating minio connection: {e}" )
        return None

def create_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        logger.info(f"First Initialze bucket {bucket_name}")
        client.make_bucket(bucket_name=bucket_name)
    else:
        logger.info(f"Bucket name {bucket_name} has already existed")

def get_primary_keys(pc: PostgresSQLClient, schema: str, table_name: str) -> list:
    query = f"""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = '{table_name}'
          AND tc.table_schema = '{schema}';
    """
    with pc.engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df['column_name'].tolist()

def detect_primary_keys(pc: PostgresSQLClient, schema: str, table_name: str, threshold: float = 0.99) -> list:
    query_cols = f"""
        SELECT column_name
        FROM information_schema.columns 
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}';
    """
    
    with pc.engine.connect() as conn:
        df_cols = pd.read_sql_query(query_cols, conn)

        column_names = df_cols['column_name'].tolist()
        result = []

        for col in column_names:
            query_unique = f"""
                SELECT 
                    COUNT(DISTINCT "{col}")::float / NULLIF(COUNT("{col}"), 0) AS distinct_percent
                FROM "{schema}"."{table_name}";
            """
            df_unique = pd.read_sql_query(query_unique, conn)
            distinct_percent = df_unique['distinct_percent'].iloc[0]

            if distinct_percent is not None and distinct_percent >= threshold:
                result.append(col)

    return result

def cal_upper_and_lower_bound(pc, schema: str, table_name: str, partition_column: str):
    """
    Tính lower và upper bound cho partition column.
    Sử dụng double quotes cho PostgreSQL identifiers.
    """
    with pc.engine.connect() as conn:
        query = f'''
            SELECT 
                MIN("{partition_column}") AS lower, 
                MAX("{partition_column}") AS upper 
            FROM "{schema}"."{table_name}"
        '''
        result = pd.read_sql_query(query, con=conn)
        lower_bound = result['lower'].iloc[0]
        upper_bound = result['upper'].iloc[0]
        return lower_bound, upper_bound 

def read_data_from_postgre(
    spark: SparkSession,
    pc: PostgresSQLClient,
    schema: str,
    table_name: str,
    primary_keys: list,
    num_partitions: int = 8
) -> DataFrame:

    jdbc_url = f"jdbc:postgresql://{pc.host}:{pc.port}/{pc.database}"

    properties = {
        "user": pc.user,
        "password": pc.password,
        "driver": "org.postgresql.Driver"
    }

    partition_column = primary_keys[0] if primary_keys else None

    if partition_column:
        try:
            lower_bound, upper_bound = cal_upper_and_lower_bound(
                pc, schema, table_name, partition_column
            )

            properties.update({
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
                "numPartitions": str(num_partitions)
            })

            logger.info(f"Reading data with partition on '{partition_column}' from {schema}.{table_name}")
        except Exception as e:
            logger.warning(f"Cannot partition on '{partition_column}': {e}. Reading without partition.")
            partition_column = None

    if not partition_column:
        logger.info(f"Reading data without partition from {schema}.{table_name}")

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f'"{schema}"."{table_name}"',   
            properties=properties
        )
        row_count = df.count()
        logger.info(f"Successfully read {row_count} rows from {schema}.{table_name}")
        return df

    except Exception as e:
        logger.error(f"Error connecting Spark to {schema}.{table_name}: {e}")
        return None

def ingest_to_bronze(
    df: DataFrame, 
    path: str, 
    layer: str, 
    table_name: str, 
    spark: SparkSession, 
    primary_keys: list
):
    """
    Load raw data to Delta Table with Hive Metastore as catalog.
    """
    try:
        logger.info(f"Loading to Layer {layer} ...")
        
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
        
        full_table_name = f"`{layer}`.`{table_name}`"

        table_exists = False
        try:
            DeltaTable.forName(spark, full_table_name)
            table_exists = True
        except:
            table_exists = False

        if table_exists:
            logger.info(f"Table {full_table_name} exists, performing merge...")
            delta_table = DeltaTable.forName(spark, full_table_name)
            
            merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in primary_keys])

            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            logger.info(f"Merge completed for {full_table_name}")
        else:
            logger.info(f"Creating new table {full_table_name}")
            df.write.format("delta")\
                .mode("overwrite")\
                .option("path", path)\
                .option("overwriteSchema", "true")\
                .saveAsTable(full_table_name)
            logger.info(f"Table {full_table_name} created and data loaded successfully")

        # Log row count
        row_count = spark.table(full_table_name).count()
        logger.info(f"Load successfully: {row_count} rows in {full_table_name}")

    except Exception as e:
        logger.error(f"Error when ingest to bronze: {e}")
        raise

def run(table_cfg: dict):
    # Load system config
    cfg_file = "/opt/spark/utils/sys_conf/config.yaml" 
    cfg = load_cfg(cfg_file)

    # Configure spark
    app_name = cfg['spark']['app_name']
    # Configure data_source
    host = cfg['data_source']['host']
    port = cfg['data_source']['port']
    # Configure database
    database_name = table_cfg['database']
    schema_name = table_cfg['schema']
    table_name = table_cfg['table']
    user = table_cfg['user']
    password = table_cfg['password']
    # Configure lakehouse
    lakehouse_cfg = cfg['lakehouse']
    access_key = lakehouse_cfg['root_user']
    secret_key = lakehouse_cfg['root_password']
    bucket_name = database_name
    minio_endpoint = f"http://{lakehouse_cfg['endpoint']}"

        
    # Create minio client
    client = create_minio_connection(access_key=access_key, secret_key=secret_key, endpoint=lakehouse_cfg['endpoint'])
    if client:
        logger.info("Minio client exists")
    else:
        logger.info("Do not exist minio client")
    
    # Create bucket
    create_bucket(client=client, bucket_name=bucket_name)
    
    spark = create_spark_connection(app_name=app_name,
                                    access_key=access_key, 
                                    secret_key=secret_key, 
                                    endpoint=minio_endpoint)

    pc = PostgresSQLClient(database=database_name, user=user, password=password, host=host, port=port)
    
    # primary_keys = get_primary_keys(pc, schema=schema_name, table_name=table_name)
    # if not primary_keys:
    #     primary_keys = detect_primary_keys(pc, schema_name, table_name)
    # print("Primary keys detected:", primary_keys)
    
    primary_keys = ['datarow_id']
    
    if spark:
        df = read_data_from_postgre(spark=spark,
                                    pc=pc,
                                    schema=schema_name,
                                    table_name=table_name,
                                    primary_keys=primary_keys)
    
        if df: 
            layer = 'bronze'
            path = f"s3a://{bucket_name}/{layer}/{schema_name}/{table_name}"
            
            ingest_to_bronze(df, path, layer, table_name, spark, primary_keys)
            logger.info(f"Successfully load data from Postgre: {database_name}.{schema_name}.{table_name} to Minio: {layer}.{table_name}")
        else:
            logger.info("DataFrame does not exist")
    else:
        logger.info("Error when load data!")
    
    spark.sql("SHOW TABLES IN bronze").show(truncate=False)
    
    query = f"SELECT * FROM `bronze`.`{table_name}`"
    res = spark.sql(query)
    res.show(5)          # hiển thị 5 dòng đầu
    print(res.count())   # số lượng row
    res.printSchema()    # in schema cột

    spark.stop()

table_cfg = {
    'database': "bkhcn",
    'schema': "public",
    'table': "01_bct_bao_cao_thuc_hien_ke_hoach_san_xuat_kinh_doanh_1008476",
    'user': "postgres",
    'password': "postgres"
}


def run_test():
    # Load system config
    cfg_file = "/opt/spark/utils/sys_conf/config.yaml" 
    cfg = load_cfg(cfg_file)

    # Configure spark
    app_name = cfg['spark']['app_name']
    dwh_cfg = cfg['dwh']
    access_key = dwh_cfg['root_user']
    secret_key = dwh_cfg['root_password']
    spark_endpoint = f"http://{dwh_cfg['endpoint']}"
    bucket_name = dwh_cfg['bucket_name']
    
    spark = create_spark_connection(app_name = app_name,
                                    access_key=access_key, 
                                    secret_key=secret_key, 
                                    endpoint=spark_endpoint)
    
    layer = 'bronze'
    data_source = 'data_source'
    table_name = 'orders'
    path = str(f"s3a://{bucket_name}/{layer}/{data_source}/{table_name}")

    pc = psql_client(database="data_source", host = "data_source")

    with pc.engine.connect() as conn:
        df = pd.read_sql_query(f"""
            SELECT 
                column_name, 
                data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}' AND table_schema = 'public'
            ORDER BY ordinal_position;
        """, con=conn)

    project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))

    dialects_folder = os.path.join(project_root, "data_quality/sensors")

    template_folder = os.path.join(project_root, "data_quality/sensors/column/accepted_values/text_found_in_set_percent")

    env = Environment(loader = FileSystemLoader([template_folder, dialects_folder]),
                  trim_blocks=True,
                  lstrip_blocks=True)
    
    template = env.get_template("spark.sql.jinja2")

    context = dict()

    context['target_table'] = dict()
    context['target_table']['schema_name'] = 'bronze'
    context['target_table']['table_name'] = 'data_source_orders'

    context['table'] = dict()
    # context['table']['filter'] = 'is_deleted = FALSE'
    context['table']['timestamp_columns'] = dict()
    context['table']['timestamp_columns']['event_timestamp_column'] = 'created_at'
    context['table']['timestamp_columns']['ingestion_timestamp_column'] = None
    context['table']['columns'] = dict()
    for index, row in df.iterrows():
        column_name = row.loc['column_name']
        data_type = row.loc['data_type']
        context['table']['columns'][column_name] = dict()
        context['table']['columns'][column_name]['type_snapshot'] = dict()

        context['table']['columns'][column_name]['type_snapshot']['column_type'] = data_type
        context['table']['columns'][column_name]['sql_expression'] = None


    context['column_name'] = 'status'

    context['error_sampling'] = dict()
    context['error_sampling']['samples_limit'] = 5
    context['error_sampling']['total_samples_limit'] = 1000
    context['error_sampling']['id_columns'] = ['order_id']

    context['data_groupings'] = dict()
    # context['data_groupings']['status'] = dict()
    # context['data_groupings']['status']['source'] = 'column_value'
    # context['data_groupings']['status']['column'] = 'status'

    context['time_series'] = dict()
    # context['time_series']['mode'] = None
    # context['time_series']['timestamp_column'] = l
    # context['time_series']['time_gradient'] = None

    context['time_window_filter'] = dict()
    # context['time_window_filter']['daily_partitioning_recent_days'] = 30
    # context['time_window_filter']['daily_partitioning_include_today'] = False

    context['parameters'] = dict()
    # context['parameters']['filter'] = 'num_of_item >= 2'
    context['parameters']['expected_values'] = ['Shipped', 'Processing', 'Returned']
    # context['parameters']['sql_expression'] = "100.0 * SUM(CASE WHEN {alias}.email LIKE '%@%' THEN 1 ELSE 0 END) / COUNT(*)"
    # context['parameters']['referenced_column']=
    # context['parameters']['referenced_table']=
    # min_date
    # max_date
    # percentile_value
    # context['parameters']['sql_condition'] = "email LIKE '%@%'"

    context['additional_filters'] = []
    
    sql = template.render(**context)
    print("=== Generated SQL ===\n")
    print(sql)
    spark.sql(sql.strip()).show()
    print("\n=== End of SQL ===")
    
    spark.stop()

run(table_cfg = table_cfg)