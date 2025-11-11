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
from jinja2 import Environment, FileSystemLoader
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
    gender         CHAR(5),    
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

def create_minio_connection(cfg: str):
    try:
        client = Minio(
            endpoint=cfg['endpoint'],
            access_key=cfg['root_user'],
            secret_key=cfg['root_password'],
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

def cal_upper_and_lower_bound(pc, schema:str, table_name:str, partition_column:str):
    with pc.engine.connect() as conn:
        result = pd.read_sql_query(f"SELECT MIN({partition_column}) AS lower, MAX({partition_column}) AS upper FROM {schema}.{table_name}", con = conn)
        lower_bound = result['lower'].iloc[0]
        upper_bound = result['upper'].iloc[0]
        return lower_bound, upper_bound
    
def read_data_from_postgre(spark: SparkSession,
                           host: str,
                           db_name: str,
                           schema: str,
                           table_name: str,
                           partition_column: str,
                           port: str="5432",
                           user: str = "postgres",
                           password: str = "postgres",
                           num_partitions: int = 8,
                           query: str = None) -> DataFrame:

    pc = PostgresSQLClient(database=db_name, host = host, user = user, password=password) 
    lower_bound, upper_bound = cal_upper_and_lower_bound(pc, schema=schema, table_name=table_name, partition_column=partition_column)
    
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "partitionColumn": partition_column,
        "upperBound": str(upper_bound),
        "lowerBound": str(lower_bound),
        "numPartitions": str(num_partitions)
    }

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"
    table_or_query = query if query else f"{schema}.{table_name}"

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            properties=properties,
            table = table_or_query
        )
        logger.info(f"Reading dirty data from Postgre Database {host}, {db_name}.{schema}.{table_name}")
        return df
    except Exception as e:
        logger.error(f"Error when connect spark with table {db_name}.{schema}.{table_name}: {e}")

def ingest_to_bronze(df: DataFrame, path: str, layer: str, source: str, target_table: str, spark: SparkSession, primary_key: str):
    """
        Start loading the raw data to the specified Minio bucket in format Delta Table.
        :param df: Transformed dataframe.
        :param path: bucket path.
        :return: None
    """
    try:
        logger.info(f"Loading to Layer {layer} ...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
        full_table_name = f"{layer}.{source}_{target_table}"

        # Transform varchar/char to stringtype
        for field in df.schema.fields:
            if "varchar" in str(field.dataType).lower() or "char" in str(field.dataType).lower():
                df = df.withColumn(field.name, col(field.name).cast(StringType))
        
        # Check if table exists:
        if spark.catalog.tableExists(full_table_name):
            logger.info(f"Table {full_table_name} exists, performing merge...")

            delta_table = DeltaTable.forName(spark, full_table_name)

            # Merge based on primary key: target -> existed data, source -> new data
            merge_condition = f"target.{primary_key} = source.{primary_key}"
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            logger.info(f"Merge completed for {full_table_name}")
        else:
            logger.info(f"Creating new table {full_table_name}")
            # Create new table
            df.write.format("delta")\
                .mode("overwrite")\
                .option("path", path)\
                .option("overwriteSchema", "true")\
                .saveAsTable(full_table_name)
            
            logger.info("Load successfully")

        # Log row count
        row_count = spark.table(full_table_name).count()
        logger.info(f"Load successfully: {row_count} rows in {full_table_name}")

    except Exception as e:
        logger.error(f"Error when ingest to bronze: {e}")
        raise

def run():
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
    
    # Create minio client
    client = create_minio_connection(dwh_cfg)
    if client:
        logger.info("Minio client exists")
    else:
        logger.info("Do not exist minio client")
    # Create bucket
    create_bucket(client=client, bucket_name=bucket_name)
    
    spark = create_spark_connection(app_name = app_name,
                                    access_key=access_key, 
                                    secret_key=secret_key, 
                                    endpoint=spark_endpoint)

    if spark:
        df = read_data_from_postgre(spark=spark, host='data_source',
                                    db_name='data_source',schema='public',
                                    table_name='orders',
                                    partition_column='order_id')

        if df: 
            # Minio
            layer = 'bronze'
            data_source = 'data_source'
            table_name = 'orders'

            path = str(f"s3a://{bucket_name}/{layer}/{data_source}/{table_name}")
            ingest_to_bronze(df = df, path = path, layer=layer, source=data_source, target_table=table_name, spark = spark, primary_key="order_id")
            logger.info(f"Successfully load data from {data_source} to {layer}.{data_source}.{table_name}")
        else:
            logger.info("DataFrame does not exist")
    else:
        logger.info("Error when load data!")
    
    spark.stop()

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

    template_folder = os.path.join(project_root, "data_quality/sensors/table/uniqueness/duplicate_record_count")

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


    context['column_name'] = 'gender'

    context['error_sampling'] = dict()
    context['error_sampling']['samples_limit'] = 5
    context['error_sampling']['total_samples_limit'] = 1000
    context['error_sampling']['id_columns'] = ['order_id']

    context['data_groupings'] = dict()
    # context['data_groupings']['gender'] = dict()
    # context['data_groupings']['gender']['source'] = None
    # context['data_groupings']['gender']['column'] = None

    context['time_series'] = dict()
    # context['time_series']['mode'] = None
    # context['time_series']['timestamp_column'] = None
    # context['time_series']['time_gradient'] = None

    context['time_window_filter'] = dict()
    # context['time_window_filter']['daily_partitioning_recent_days'] = 30
    # context['time_window_filter']['daily_partitioning_include_today'] = False

    context['parameters'] = dict()
    context['parameters']['filter'] = 'num_of_item >= 2'
    # context['parameters']['expected_values'] = ["A", "B", "C"]
    context['parameters']['columns'] = ['gender']
    # context['parameters']['sql_expression'] = "100.0 * SUM(CASE WHEN {alias}.email LIKE '%@%' THEN 1 ELSE 0 END) / COUNT(*)"
    # context['parameters']['sql_condition'] = "email LIKE '%@%'"

    context['additional_filters'] = []
    
    sql = template.render(**context)
    print("=== Generated SQL ===\n")
    print(sql)
    spark.sql(sql.strip()).show()
    print("\n=== End of SQL ===")

run_test()