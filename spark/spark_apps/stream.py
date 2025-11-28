from set_policy import Minio
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import logging
import yaml

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

logger = create_logger(name = "spark_processing")

def create_spark_connection(app_name:str, access_key:str, secret_key:str, endpoint:str) -> SparkSession:
    """
        Initialize the Spark Session with provided configurations.
        
        :param app_name: Name of the spark application.
        :param access_key: Access key for Minio.
        :param secret_key: Secret key for Minio.
        :param endpoint: Endpoint of Minio.
        :return: Spark session object or None if there's an error.
    """
    spark_conn = None

    try:
        spark_conn = (
            SparkSession.builder\
                .appName(app_name)\
                .config("spark.hadoop.fs.s3a.access.key", access_key)\
                .config("spark.hadoop.fs.s3a.secret.key", secret_key)\
                .config("spark.hadoop.fs.s3a.endpoint", endpoint)\
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
                .config("spark.hadoop.fs.s3a.path.style.access", "true")\
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
                .getOrCreate()
        )
        
        spark_conn.sparkContext.setLogLevel("Error")
        logger.info("Spark Session initialized successfully!")
        return spark_conn
    except Exception as e:
        logger.error(f"Error when creating spark connection: {e}")
        return None

def get_streaming_dataframe(spark: SparkSession, brokers:str, topic:str) -> DataFrame:
    """
        Get a streaming dataframe from Kafka.

        :param spark: Initialized Spark session.
        :param brokers: Comma-separated list of Kafka brokers.
        :param topic: Kafka topic to subscribe to.
        :return: Dataframe object or None if there's an error.
    """
    try:
        df = spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", brokers)\
            .option("subscribe", topic)\
            .option("startingOffsets", "earliest")\
            .load()
        logger.info("Streaming dataframe fetched successfully!")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch streaming dataframe: {e}")
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

def transform_streaming_data(df : DataFrame):
    """
        Transform the initial dataframe to get the final structure.
        
        :param df: Initial dataframe with raw data.
        :return: Transformed dataframe.
    """
    schema = StructType([
        StructField("full_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("location", StringType(), False),
        StructField("city", StringType(), False),
        StructField("country", StringType(), False),
        StructField("postcode", IntegerType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
        StructField("email", StringType(), False)
    ])

    transformed_df = df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col('value'), schema)).alias("data")\
        .select("data.*")
    if transformed_df:
        logger.info("Transform data with the suitable schema")
        return transformed_df
    else: 
        logger.info("Can not complete transforming data")
        return None

def initiate_streaming_to_bucket(df: DataFrame, path: str, checkpoint_location: str):
    """
        Start streaming the transformed data to the specified Minio bucket in parquet format.
        
        :param df: Transformed dataframe.
        :param path: bucket path.
        :param checkpoint_location: Checkpoint location for streaming.
        :return: None
    """
    logger.info("Initiating streaming process ...")
    streaming_query = df.writeStream.format("parquet")\
            .outputMode("append")\
            .option("path", path)\
            .option("checkpointLocation", checkpoint_location)\
            .start()
    streaming_query.awaitTermination()
    logger.info("Streaming to Minio successfully")

def run_streaming_job():

    # Load system config
    cfg_file = "/opt/airflow/sys_conf/config.yaml" 
    cfg = load_cfg(cfg_file)

    # Configure spark
    app_name = "people-info-streaming"
    dwh_cfg = cfg['dwh']
    kafka_cfg = cfg['kafka']
    access_key = dwh_cfg['root_user']
    secret_key = dwh_cfg['root_password']

    path = str(f"s3a://{dwh_cfg['bucket_name']}/data/")
    checkpoint_location = str(f"s3a://{dwh_cfg['bucket_name']}/checkpoint/")
    brokers = kafka_cfg['brokers']
    topic = kafka_cfg['topic']

    # Create minio client
    client = create_minio_connection(dwh_cfg)
    if client:
        logger.info("Minio client exists")
    else:
        logger.info("Do not exist minio client")
    # Create bucket
    create_bucket(client=client, bucket_name=dwh_cfg['bucket_name'])
    
    spark = create_spark_connection(app_name = app_name,
                                    access_key=access_key, 
                                    secret_key=secret_key, 
                                    endpoint=f"http://{dwh_cfg['endpoint']}")
    if spark:
        df = get_streaming_dataframe(spark = spark, brokers = brokers, topic = topic)
        if df:
            transformed_df = transform_streaming_data(df)
            initiate_streaming_to_bucket(df = transformed_df, 
                                         path = path, 
                                         checkpoint_location=checkpoint_location)
            logger.info("Successfully streaming data to Minio")
        else:
            logger.info("DataFrame from Kafka does not exist")
    else:
        logger.info("Fail to complete streaming data")

run_streaming_job()