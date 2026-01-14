import sys
import os
from pyspark.sql import SparkSession, DataFrame
import logging
from utils.helpers import create_logger, load_cfg

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys_conf = load_cfg("utils/config.yaml")
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("CreateSparkConnection")

def create_spark_connection(app_name=sys_conf['spark']['app'],
                            access_key=sys_conf['lakehouse']['root_user'],
                            secret_key=sys_conf['lakehouse']['root_password'],
                            endpoint=f"http://{sys_conf['lakehouse']['endpoint']}",
                            master_url="spark://spark-master:7077"):
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
            logger.info("Reusing existing Spark session.")
            return existing_spark
        
        spark_conn = (
            SparkSession.builder
                .appName(app_name)
                .master(master_url)
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
        logger.info("Spark Session initialized successfully!")
        return spark_conn

    except Exception as e:
        logger.error(f"Error when creating spark connection: {e}")
        return None
