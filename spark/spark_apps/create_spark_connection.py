import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from utils.helpers import create_logger, load_cfg

sys_conf = load_cfg("utils/config.yaml")
logger = create_logger(name="SparkConnection")


def get_or_create_spark(
    app_name=sys_conf['spark']['app'],
    access_key=sys_conf['lakehouse']['root_user'],
    secret_key=sys_conf['lakehouse']['root_password'],
    endpoint=f"http://{sys_conf['lakehouse']['endpoint']}",
    master_url="spark://spark-master:7077"
) -> SparkSession:
    existing = SparkSession.getActiveSession()
    if existing is not None:
        logger.info("Reusing existing Spark session")
        return existing
    
    logger.info("Creating new Spark session...")
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully!")
        return spark

    except Exception as e:
        logger.error(f"Error creating spark connection: {e}")
        raise

def stop_spark():
    existing = SparkSession.getActiveSession()
    if existing:
        logger.info("Stopping Spark session...")
        existing.stop()

create_spark_connection = get_or_create_spark
