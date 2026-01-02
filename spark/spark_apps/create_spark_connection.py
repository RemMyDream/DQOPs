import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from utils.helpers import create_logger, load_cfg

sys_conf = load_cfg("utils/config.yaml")
logger = create_logger(name="SparkConnectionCreation")

def create_spark_connection(
    app_name = sys_conf['spark']['app'],
    access_key = sys_conf['lakehouse']['root_user'],
    secret_key = sys_conf['lakehouse']['root_password'],
    endpoint=f"http://{sys_conf['lakehouse']['endpoint']}",
    master_url="spark://spark-master:7077"
):    
    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark is not None:
            logger.info("Reusing existing Spark session.")
            return existing_spark
        
        spark_conn = (
            SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .master(master_url)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .getOrCreate()
        )
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session initialized successfully!")
        return spark_conn

    except Exception as e:
        logger.error(f"Error when creating spark connection: {e}")
        return None