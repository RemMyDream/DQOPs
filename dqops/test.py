import pandas as pd
from pandas import DataFrame
import numpy as np
import sys
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), "../")) 
from utils.postgresql_client import PostgresSQLClient
from sqlalchemy import create_engine
import logging

# Logger
def create_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(name)
    return logger

logger = create_logger(name = "Ingest to postgre")

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

def basic_statistic(df: DataFrame):
    num_rec = len(df)
    return df.describe()

df = pd.read_csv(r"C:\Users\Chien\Documents\Project VDT\dqops\data\orders.csv")
print(basic_statistic(df))

