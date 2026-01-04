import sys
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
import pandas_datareader.data as pdr
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from utils.helpers import create_logger, load_cfg

logger = create_logger("gdelt_stooq_to_minio")

lakehouse_cfg = load_cfg("utils/config.yaml")['lakehouse']
BIGQUERY_API_KEY = "/opt/spark/utils/bigquery_api.json"


@dataclass
class StooqConfig:
    tickers: List[str]
    start_date: str
    end_date: Optional[str] = None

@dataclass
class GdeltConfig:
    start_date: str
    end_date: str
    stock_config: Dict[str, Dict]

class DataProvider(ABC):
    @abstractmethod
    def fetch_data(self) -> Optional[pd.DataFrame]:
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        pass
    
    @abstractmethod
    def get_primary_keys(self) -> List[str]:
        pass


class StooqProvider(DataProvider):
    
    def __init__(self, config: StooqConfig):
        self.config = config
        self._logger = create_logger(self.__class__.__name__)
    
    def fetch_data(self) -> Optional[pd.DataFrame]:
        self._logger.info(f"Fetching Stooq data for: {self.config.tickers}")
        
        start = datetime.strptime(self.config.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.config.end_date, '%Y-%m-%d') if self.config.end_date else datetime.now()
        
        all_data = []
        
        for ticker in self.config.tickers:
            try:
                self._logger.info(f"Downloading {ticker}: {start.date()} to {end.date()}")
                df = pdr.DataReader(ticker, 'stooq', start=start, end=end)
                
                if not df.empty:
                    df = df.reset_index()
                    df['ticker'] = ticker
                    df.columns = [c.lower() for c in df.columns]
                    all_data.append(df)
                    self._logger.info(f"Fetched {len(df)} rows for {ticker}")
                else:
                    self._logger.warning(f"No data for {ticker}")
            except Exception as e:
                self._logger.error(f"Error fetching {ticker}: {e}")
                continue
        
        if not all_data:
            return None
        
        return pd.concat(all_data, ignore_index=True)
    
    def get_table_name(self) -> str:
        return "stooq"
    
    def get_primary_keys(self) -> List[str]:
        return ["date", "ticker"]


class GdeltGkgProvider(DataProvider):
    
    def __init__(self, config: GdeltConfig):
        self.config = config
        self._logger = create_logger(self.__class__.__name__)
    
    def _generate_bq_filter(self) -> str:
        import re
        
        stock_conditions = []
        for ticker, rules in self.config.stock_config.items():
            aliases = [re.escape(a) for a in rules['aliases']]
            alias_str = "|".join(aliases)
            condition = f"REGEXP_CONTAINS(DocumentIdentifier, r'(?i){alias_str}')"
            
            if rules.get('blacklist'):
                blacklist = [re.escape(b) for b in rules['blacklist']]
                blk_str = "|".join(blacklist)
                condition += f" AND NOT REGEXP_CONTAINS(DocumentIdentifier, r'(?i){blk_str}')"
            
            stock_conditions.append(f"({condition})")
        
        return " OR ".join(stock_conditions)
    
    def fetch_data(self) -> Optional[pd.DataFrame]:
        try:
            from google.cloud import bigquery
            import os
            
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_API_KEY
            
            self._logger.info(f"Fetching GDELT GKG: {self.config.start_date} to {self.config.end_date}")
            
            client = bigquery.Client()
            filter_logic = self._generate_bq_filter()
            
            query = f"""
                SELECT
                    DATE,
                    DocumentIdentifier as source_url,
                    COALESCE(Themes, '') as themes,
                    COALESCE(Organizations, '') as organizations,
                    COALESCE(V2Tone, '') as tone,
                    COALESCE(Extras, '') as extras
                FROM `gdelt-bq.gdeltv2.gkg_partitioned`
                WHERE _PARTITIONDATE BETWEEN DATE('{self.config.start_date}') AND DATE('{self.config.end_date}')
                  AND (TranslationInfo IS NULL OR LENGTH(TranslationInfo) = 0)
                  AND ({filter_logic})
            """
            
            self._logger.info("Running BigQuery job...")
            query_job = client.query(query)
            df = query_job.to_dataframe()
            
            if df.empty:
                self._logger.warning("BigQuery returned 0 rows")
                return None
            
            df.columns = [c.lower() for c in df.columns]
            df = df.fillna('')
            
            self._logger.info(f"Fetched {len(df)} rows from BigQuery")
            return df
            
        except Exception as e:
            self._logger.error(f"BigQuery error: {e}")
            raise
    
    def get_table_name(self) -> str:
        return "gdelt_gkg"
    
    def get_primary_keys(self) -> List[str]:
        return ["date", "source_url"]


class IcebergWriter:
    
    def __init__(self, spark: SparkSession, catalog: str = "bronze"):
        self.spark = spark
        self.catalog = catalog
        self._logger = create_logger(self.__class__.__name__)
    
    def write(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        primary_keys: List[str]
    ) -> int:
        if df is None or df.empty:
            self._logger.warning("No data to write")
            return 0
        
        # Convert to Spark DataFrame
        if table_name == "gdelt_gkg":
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M%S', errors='coerce')
            
            schema = StructType([
                StructField("date", TimestampType(), True),
                StructField("source_url", StringType(), True),
                StructField("themes", StringType(), True),
                StructField("organizations", StringType(), True),
                StructField("tone", StringType(), True),
                StructField("extras", StringType(), True),
            ])
            spark_df = self.spark.createDataFrame(df, schema=schema)
        else:
            spark_df = self.spark.createDataFrame(df)
        
        spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp())
        iceberg_table = f"{self.catalog}.{table_name}"
        
        if not self._table_exists(iceberg_table):
            self._logger.info(f"Creating new table: {iceberg_table}")
            spark_df.write.format("iceberg").mode("overwrite").saveAsTable(iceberg_table)
        else:
            self._logger.info(f"Merging into: {iceberg_table}")
            self._merge_into_table(spark_df, iceberg_table, primary_keys)
        
        row_count = self.spark.table(iceberg_table).count()
        self._logger.info(f"{iceberg_table}: {row_count} total rows")
        return row_count

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except:
            return False
    
    def _merge_into_table(
        self, 
        source_df: DataFrame, 
        target_table: str, 
        primary_keys: List[str]
    ) -> None:
        temp_view = "source_temp_view"
        source_df.createOrReplaceTempView(temp_view)
        
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        columns = source_df.columns
        update_set = ", ".join([f"target.{c} = source.{c}" for c in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"source.{c}" for c in columns])
        
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        self.spark.sql(merge_sql)
        self._logger.info("Merge completed")


class BronzeIngestionService:    
    def __init__(self, spark: SparkSession, catalog: str = "bronze"):
        self.spark = spark
        self.catalog = catalog
        self.writer = IcebergWriter(spark, catalog)
        self._logger = create_logger(self.__class__.__name__)
    
    def ingest(self, provider: DataProvider) -> int:
        self._logger.info(f"Ingesting: {provider.get_table_name()} → {self.catalog}")
        
        df = provider.fetch_data()
        if df is None:
            self._logger.warning("No data fetched")
            return 0
        
        count = self.writer.write(
            df=df,
            table_name=provider.get_table_name(),
            primary_keys=provider.get_primary_keys(),
        )
        
        self._logger.info(f"Completed: {count} rows")
        return count


def ingest_stooq(
    spark: SparkSession,
    tickers: List[str],
    start_date: str,
    end_date: Optional[str] = None,
    catalog: str = "bronze"
) -> int:
    config = StooqConfig(tickers=tickers, start_date=start_date, end_date=end_date)
    provider = StooqProvider(config)
    service = BronzeIngestionService(spark, catalog=catalog)
    return service.ingest(provider)

def ingest_gdelt_gkg(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    stock_config: Dict[str, Dict],
    catalog: str = "bronze"
) -> int:
    config = GdeltConfig(start_date=start_date, end_date=end_date, stock_config=stock_config)
    provider = GdeltGkgProvider(config)
    service = BronzeIngestionService(spark, catalog=catalog)
    return service.ingest(provider)


