import sys
import os
from typing import List
import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, avg, stddev, lag, greatest, log, when, lit, to_date,
    current_timestamp, abs as spark_abs,
    sum as spark_sum, min as spark_min, max as spark_max
)
from pyspark.sql.types import StructType, StructField, DoubleType, DateType, StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from create_spark_connection import create_spark_connection
from utils.helpers import create_logger

logger = create_logger("bronze_to_silver_stooq")


class StooqSilverProcessor:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def process_ticker(self, df: DataFrame, ticker: str) -> DataFrame:
        df = df.select(
            to_date(col("date")).alias("Date"),
            col("open").alias("Open"),
            col("high").alias("High"),
            col("low").alias("Low"),
            col("close").alias("Close"),
            col("volume").alias("Volume"),
            col("ticker")
        )
        
        df = df.filter(col("ticker") == ticker)
        window_spec = Window.partitionBy("ticker").orderBy(col("Date"))
        
        df = self._calculate_moving_averages(df, window_spec)
        df = self._calculate_technical_indicators_pandas(df)
        df = self._calculate_stochastic(df, window_spec)
        df = self._calculate_roc(df, window_spec)
        df = self._calculate_bollinger_bands(df, window_spec)
        df = self._calculate_atr(df, window_spec)
        df = self._calculate_volume_indicators(df, window_spec)
        df = self._calculate_returns(df, window_spec)
        df = self._calculate_price_changes(df, window_spec)
        
        return df
        
    def _calculate_moving_averages(self, df: DataFrame, window_spec) -> DataFrame:
        periods = [5, 10, 20, 50, 200]
        for period in periods:
            window = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(-period + 1, 0)
            df = df.withColumn(f"MA_{period}", avg("Close").over(window))
        return df

    def _calculate_technical_indicators_pandas(self, df: DataFrame) -> DataFrame:        
        schema = StructType([
            StructField("Date", DateType(), True),
            StructField("ticker", StringType(), True),
            StructField("EMA_12", DoubleType(), True),
            StructField("EMA_26", DoubleType(), True),
            StructField("EMA_50", DoubleType(), True),
            StructField("EMA_200", DoubleType(), True),
            StructField("MACD", DoubleType(), True),
            StructField("MACD_signal", DoubleType(), True),
            StructField("MACD_hist", DoubleType(), True),
            StructField("RSI", DoubleType(), True),
        ])
        
        def calc_indicators(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf = pdf.sort_values("Date").reset_index(drop=True)
            
            # EMA
            for period in [12, 26, 50, 200]:
                pdf[f"EMA_{period}"] = pdf["Close"].ewm(span=period, adjust=False).mean()
            
            # MACD
            pdf["MACD"] = pdf["EMA_12"] - pdf["EMA_26"]
            pdf["MACD_signal"] = pdf["MACD"].ewm(span=9, adjust=False).mean()
            pdf["MACD_hist"] = pdf["MACD"] - pdf["MACD_signal"]
            
            delta = pdf["Close"].diff()
            gain = delta.where(delta > 0, 0.0)
            loss = (-delta).where(delta < 0, 0.0)
            
            avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
            avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
            
            rs = avg_gain / avg_loss
            pdf["RSI"] = 100 - (100 / (1 + rs))
            
            return pdf[["Date", "ticker", "EMA_12", "EMA_26", "EMA_50", "EMA_200", 
                       "MACD", "MACD_signal", "MACD_hist", "RSI"]]
        
        indicators_df = df.select("Date", "ticker", "Close").groupBy("ticker").applyInPandas(
            calc_indicators, schema
        )
        
        df = df.join(indicators_df, on=["Date", "ticker"], how="left")
        return df

    def _calculate_stochastic(self, df: DataFrame, window_spec, k_period: int = 14, d_period: int = 3) -> DataFrame:
        window_k = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(-k_period + 1, 0)
        
        df = df.withColumn("low_min", spark_min("Low").over(window_k))
        df = df.withColumn("high_max", spark_max("High").over(window_k))
        df = df.withColumn(
            "Stoch_K",
            (col("Close") - col("low_min")) / (col("high_max") - col("low_min")) * 100
        )
        
        window_d = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(-d_period + 1, 0)
        df = df.withColumn("Stoch_D", avg("Stoch_K").over(window_d))
        df = df.drop("low_min", "high_max")
        return df

    def _calculate_roc(self, df: DataFrame, window_spec, period: int = 12) -> DataFrame:
        df = df.withColumn("close_shifted", lag("Close", period).over(window_spec))
        df = df.withColumn("ROC", ((col("Close") - col("close_shifted")) / col("close_shifted")) * 100)
        df = df.drop("close_shifted")
        return df
    
    def _calculate_bollinger_bands(self, df: DataFrame, window_spec, period: int = 20, std_dev: int = 2) -> DataFrame:
        window = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(-period + 1, 0)
        
        df = df.withColumn("BB_middle", avg("Close").over(window))
        df = df.withColumn("bb_std", stddev("Close").over(window))
        df = df.withColumn("BB_upper", col("BB_middle") + (lit(std_dev) * col("bb_std")))
        df = df.withColumn("BB_lower", col("BB_middle") - (lit(std_dev) * col("bb_std")))
        df = df.withColumn("BB_width", col("BB_upper") - col("BB_lower"))
        df = df.drop("bb_std")
        return df

    def _calculate_atr(self, df: DataFrame, window_spec, period: int = 14) -> DataFrame:
        df = df.withColumn("prev_close", lag("Close", 1).over(window_spec))
        df = df.withColumn("hl", col("High") - col("Low"))
        df = df.withColumn("hc", spark_abs(col("High") - col("prev_close")))
        df = df.withColumn("lc", spark_abs(col("Low") - col("prev_close")))
        df = df.withColumn("true_range", greatest("hl", "hc", "lc"))
        
        window = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(-period + 1, 0)
        df = df.withColumn("ATR", avg("true_range").over(window))
        df = df.drop("prev_close", "hl", "hc", "lc", "true_range")
        return df

    def _calculate_volume_indicators(self, df: DataFrame, window_spec, period: int = 20) -> DataFrame:
        df = df.withColumn("price_change_sign", 
            when(col("Close") > lag("Close", 1).over(window_spec), 1)
            .when(col("Close") < lag("Close", 1).over(window_spec), -1)
            .otherwise(0)
        )
        
        window_all = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(Window.unboundedPreceding, 0)
        df = df.withColumn("OBV", spark_sum(col("price_change_sign") * col("Volume")).over(window_all))
        
        df = df.withColumn("typical_price", (col("High") + col("Low") + col("Close")) / 3)
        df = df.withColumn("cum_vp", spark_sum(col("typical_price") * col("Volume")).over(window_all))
        df = df.withColumn("cum_vol", spark_sum(col("Volume")).over(window_all))
        df = df.withColumn("VWAP", col("cum_vp") / col("cum_vol"))
        
        window = Window.partitionBy("ticker").orderBy(col("Date")).rowsBetween(-period + 1, 0)
        df = df.withColumn("Volume_MA", avg("Volume").over(window))
        df = df.withColumn("OBV_change", col("OBV") - lag("OBV", 1).over(window_spec))
        df = df.drop("price_change_sign", "cum_vp", "cum_vol", "typical_price")
        return df
    
    def _calculate_returns(self, df: DataFrame, window_spec) -> DataFrame:
        df = df.withColumn("prev_close", lag("Close", 1).over(window_spec))
        df = df.withColumn("Return", (col("Close") - col("prev_close")) / col("prev_close"))
        df = df.withColumn("Log_Return", log(col("Close") / col("prev_close")))
        df = df.drop("prev_close")
        return df
    
    def _calculate_price_changes(self, df: DataFrame, window_spec) -> DataFrame:
        df = df.withColumn("Price_Change", col("Close") - lag("Close", 1).over(window_spec))
        df = df.withColumn("Price_Change_Pct", 
            (col("Close") - lag("Close", 1).over(window_spec)) / lag("Close", 1).over(window_spec) * 100
        )
        df = df.withColumn("High_Low_Range", col("High") - col("Low"))
        df = df.withColumn("Open_Close_Range", col("Close") - col("Open"))
        return df
    
    def drop_unnecessary_columns(self, df: DataFrame) -> DataFrame:
        drop_cols = [
            'Close', 'Open', 'High', 'Low', 'Volume',
            "MA_5", "MA_10", "MA_50",
            "EMA_12", "EMA_50", "EMA_200",
            "BB_middle", "BB_upper", "BB_lower",
            "MACD_hist",
            "Stoch_D",
            "Volume_MA", "OBV_change",
            "Log_Return", "Price_Change",
        ]
        
        existing_cols = df.columns
        cols_to_drop = [c for c in drop_cols if c in existing_cols]
        
        return df.drop(*cols_to_drop) if cols_to_drop else df


class SilverIngestionService:
    
    def __init__(self, spark: SparkSession, source_catalog: str = "bronze", target_catalog: str = "silver"):
        self.spark = spark
        self.source_catalog = source_catalog
        self.target_catalog = target_catalog
        self.processor = StooqSilverProcessor(spark)
    
    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except:
            return False
    
    def _merge_into_table(self, source_df: DataFrame, target_table: str, primary_keys: List[str]) -> None:
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
    
    def process_stooq_to_silver(self, tickers: List[str]) -> int:
        bronze_df = self.spark.table(f"{self.source_catalog}.stooq")
        
        processed_dfs = []
        for ticker in tickers:
            ticker_df = self.processor.process_ticker(bronze_df, ticker)
            ticker_df = self.processor.drop_unnecessary_columns(ticker_df)
            processed_dfs.append(ticker_df)
        
        final_df = processed_dfs[0]
        for df in processed_dfs[1:]:
            final_df = final_df.unionByName(df, allowMissingColumns=True)
        
        final_df = final_df.withColumn("ingestion_timestamp", current_timestamp())
        
        silver_table = f"{self.target_catalog}.stock_indicators"
        table_exists = self._table_exists(silver_table)
        
        if not table_exists:
            final_df.write.format("iceberg").mode("overwrite").saveAsTable(silver_table)
        else:
            self._merge_into_table(final_df, silver_table, ["Date", "ticker"])
        
        return self.spark.table(silver_table).count()


def process_bronze_to_silver(
    tickers: List[str],
    source_catalog: str = "bronze",
    target_catalog: str = "silver"
) -> int:
    spark = create_spark_connection()
    try:
        service = SilverIngestionService(spark, source_catalog, target_catalog)
        return service.process_stooq_to_silver(tickers)
    finally:
        spark.stop()


def main():
    tickers = ['NVDA', 'MSFT']
    row_count = process_bronze_to_silver(tickers=tickers)
    logger.info(f"Processing completed: {row_count} rows in stock_indicators table")


if __name__ == "__main__":
    main()