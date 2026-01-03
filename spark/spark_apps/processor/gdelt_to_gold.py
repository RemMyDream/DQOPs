import sys
import os
from typing import List, Dict
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from create_spark_connection import create_spark_connection
from utils.helpers import create_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

logger = create_logger("silver_to_gold_gdelt")

MODEL_REPO = "bnmbanhmi/finstructabsa-flan-t5-large"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

ATSC_INSTRUCT = """Definition: Analyst task. Classify the sentiment of the financial Aspect as Positive, Negative, or Neutral.
    Example 1:
    input: Revenue grew significantly in the last quarter. The aspect is Revenue.
    output: positive
    
    input: """

DELIM_INSTRUCT = " The aspect is "
EOS_INSTRUCT = " \noutput:"

class SentimentAnalyzer:
    
    def __init__(self, repo_id: str = MODEL_REPO, device: str = DEVICE):
        self._logger = create_logger(self.__class__.__name__)
        self.device = device
        self._logger.info(f"Loading model from {repo_id}")
        self._logger.info(f"Using device: {device}")
        self.tokenizer = AutoTokenizer.from_pretrained(repo_id)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(repo_id).to(device)
        self._logger.info("Model loaded successfully")
    
    def predict_sentiment(self, text: str, aspect: str) -> str:
        full_prompt = f"{ATSC_INSTRUCT}{text}{DELIM_INSTRUCT}{aspect}{EOS_INSTRUCT}"
        inputs = self.tokenizer(
            full_prompt, 
            return_tensors="pt",
            truncation=True,      
            max_length=512          
        ).to(self.device)        
        with torch.no_grad():
            outputs = self.model.generate(**inputs, max_new_tokens=10)
        
        return self.tokenizer.decode(outputs[0], skip_special_tokens=True).strip().lower()
    
    def score_batch(self, texts: List[str], aspects: List[str]) -> List[Dict]:
        results = []
        score_map = {'positive': 1.0, 'negative': -1.0, 'neutral': 0.0}
        
        for idx, (text, aspect) in enumerate(zip(texts, aspects)):
            try:
                label = self.predict_sentiment(text, aspect)
                score = score_map.get(label, 0.0)
                results.append({'sentiment_label': label, 'sentiment_score': score})
                
                if (idx + 1) % 10 == 0:
                    self._logger.info(f"Processed {idx + 1}/{len(texts)} texts")
            except Exception as e:
                self._logger.error(f"Error scoring text at index {idx}: {e}")
                results.append({'sentiment_label': 'neutral', 'sentiment_score': 0.0})
        
        return results


class GoldSentimentService:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._logger = create_logger(self.__class__.__name__)
    
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
        
        self._logger.info("Executing merge operation")
        self.spark.sql(merge_sql)
        self._logger.info("Merge completed successfully")
    
    def process_silver_to_gold(self) -> int:
        logger.info("Reading from silver.gdelt_news")
        df = self.spark.read.format("iceberg").load("silver.gdelt_news")
        
        row_count = df.count()
        logger.info(f"Loaded {row_count} rows from silver.gdelt_news")
        
        logger.info("Converting to Pandas DataFrame")
        pdf = df.select("date", "ticker", "extracted_title").toPandas()
        
        ticker_to_name = {'NVDA': 'Nvidia', 'MSFT': 'Microsoft'}
        pdf['company_name'] = pdf['ticker'].map(ticker_to_name)
        
        logger.info("Initializing sentiment analyzer")
        analyzer = SentimentAnalyzer()
        
        batch_size = 100
        all_results = []
        total_batches = (len(pdf) + batch_size - 1) // batch_size
        
        logger.info(f"Starting sentiment analysis: {len(pdf)} news items in {total_batches} batches")
        
        for i in range(0, len(pdf), batch_size):
            batch_num = i // batch_size + 1
            logger.info(f"Processing batch {batch_num}/{total_batches}")
            
            batch = pdf.iloc[i:i+batch_size]
            batch_results = analyzer.score_batch(
                batch['extracted_title'].tolist(),
                batch['company_name'].tolist()
            )
            all_results.extend(batch_results)
            
            logger.info(f"Batch {batch_num} completed")
        
        pdf['sentiment_label'] = [r['sentiment_label'] for r in all_results]
        pdf['sentiment_score'] = [r['sentiment_score'] for r in all_results]
        
        logger.info(f"Sentiment distribution: {pdf['sentiment_label'].value_counts().to_dict()}")
        
        logger.info("Aggregating sentiment by date and ticker")
        agg_df = pdf.groupby(['date', 'ticker']).agg({
            'sentiment_score': ['sum', 'mean', 'count'],
        }).reset_index()
        agg_df.columns = ['date', 'ticker', 'sentiment_sum', 'sentiment_avg', 'news_count']
        
        logger.info("Calculating polarity ratios")
        polarity_df = pdf.groupby(['date', 'ticker', 'sentiment_label']).size().reset_index(name='count')
        polarity_pivot = polarity_df.pivot_table(
            index=['date', 'ticker'],
            columns='sentiment_label',
            values='count',
            fill_value=0
        ).reset_index()
        
        total_news = polarity_pivot.get('positive', 0) + polarity_pivot.get('negative', 0) + polarity_pivot.get('neutral', 0)
        polarity_pivot['polarity_ratio'] = (polarity_pivot.get('positive', 0) / total_news).fillna(0.5)
        
        final_df = agg_df.merge(
            polarity_pivot[['date', 'ticker', 'polarity_ratio']],
            on=['date', 'ticker'],
            how='left'
        )
        
        final_df = final_df[['date', 'ticker', 'sentiment_sum', 'polarity_ratio']]
        final_df['date'] = pd.to_datetime(final_df['date'])
        
        logger.info(f"Aggregated to {len(final_df)} date-ticker pairs")
        
        schema = StructType([
            StructField("date", DateType(), False),
            StructField("ticker", StringType(), False),
            StructField("sentiment_sum", DoubleType(), True),
            StructField("polarity_ratio", DoubleType(), True),
        ])
        
        logger.info("Converting to Spark DataFrame")
        gold_df = self.spark.createDataFrame(final_df, schema=schema)
        gold_df = gold_df.withColumn("ingestion_timestamp", current_timestamp())
        
        gold_table = "gold.sentiment_features"
        table_exists = self._table_exists(gold_table)
        
        if not table_exists:
            logger.info(f"Creating new table: {gold_table}")
            gold_df.write.format("iceberg").mode("overwrite").saveAsTable(gold_table)
            logger.info(f"Table created successfully")
        else:
            logger.info(f"Table exists, performing merge")
            self._merge_into_table(gold_df, gold_table, ["date", "ticker"])
        
        final_count = self.spark.table(gold_table).count()
        logger.info(f"Written {final_count} rows to {gold_table}")
        
        return final_count


def process_silver_to_gold(spark: SparkSession):
    service = GoldSentimentService(spark)
    return service.process_silver_to_gold()


def main():
    logger.info("Starting silver to gold GDELT processing")
    spark = create_spark_connection()
    
    try:
        row_count = process_silver_to_gold(spark)
        logger.info(f"Processing completed successfully: {row_count} rows in gold.sentiment_features")
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()