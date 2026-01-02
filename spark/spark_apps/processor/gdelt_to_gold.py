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
from pyspark.sql import SparkSession
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
        self._logger.info(f"Loading model from {repo_id} on {device}")
        self.tokenizer = AutoTokenizer.from_pretrained(repo_id)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(repo_id).to(device)
    
    def predict_sentiment(self, text: str, aspect: str) -> str:
        full_prompt = f"{ATSC_INSTRUCT}{text}{DELIM_INSTRUCT}{aspect}{EOS_INSTRUCT}"
        inputs = self.tokenizer(full_prompt, return_tensors="pt").to(self.device)
        
        with torch.no_grad():
            outputs = self.model.generate(**inputs, max_new_tokens=10)
        
        return self.tokenizer.decode(outputs[0], skip_special_tokens=True).strip().lower()
    
    def score_batch(self, texts: List[str], aspects: List[str]) -> List[Dict]:
        results = []
        score_map = {'positive': 1.0, 'negative': -1.0, 'neutral': 0.0}
        
        for text, aspect in zip(texts, aspects):
            try:
                label = self.predict_sentiment(text, aspect)
                score = score_map.get(label, 0.0)
                results.append({'sentiment_label': label, 'sentiment_score': score})
            except Exception as e:
                self._logger.error(f"Error scoring text: {e}")
                results.append({'sentiment_label': 'neutral', 'sentiment_score': 0.0})
        
        return results


def process_silver_to_gold(spark: SparkSession):
    logger.info("Reading from silver.gdelt_news")
    df = spark.read.format("iceberg").load("silver.gdelt_news")
    logger.info(f"Loaded {df.count()} rows from Silver")
    
    pdf = df.select("date", "ticker", "extracted_title").toPandas()
    
    ticker_to_name = {'NVDA': 'Nvidia', 'MSFT': 'Microsoft'}
    pdf['company_name'] = pdf['ticker'].map(ticker_to_name)
    
    analyzer = SentimentAnalyzer()
    logger.info(f"Scoring {len(pdf)} news items")
    
    batch_size = 100
    all_results = []
    
    for i in range(0, len(pdf), batch_size):
        batch = pdf.iloc[i:i+batch_size]
        batch_results = analyzer.score_batch(
            batch['extracted_title'].tolist(),
            batch['company_name'].tolist()
        )
        all_results.extend(batch_results)
    
    pdf['sentiment_label'] = [r['sentiment_label'] for r in all_results]
    pdf['sentiment_score'] = [r['sentiment_score'] for r in all_results]
    
    logger.info(f"Sentiment distribution: {pdf['sentiment_label'].value_counts().to_dict()}")
    
    agg_df = pdf.groupby(['date', 'ticker']).agg({
        'sentiment_score': ['sum', 'mean', 'count'],
    }).reset_index()
    agg_df.columns = ['date', 'ticker', 'sentiment_sum', 'sentiment_avg', 'news_count']
    
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
    
    gold_df = spark.createDataFrame(final_df, schema=schema)
    gold_df = gold_df.withColumn("ingestion_timestamp", current_timestamp())
    
    gold_df.write.format("iceberg").mode("overwrite").saveAsTable("gold.sentiment_features")
    
    logger.info(f"Written {spark.table('gold.sentiment_features').count()} rows to gold.sentiment_features")


def main():
    spark = create_spark_connection()
    
    try:
        process_silver_to_gold(spark)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()