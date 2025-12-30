import logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
from spark_utils import create_spark_session

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_gold_events(spark):
    """Process GDELT Events from Silver to Gold layer with aggregations"""
    logger.info("Processing Gold Events...")
    
    # Read from Silver layer
    silver_events = spark.read.table("lakehouse.silver.gdelt_events")
    
    logger.info(f"Loaded {silver_events.count()} events from silver layer")
    
    # Add text features (vectorized operations)
    gold_events = silver_events
    
    if 'title' in gold_events.columns:
        gold_events = gold_events.withColumn('title_length', F.length(F.col('title')))
        gold_events = gold_events.withColumn('title_word_count', F.size(F.split(F.col('title'), ' ')))
    else:
        gold_events = gold_events.withColumn('title_length', F.lit(0))
        gold_events = gold_events.withColumn('title_word_count', F.lit(0))
    
    # Aggregate by date (daily aggregates)
    gold_events_daily = gold_events.groupBy('date').agg(
        F.mean('goldsteinscale').alias('goldsteinscale_mean'),
        F.stddev('goldsteinscale').alias('goldsteinscale_std'),
        F.min('goldsteinscale').alias('goldsteinscale_min'),
        F.max('goldsteinscale').alias('goldsteinscale_max'),
        F.mean('avgtone').alias('avgtone_mean'),
        F.stddev('avgtone').alias('avgtone_std'),
        F.min('avgtone').alias('avgtone_min'),
        F.max('avgtone').alias('avgtone_max'),
        F.sum('nummentions').alias('nummentions_sum'),
        F.sum('numsources').alias('numsources_sum'),
        F.sum('numarticles').alias('numarticles_sum'),
        F.mean('title_length').alias('title_length_mean'),
        F.mean('title_word_count').alias('title_word_count_mean'),
        F.count('globaleventid').alias('event_count')
    )
    
    # Fill null values (from std when only 1 event)
    gold_events_daily = gold_events_daily.fillna(0)
    
    # Sort by date
    gold_events_daily = gold_events_daily.orderBy('date')
    
    logger.info(f"Gold events daily shape: {gold_events_daily.count()} rows, {len(gold_events_daily.columns)} columns")
    
    # Write to Gold layer
    logger.info("Writing gold events daily to Iceberg table...")
    gold_events_daily.writeTo("lakehouse.gold.gdelt_events_daily") \
        .using("iceberg") \
        .createOrReplace()
    
    logger.info("Gold events processing completed!")
    return gold_events_daily


def process_gold_gkg(spark):
    """Process GDELT GKG from Silver to Gold layer with aggregations"""
    logger.info("Processing Gold GKG...")
    
    # Read from Silver layer
    silver_gkg = spark.read.table("lakehouse.silver.gdelt_gkg")
    
    logger.info(f"Loaded {silver_gkg.count()} GKG records from silver layer")
    
    # Add text features
    gold_gkg = silver_gkg
    
    if 'title' in gold_gkg.columns:
        gold_gkg = gold_gkg.withColumn('title_length', F.length(F.col('title')))
        gold_gkg = gold_gkg.withColumn('title_word_count', F.size(F.split(F.col('title'), ' ')))
    else:
        gold_gkg = gold_gkg.withColumn('title_length', F.lit(0))
        gold_gkg = gold_gkg.withColumn('title_word_count', F.lit(0))
    
    # Extract date for aggregation
    gold_gkg = gold_gkg.withColumn('date', F.to_date(F.col('datetime')))
    
    # Aggregate by date (daily aggregates)
    agg_exprs = [
        F.count('gkgrecordid').alias('gkg_count'),
        F.mean('title_length').alias('title_length_mean'),
        F.mean('title_word_count').alias('title_word_count_mean')
    ]
    
    # Add tone aggregations if available
    if 'tone' in gold_gkg.columns:
        agg_exprs.extend([
            F.mean('tone').alias('tone_mean'),
            F.stddev('tone').alias('tone_std'),
            F.min('tone').alias('tone_min'),
            F.max('tone').alias('tone_max')
        ])
    
    if 'tone_positive' in gold_gkg.columns:
        agg_exprs.extend([
            F.mean('tone_positive').alias('tone_positive_mean'),
            F.stddev('tone_positive').alias('tone_positive_std')
        ])
    
    if 'tone_negative' in gold_gkg.columns:
        agg_exprs.extend([
            F.mean('tone_negative').alias('tone_negative_mean'),
            F.stddev('tone_negative').alias('tone_negative_std')
        ])
    
    if 'tone_polarity' in gold_gkg.columns:
        agg_exprs.extend([
            F.mean('tone_polarity').alias('tone_polarity_mean'),
            F.stddev('tone_polarity').alias('tone_polarity_std')
        ])
    
    gold_gkg_daily = gold_gkg.groupBy('date').agg(*agg_exprs)
    
    # Fill null values
    gold_gkg_daily = gold_gkg_daily.fillna(0)
    
    # Sort by date
    gold_gkg_daily = gold_gkg_daily.orderBy('date')
    
    logger.info(f"Gold GKG daily shape: {gold_gkg_daily.count()} rows, {len(gold_gkg_daily.columns)} columns")
    
    # Write to Gold layer
    logger.info("Writing gold GKG daily to Iceberg table...")
    gold_gkg_daily.writeTo("lakehouse.gold.gdelt_gkg_daily") \
        .using("iceberg") \
        .createOrReplace()
    
    logger.info("Gold GKG processing completed!")
    return gold_gkg_daily


def process_gold_stooq(spark):
    """Process Stooq stock data from Silver to Gold layer"""
    logger.info("Processing Gold Stooq...")
    
    # Read from Silver layer
    silver_stooq = spark.read.table("lakehouse.silver.stooq_stock_prices")
    
    # Gold stooq is just a copy of silver (already clean)
    gold_stooq = silver_stooq
    
    logger.info(f"Gold stooq shape: {gold_stooq.count()} rows")
    
    # Write to Gold layer
    logger.info("Writing gold stooq to Iceberg table...")
    gold_stooq.writeTo("lakehouse.gold.stooq_stock_prices") \
        .using("iceberg") \
        .createOrReplace()
    
    logger.info("Gold Stooq processing completed!")
    return gold_stooq


def create_gold_symbol_datasets(spark):
    """Create gold_<symbol> datasets by joining events, gkg, and stock data"""
    logger.info("Creating gold_<symbol> datasets...")
    
    # Read gold datasets
    gold_events_daily = spark.read.table("lakehouse.gold.gdelt_events_daily")
    gold_gkg_daily = spark.read.table("lakehouse.gold.gdelt_gkg_daily")
    gold_stooq = spark.read.table("lakehouse.gold.stooq_stock_prices")
    
    # Get unique symbols
    symbols = [row.symbol for row in gold_stooq.select('symbol').distinct().collect()]
    
    logger.info(f"Found {len(symbols)} symbols: {symbols}")
    
    for symbol in symbols:
        logger.info(f"Processing gold_{symbol.lower()}...")
        
        # Filter stock data for this symbol
        symbol_stock = gold_stooq.filter(F.col('symbol') == symbol)
        
        # Merge with events (left join to keep all stock dates)
        gold_symbol = symbol_stock.join(
            gold_events_daily,
            on='date',
            how='left'
        )
        
        # Merge with GKG (left join)
        gold_symbol = gold_symbol.join(
            gold_gkg_daily,
            on='date',
            how='left'
        )
        
        # Sort by date
        gold_symbol = gold_symbol.orderBy('date')
        
        # Forward fill missing values (use previous day's news sentiment if no news today)
        # Define window for forward fill
        window_spec = Window.partitionBy('symbol').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        # Get all columns except date and symbol
        fill_columns = [col for col in gold_symbol.columns if col not in ['date', 'symbol']]
        
        # Forward fill each column
        for col_name in fill_columns:
            gold_symbol = gold_symbol.withColumn(
                col_name,
                F.last(col_name, ignorenulls=True).over(window_spec)
            )
        
        # Fill remaining nulls with 0
        gold_symbol = gold_symbol.fillna(0)
        
        # Add time-based features
        gold_symbol = gold_symbol.withColumn('day_of_week', F.dayofweek(F.col('date')))
        gold_symbol = gold_symbol.withColumn('month', F.month(F.col('date')))
        gold_symbol = gold_symbol.withColumn('quarter', F.quarter(F.col('date')))
        
        # Add lagged features (previous day)
        window_lag = Window.partitionBy('symbol').orderBy('date')
        gold_symbol = gold_symbol.withColumn('prev_close', F.lag('close', 1).over(window_lag))
        gold_symbol = gold_symbol.withColumn('prev_volume', F.lag('volume', 1).over(window_lag))
        gold_symbol = gold_symbol.withColumn('prev_daily_return', F.lag('daily_return', 1).over(window_lag))
        
        # Add rolling window features (7-day moving averages)
        window_ma = Window.partitionBy('symbol').orderBy('date').rowsBetween(-6, 0)
        gold_symbol = gold_symbol.withColumn('close_ma7', F.avg('close').over(window_ma))
        gold_symbol = gold_symbol.withColumn('volume_ma7', F.avg('volume').over(window_ma))
        
        logger.info(f"  - Shape: {gold_symbol.count()} rows, {len(gold_symbol.columns)} columns")
        
        # Write to Gold layer
        table_name = f"gold_{symbol.lower()}"
        logger.info(f"Writing {table_name} to Iceberg table...")
        gold_symbol.writeTo(f"lakehouse.gold.{table_name}") \
            .using("iceberg") \
            .createOrReplace()
    
    logger.info(f"Gold symbol datasets created for {len(symbols)} symbols!")


def main():
    """Main Gold layer processing job"""
    logger.info("Starting Gold Layer Processing Job")
    
    # Create Spark session
    spark = create_spark_session(app_name="Gold Layer Processing")
    
    try:
        # Create gold namespace if it doesn't exist
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        
        # Process all datasets
        logger.info("=== Processing Events ===")
        process_gold_events(spark)
        
        logger.info("=== Processing GKG ===")
        process_gold_gkg(spark)
        
        logger.info("=== Processing Stooq ===")
        process_gold_stooq(spark)
        
        logger.info("=== Creating Symbol Datasets ===")
        create_gold_symbol_datasets(spark)
        
        # Verify gold tables
        logger.info("=== Verification ===")
        gold_tables = spark.sql("SHOW TABLES IN lakehouse.gold").collect()
        logger.info(f"Gold tables created: {len(gold_tables)}")
        for table in gold_tables:
            table_name = table.tableName
            count = spark.table(f"lakehouse.gold.{table_name}").count()
            logger.info(f"  - {table_name}: {count} rows")
        
        logger.info("Gold Layer Processing Job completed successfully!")
        logger.info("✓ All gold datasets are ready for train/val/test split!")
        
    except Exception as e:
        logger.error(f"Gold Layer Processing Job failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
