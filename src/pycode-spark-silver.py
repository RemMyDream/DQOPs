import logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from spark_utils import create_spark_session

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_silver_events(spark):
    """Process GDELT Events from Bronze to Silver layer"""
    logger.info("Processing Silver Events...")
    
    # Read from Bronze layer (Iceberg table)
    bronze_events = spark.read.table("lakehouse.bronze.gdelt_events")
    
    logger.info(f"Loaded {bronze_events.count()} events from bronze layer")
    
    # Drop rows where title is NaN (if title column exists)
    if 'title' in bronze_events.columns:
        silver_events = bronze_events.filter(F.col('title').isNotNull())
        logger.info(f"Dropped {bronze_events.count() - silver_events.count()} rows with missing titles")
    else:
        silver_events = bronze_events
        logger.info("No title column found, skipping title filtering")
    
    # Select relevant columns for financial prediction
    events_columns = [
        'globaleventid',
        'sqldate',
        'year',
        'monthyear',
        'eventcode',
        'cameocodedescription',
        'eventbasecode',
        'eventrootcode',
        'quadclass',
        'goldsteinscale',
        'nummentions',
        'numsources',
        'numarticles',
        'avgtone',
        'actor1name',
        'actor1countrycode',
        'actor2name',
        'actor2countrycode',
        'actiongeo_countrycode',
        'actiongeo_fullname',
        'actiongeo_lat',
        'actiongeo_long',
        'sourceurl'
    ]
    
    # Add title if it exists
    if 'title' in silver_events.columns:
        events_columns.append('title')
    
    # Select only columns that exist in the dataframe
    available_columns = [col for col in events_columns if col in silver_events.columns]
    silver_events = silver_events.select(available_columns)
    
    # Convert sqldate to date format
    silver_events = silver_events.withColumn(
        'date',
        F.to_date(F.col('sqldate').cast('string'), 'yyyyMMdd')
    )
    
    # Drop rows with invalid dates
    invalid_dates = silver_events.filter(F.col('date').isNull()).count()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} invalid dates, dropping them")
        silver_events = silver_events.filter(F.col('date').isNotNull())
    
    # Sort by date
    silver_events = silver_events.orderBy('date')
    
    # Remove duplicate events (same event reported multiple times)
    silver_events = silver_events.dropDuplicates(['globaleventid'])
    
    logger.info(f"Silver events shape: {silver_events.count()} rows, {len(silver_events.columns)} columns")
    
    # Write to Silver layer (Iceberg table)
    logger.info("Writing silver events to Iceberg table...")
    silver_events.writeTo("lakehouse.silver.gdelt_events") \
        .using("iceberg") \
        .createOrReplace()
    
    logger.info("Silver events processing completed!")
    return silver_events


def process_silver_gkg(spark):
    """Process GDELT GKG from Bronze to Silver layer"""
    logger.info("Processing Silver GKG...")
    
    # Read from Bronze layer
    bronze_gkg = spark.read.table("lakehouse.bronze.gdelt_gkg")
    
    logger.info(f"Loaded {bronze_gkg.count()} GKG records from bronze layer")
    
    # Drop rows where title is NaN (if title column exists)
    if 'title' in bronze_gkg.columns:
        silver_gkg = bronze_gkg.filter(F.col('title').isNotNull())
        logger.info(f"Dropped {bronze_gkg.count() - silver_gkg.count()} rows with missing titles")
    else:
        silver_gkg = bronze_gkg
        logger.info("No title column found, skipping title filtering")
    
    # Select relevant columns
    gkg_columns = [
        'gkgrecordid',
        'date',
        'sourcecommonname',
        'documentidentifier',
        'themes',
        'v2themes',
        'locations',
        'v2locations',
        'persons',
        'v2persons',
        'organizations',
        'v2organizations',
        'v2tone',
        'allnames',
        'amounts'
    ]
    
    # Add title if it exists
    if 'title' in silver_gkg.columns:
        gkg_columns.append('title')
    
    # Select only columns that exist
    available_columns = [col for col in gkg_columns if col in silver_gkg.columns]
    silver_gkg = silver_gkg.select(available_columns)
    
    # Convert date to datetime (GDELT GKG date format: YYYYMMDDHHMMSS)
    silver_gkg = silver_gkg.withColumn(
        'datetime',
        F.to_timestamp(F.col('date').cast('string'), 'yyyyMMddHHmmss')
    )
    
    silver_gkg = silver_gkg.withColumn(
        'date_only',
        F.to_date(F.col('datetime'))
    )
    
    # Drop rows with invalid dates
    invalid_dates = silver_gkg.filter(F.col('datetime').isNull()).count()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} invalid dates, dropping them")
        silver_gkg = silver_gkg.filter(F.col('datetime').isNotNull())
    
    # Sort by datetime
    silver_gkg = silver_gkg.orderBy('datetime')
    
    # Remove duplicate records
    silver_gkg = silver_gkg.dropDuplicates(['gkgrecordid'])
    
    # Parse v2tone (format: "tone,positive,negative,polarity,...")
    if 'v2tone' in silver_gkg.columns:
        silver_gkg = silver_gkg.withColumn('tone_split', F.split(F.col('v2tone'), ','))
        silver_gkg = silver_gkg.withColumn('tone', F.col('tone_split')[0].cast('double'))
        silver_gkg = silver_gkg.withColumn('tone_positive', F.col('tone_split')[1].cast('double'))
        silver_gkg = silver_gkg.withColumn('tone_negative', F.col('tone_split')[2].cast('double'))
        silver_gkg = silver_gkg.withColumn('tone_polarity', F.col('tone_split')[3].cast('double'))
        silver_gkg = silver_gkg.drop('tone_split')
    
    logger.info(f"Silver GKG shape: {silver_gkg.count()} rows, {len(silver_gkg.columns)} columns")
    
    # Write to Silver layer
    logger.info("Writing silver GKG to Iceberg table...")
    silver_gkg.writeTo("lakehouse.silver.gdelt_gkg") \
        .using("iceberg") \
        .createOrReplace()
    
    logger.info("Silver GKG processing completed!")
    return silver_gkg


def process_silver_stooq(spark):
    """Process Stooq stock data from Bronze to Silver layer"""
    logger.info("Processing Silver Stooq...")
    
    # Read from Bronze layer
    bronze_stooq = spark.read.table("lakehouse.bronze.stooq_stock_prices")
    
    logger.info(f"Loaded {bronze_stooq.count()} stock records from bronze layer")
    
    # Convert timestamp to date
    silver_stooq = bronze_stooq.withColumn(
        'date',
        F.to_date(F.col('timestamp'))
    )
    
    # Drop rows with invalid dates
    invalid_dates = silver_stooq.filter(F.col('date').isNull()).count()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} invalid dates, dropping them")
        silver_stooq = silver_stooq.filter(F.col('date').isNotNull())
    
    # Sort by symbol and date
    silver_stooq = silver_stooq.orderBy('symbol', 'date')
    
    # Remove duplicates (keep last record for same symbol and date)
    from pyspark.sql import Window
    window_spec = Window.partitionBy('symbol', 'date').orderBy(F.col('timestamp').desc())
    silver_stooq = silver_stooq.withColumn('row_num', F.row_number().over(window_spec))
    silver_stooq = silver_stooq.filter(F.col('row_num') == 1).drop('row_num')
    
    # Add calculated features
    window_symbol = Window.partitionBy('symbol').orderBy('date')
    
    # Daily return
    silver_stooq = silver_stooq.withColumn(
        'daily_return',
        (F.col('close') - F.lag('close', 1).over(window_symbol)) / F.lag('close', 1).over(window_symbol)
    )
    
    # Price range
    silver_stooq = silver_stooq.withColumn(
        'price_range',
        F.col('high') - F.col('low')
    )
    
    # Price change
    silver_stooq = silver_stooq.withColumn(
        'price_change',
        F.col('close') - F.col('open')
    )
    
    # Select relevant columns
    stooq_columns = [
        'date',
        'symbol',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'daily_return',
        'price_range',
        'price_change'
    ]
    
    silver_stooq = silver_stooq.select(stooq_columns)
    
    logger.info(f"Silver stooq shape: {silver_stooq.count()} rows, {len(silver_stooq.columns)} columns")
    
    # Write to Silver layer
    logger.info("Writing silver stooq to Iceberg table...")
    silver_stooq.writeTo("lakehouse.silver.stooq_stock_prices") \
        .using("iceberg") \
        .createOrReplace()
    
    logger.info("Silver Stooq processing completed!")
    return silver_stooq


def main():
    """Main Silver layer processing job"""
    logger.info("Starting Silver Layer Processing Job")
    
    # Create Spark session
    spark = create_spark_session(app_name="Silver Layer Processing")
    
    try:
        # Process all datasets
        logger.info("=== Processing Events ===")
        process_silver_events(spark)
        
        logger.info("=== Processing GKG ===")
        process_silver_gkg(spark)
        
        logger.info("=== Processing Stooq ===")
        process_silver_stooq(spark)
        
        # Verify silver tables
        logger.info("=== Verification ===")
        silver_tables = spark.sql("SHOW TABLES IN lakehouse.silver").collect()
        logger.info(f"Silver tables created: {len(silver_tables)}")
        for table in silver_tables:
            table_name = table.tableName
            count = spark.table(f"lakehouse.silver.{table_name}").count()
            logger.info(f"  - {table_name}: {count} rows")
        
        logger.info("Silver Layer Processing Job completed successfully!")
        
    except Exception as e:
        logger.error(f"Silver Layer Processing Job failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
