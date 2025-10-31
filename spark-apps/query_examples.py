"""
Example queries for GDELT and SQuAD data in Iceberg
Run this with: ./query_data.sh
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, avg

def create_spark_session():
    """Create Spark session with Iceberg catalog"""
    spark = SparkSession.builder \
        .appName("Query Iceberg Data") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    return spark

def query_1_gdelt_events_summary(spark):
    """Query 1: GDELT Events Summary"""
    print("\n" + "="*70)
    print("Query 1: GDELT Events Summary - Top Countries by Event Count")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            actor1_country_code,
            actor1_name,
            COUNT(*) as event_count,
            AVG(goldstein_scale) as avg_goldstein,
            AVG(avg_tone) as avg_tone
        FROM lakehouse.gdelt_events
        GROUP BY actor1_country_code, actor1_name
        ORDER BY event_count DESC
        LIMIT 10
    """)
    
    result.show(truncate=False)

def query_2_conflict_events(spark):
    """Query 2: High Conflict Events"""
    print("\n" + "="*70)
    print("Query 2: High Conflict Events (Negative Goldstein Scale)")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            event_date,
            actor1_name,
            actor2_name,
            goldstein_scale,
            avg_tone,
            action_geo_fullname,
            num_mentions
        FROM lakehouse.gdelt_events
        WHERE goldstein_scale < 0
        ORDER BY goldstein_scale ASC
        LIMIT 10
    """)
    
    result.show(truncate=False)

def query_3_mentions_analysis(spark):
    """Query 3: Media Coverage Analysis"""
    print("\n" + "="*70)
    print("Query 3: Events by Media Coverage (Number of Mentions)")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            e.global_event_id,
            e.actor1_name,
            e.actor2_name,
            e.event_date,
            COUNT(m.mention_id) as mention_count,
            AVG(m.confidence) as avg_confidence
        FROM lakehouse.gdelt_events e
        LEFT JOIN lakehouse.gdelt_mentions m 
            ON e.global_event_id = m.global_event_id
        GROUP BY e.global_event_id, e.actor1_name, e.actor2_name, e.event_date
        ORDER BY mention_count DESC
    """)
    
    result.show(truncate=False)

def query_4_geographic_distribution(spark):
    """Query 4: Geographic Distribution of Events"""
    print("\n" + "="*70)
    print("Query 4: Events by Geographic Location")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            action_geo_country_code,
            action_geo_fullname,
            COUNT(*) as event_count,
            AVG(goldstein_scale) as avg_goldstein,
            AVG(num_mentions) as avg_media_coverage
        FROM lakehouse.gdelt_events
        WHERE action_geo_fullname IS NOT NULL
        GROUP BY action_geo_country_code, action_geo_fullname
        ORDER BY event_count DESC
    """)
    
    result.show(truncate=False)

def query_5_squad_analysis(spark):
    """Query 5: SQuAD Dataset Analysis"""
    print("\n" + "="*70)
    print("Query 5: SQuAD Question-Answer Pairs by Topic")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            title,
            question,
            answer_text,
            CASE 
                WHEN is_impossible THEN 'Impossible'
                ELSE 'Answerable'
            END as status
        FROM lakehouse.squad_data
        ORDER BY title
    """)
    
    result.show(truncate=False)

def query_6_parquet_comparison(spark):
    """Query 6: Compare Parquet vs Iceberg Performance"""
    print("\n" + "="*70)
    print("Query 6: Reading from Direct Parquet Files")
    print("="*70)
    
    print("Reading from Parquet...")
    df_parquet = spark.read.parquet("s3a://warehouse/gdelt_events_parquet")
    print(f"Total rows in Parquet: {df_parquet.count()}")
    
    print("\nSample from Parquet:")
    df_parquet.select("event_date", "actor1_name", "actor2_name", "goldstein_scale").show(5)

def query_7_time_series(spark):
    """Query 7: Time Series Analysis"""
    print("\n" + "="*70)
    print("Query 7: Events Over Time")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            event_date,
            COUNT(*) as event_count,
            AVG(goldstein_scale) as avg_goldstein,
            SUM(num_mentions) as total_mentions
        FROM lakehouse.gdelt_events
        GROUP BY event_date
        ORDER BY event_date
    """)
    
    result.show(truncate=False)

def query_8_iceberg_metadata(spark):
    """Query 8: Iceberg Table Metadata"""
    print("\n" + "="*70)
    print("Query 8: Iceberg Table Snapshots (Time Travel)")
    print("="*70)
    
    print("GDELT Events Snapshots:")
    spark.sql("SELECT * FROM lakehouse.gdelt_events.snapshots").show(truncate=False)
    
    print("\nTable History:")
    spark.sql("SELECT * FROM lakehouse.gdelt_events.history").show(truncate=False)

def main():
    print("Starting Iceberg Data Queries...")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # List all tables
        print("\n" + "="*70)
        print("Available Iceberg Tables")
        print("="*70)
        # This command is great for debugging your catalog's structure
        spark.sql("SHOW NAMESPACES FROM lakehouse").show()
        #spark.sql("SHOW TABLES FROM lakehouse").show(truncate=False)
        print(spark.catalog.listTables())
        
        
        # Run queries
        query_1_gdelt_events_summary(spark)
        query_2_conflict_events(spark)
        query_3_mentions_analysis(spark)
        query_4_geographic_distribution(spark)
        query_5_squad_analysis(spark)
        query_6_parquet_comparison(spark)
        query_7_time_series(spark)
        query_8_iceberg_metadata(spark)
        
        print("\n" + "="*70)
        print("All queries completed successfully!")
        print("="*70)
        
        print("\nTry your own queries in PySpark shell:")
        print("./start_pyspark.sh")
        
    except Exception as e:
        print(f"\nError during queries: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()