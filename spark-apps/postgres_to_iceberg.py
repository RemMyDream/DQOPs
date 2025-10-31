"""
Spark job to read data from PostgreSQL and write to MinIO as Parquet with Iceberg
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
import sys

def create_spark_session():
    """Create Spark session with required configurations"""
    spark = SparkSession.builder \
        .appName("PostgreSQL to Iceberg on MinIO") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                "org.postgresql:postgresql:42.7.3,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
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

def read_from_postgres(spark, table_name):
    """Read data from PostgreSQL"""
    print(f"Reading table: {table_name} from PostgreSQL...")
    
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/events_db") \
        .option("dbtable", table_name) \
        .option("user", "spark") \
        .option("password", "sparkpass") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"Successfully read {df.count()} rows from {table_name}")
    return df

def write_to_iceberg(df, table_name, partition_cols=None):
    """Write DataFrame to Iceberg table on MinIO"""
    print(f"Writing to Iceberg table: lakehouse.{table_name}")
    
    writer = df.writeTo(f"lakehouse.{table_name}") \
        .using("iceberg")
    
    if partition_cols:
        writer = writer.partitionedBy(*partition_cols)
    
    writer.createOrReplace()
    
    print(f"Successfully wrote to lakehouse.{table_name}")

def write_to_parquet(df, path):
    """Write DataFrame directly to Parquet on MinIO (without Iceberg)"""
    print(f"Writing to Parquet: {path}")
    
    df.write \
        .mode("overwrite") \
        .parquet(path)
    
    print(f"Successfully wrote to {path}")

def migrate_gdelt_events(spark):
    """Migrate GDELT events table"""
    df = read_from_postgres(spark, "gdelt_events")
    
    # Add partition columns if not already present
    if "event_year" not in df.columns:
        df = df.withColumn("event_year", year(col("event_date")))
    if "event_month" not in df.columns:
        df = df.withColumn("event_month", month(col("event_date")))
    
    # Write to Iceberg with partitioning
    write_to_iceberg(df, "gdelt_events", partition_cols=["event_year", "event_month"])
    
    # Also write to regular Parquet for comparison
    write_to_parquet(df, "s3a://warehouse/gdelt_events_parquet")
    
    return df

def migrate_gdelt_mentions(spark):
    """Migrate GDELT mentions table"""
    df = read_from_postgres(spark, "gdelt_mentions")
    
    # Write to Iceberg
    write_to_iceberg(df, "gdelt_mentions")
    
    # Also write to regular Parquet
    write_to_parquet(df, "s3a://warehouse/gdelt_mentions_parquet")
    
    return df

def migrate_squad_data(spark):
    """Migrate SQuAD data table"""
    df = read_from_postgres(spark, "squad_data")
    
    # Write to Iceberg
    write_to_iceberg(df, "squad_data")
    
    # Also write to regular Parquet
    write_to_parquet(df, "s3a://warehouse/squad_data_parquet")
    
    return df

def query_iceberg_tables(spark):
    """Query Iceberg tables to verify data"""
    print("\n" + "="*50)
    print("Querying Iceberg Tables")
    print("="*50)
    
    # Query GDELT events
    print("\nGDELT Events Sample:")
    spark.sql("SELECT * FROM lakehouse.gdelt_events LIMIT 5").show()
    
    # Show table metadata
    print("\nGDELT Events Metadata:")
    spark.sql("DESCRIBE EXTENDED lakehouse.gdelt_events").show(truncate=False)
    
    # Query GDELT mentions
    print("\nGDELT Mentions Count:")
    result = spark.sql("SELECT COUNT(*) as total FROM lakehouse.gdelt_mentions")
    result.show()
    
    # Query SQuAD data
    print("\nSQuAD Data Sample:")
    spark.sql("SELECT title, question, answer_text FROM lakehouse.squad_data LIMIT 5").show(truncate=False)

def main():
    print("Starting PostgreSQL to Iceberg migration...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Migrate tables
        print("\n" + "="*50)
        print("Migrating GDELT Events")
        print("="*50)
        migrate_gdelt_events(spark)
        
        print("\n" + "="*50)
        print("Migrating GDELT Mentions")
        print("="*50)
        migrate_gdelt_mentions(spark)
        
        print("\n" + "="*50)
        print("Migrating SQuAD Data")
        print("="*50)
        migrate_squad_data(spark)
        
        # Query and verify
        query_iceberg_tables(spark)
        
        print("\n" + "="*50)
        print("Migration completed successfully!")
        print("="*50)
        
        print("\nYou can access:")
        print("- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)")
        print("- Spark UI: http://localhost:4040")
        print("\nData locations:")
        print("- Iceberg tables: s3a://lakehouse/warehouse/")
        print("- Parquet files: s3a://warehouse/")
        
    except Exception as e:
        print(f"\nError during migration: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()