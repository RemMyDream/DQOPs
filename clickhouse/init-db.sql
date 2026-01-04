-- Create Database
CREATE DATABASE IF NOT EXISTS warehouse;

--------------------------------------------------------------------------------
-- 1. GOLD LAYER (Iceberg Connection)
--------------------------------------------------------------------------------
-- Kết nối trực tiếp vào bảng Iceberg có sẵn trong MinIO.
-- Bảng này chứa đầy đủ dữ liệu Price + Sentiment theo format bạn cần.
CREATE TABLE IF NOT EXISTS warehouse.gold_sentiment_features
ENGINE = Iceberg('http://minio:9000/gold/sentiment_features', 'minio_access_key', 'minio_secret_key')
SETTINGS allow_experimental_iceberg_engine = 1;


--------------------------------------------------------------------------------
-- 2. SERVING LAYER (Views & Compatibility)
--------------------------------------------------------------------------------
-- Tạo các "bí danh" (Views) để Dashboard cũ (tên 'csv_data' hoặc 'gold_stock_analytics') 
-- có thể lấy dữ liệu trực tiếp từ nguồn Iceberg ở trên.

-- Cầu nối cho Dashboard dùng tên mới gold_stock_analytics
CREATE VIEW IF NOT EXISTS warehouse.gold_stock_analytics AS 
SELECT * FROM warehouse.gold_sentiment_features;

-- Cầu nối cho Dashboard cũ dùng tên csv_data
CREATE VIEW IF NOT EXISTS warehouse.csv_data AS 
SELECT * FROM warehouse.gold_sentiment_features;

-- Cầu nối cho Dashboard dùng tên gold_analytics_summary
CREATE VIEW IF NOT EXISTS warehouse.gold_analytics_summary AS 
SELECT * FROM warehouse.gold_sentiment_features;


--------------------------------------------------------------------------------
-- 3. SPEED LAYER (Real-time Ingestion từ Kafka)
--------------------------------------------------------------------------------

-- A. Bảng đích lưu trữ (Final Storage cho Grafana)
CREATE TABLE IF NOT EXISTS warehouse.stock_realtime
(
    symbol String,
    price Float64,
    volume Float64,
    timestamp DateTime
)
ENGINE = MergeTree()
ORDER BY (symbol, timestamp);

-- B. Kafka Consumer (Đầu hút dữ liệu)
CREATE TABLE IF NOT EXISTS warehouse.kafka_stock_stream
(
    before Tuple(id Int32, symbol String, price Float64, volume Int64, timestamp Int64),
    after Tuple(id Int32, symbol String, price Float64, volume Int64, timestamp Int64),
    op String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'sourcedb.public.finnhub_stock_prices',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'JSONEachRow';

-- C. Materialized View (Cầu nối tự động bơm data)
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.mv_kafka_to_stock
TO warehouse.stock_realtime
AS SELECT
    after.symbol AS symbol,
    after.price AS price,
    after.volume AS volume,
    toDateTime(after.timestamp / 1000) AS timestamp
FROM warehouse.kafka_stock_stream
WHERE op IN ('c', 'r');
