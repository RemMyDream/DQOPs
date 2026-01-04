-- Create Database
CREATE DATABASE IF NOT EXISTS warehouse;

--------------------------------------------------------------------------------
-- 1. BATCH LAYER (Iceberg - Zero Copy)
--------------------------------------------------------------------------------
-- Kết nối trực tiếp vào MinIO Gold Layer. 
-- Tự động có dữ liệu sau khi Spark job 'gold' chạy xong.
CREATE TABLE IF NOT EXISTS warehouse.gold_ml_features
ENGINE = Iceberg('http://minio:9000/gold/ml_features', 'admin', 'password')
SETTINGS allow_experimental_iceberg_engine = 1;


--------------------------------------------------------------------------------
-- 2. SERVING LAYER (MergeTree - High Performance)
--------------------------------------------------------------------------------
-- Dùng cho các dashboard cần truy vấn nhanh, nạp tin từ CSV hoặc dự liệu đã aggregate.
-- Thay thế cho bảng 'csv_data' cũ.
CREATE TABLE IF NOT EXISTS warehouse.gold_stock_analytics (
    date Date, 
    ticker String, 
    close Float64, 
    open Float64, 
    high Float64, 
    low Float64, 
    volume Float64, 
    sentiment_avg Float64 DEFAULT 0, 
    sentiment_sum Float64 DEFAULT 0, 
    polarity_ratio Float64 DEFAULT 0.5, 
    news_volume Float64 DEFAULT 0, 
    return_t Float64 DEFAULT 0, 
    ma_7 Float64 DEFAULT 0, 
    volatility_7 Float64 DEFAULT 0, 
    target_next_return Float64 DEFAULT 0
) 
ENGINE = MergeTree() 
ORDER BY (ticker, date);


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

-- B. Kafka Consumer (Đầu hút dữ liệu - nhận từ CDC Postgres qua Debezium)
-- Lưu ý: Engine Kafka không lưu data, nó chỉ là interface để lấy tin từ topic.
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
         -- Sử dụng JSONEachRow để dễ debug, có thể đổi sang AvroConfluent nếu dùng Schema Registry

-- C. Materialized View (Cầu nối tự động bơm data)
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.mv_kafka_to_stock
TO warehouse.stock_realtime
AS SELECT
    after.symbol AS symbol,
    after.price AS price,
    after.volume AS volume,
    toDateTime(after.timestamp / 1000) AS timestamp
FROM warehouse.kafka_stock_stream
WHERE op IN ('c', 'r'); -- 'c' = Create, 'r' = Read (snapshot)
