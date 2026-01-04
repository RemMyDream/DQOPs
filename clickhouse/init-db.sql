-- Create Database
CREATE DATABASE IF NOT EXISTS warehouse;

-- Bước 1: Table cho dữ liệu Gold (Lakehouse - Iceberg)
-- Kết nối trực tiếp vào MinIO, tự động đồng bộ khi Spark job (gold layer) chạy xong.
CREATE TABLE IF NOT EXISTS warehouse.gold_ml_features
ENGINE = Iceberg('http://minio:9000/gold/ml_features', 'admin', 'password')
SETTINGS allow_experimental_iceberg_engine = 1;

-- Bước 2: Table cho dữ liệu Serving Analytics (MergeTree)
-- Tên cũ: csv_data. Bảng này dùng để lưu trữ dữ liệu đã qua xử lý, tối ưu cho Superset query.
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
