#!/bin/bash
# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse..."
sleep 5

# Create Database and Table with correct Types
docker exec -i clickhouse-server clickhouse-client --user admin --password password123 --query "
CREATE DATABASE IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.csv_data (
    date Date, 
    ticker String, 
    close Float64, 
    open Float64, 
    high Float64, 
    low Float64, 
    volume Float64, 
    sentiment_avg Float64, 
    sentiment_sum Float64, 
    polarity_ratio Float64, 
    news_volume Float64, 
    return_t Float64 DEFAULT 0, 
    ma_7 Float64 DEFAULT 0, 
    volatility_7 Float64 DEFAULT 0, 
    target_next_return Float64
) 
ENGINE = MergeTree() 
ORDER BY (ticker, date);"

echo "Table warehouse.csv_data created with correct types."
# clickhousedb://admin:password123@clickhouse-server:8123/warehouse