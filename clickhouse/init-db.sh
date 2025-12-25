#!/bin/bash
# Wait for ClickHouse to wake up
sleep 5
# Create database and table
docker exec -i clickhouse-server clickhouse-client --user admin --password password123 --query "CREATE DATABASE IF NOT EXISTS warehouse; CREATE TABLE IF NOT EXISTS warehouse.csv_data (date String, ticker String, close String, open String, high String, low String, volume String, sentiment_avg String, sentiment_sum String, polarity_ratio String, news_volume String, return_t String, ma_7 String, volatility_7 String, target_next_return String) ENGINE = MergeTree() ORDER BY tuple();"
# clickhousedb://admin:password123@clickhouse-server:8123/warehouse