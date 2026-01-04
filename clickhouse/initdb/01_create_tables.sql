-- Tạo database
CREATE DATABASE IF NOT EXISTS warehouse;

-- Bảng engine Kafka để đọc trực tiếp từ Kafka topic
CREATE TABLE IF NOT EXISTS warehouse.stock_prices_kafka
(
    payload String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka-node-1:9092',
    kafka_topic_list = 'sourcedb.public.finnhub_stock_prices',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_skip_broken_messages = 10;

-- Bảng MergeTree để lưu data thực
CREATE TABLE IF NOT EXISTS warehouse.stock_prices_raw
(
    symbol String,
    current_price Float64,
    high Float64,
    low Float64,
    open Float64,
    previous_close Float64,
    change Float64,
    percent_change Float64,
    timestamp DateTime64(6),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
SETTINGS index_granularity = 8192;

-- Materialized View để parse JSON và insert vào bảng raw
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.stock_prices_consumer TO warehouse.stock_prices_raw
AS SELECT
    JSONExtractString(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'symbol', 'string') AS symbol,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'current_price', 'double') AS current_price,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'high', 'double') AS high,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'low', 'double') AS low,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'open', 'double') AS open,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'previous_close', 'double') AS previous_close,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'change', 'double') AS change,
    JSONExtractFloat(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'percent_change', 'double') AS percent_change,
    fromUnixTimestamp64Micro(JSONExtractUInt(JSONExtractRaw(payload, 'value'), 'after', 'sourcedb.public.finnhub_stock_prices.Value', 'timestamp', 'long')) AS timestamp
FROM warehouse.stock_prices_kafka
WHERE JSONExtractString(payload, 'value', 'op') = 'c'
  AND symbol != '';

CREATE TABLE IF NOT EXISTS warehouse.stock_candles_1min
(
    symbol String,
    time_bucket DateTime,
    open SimpleAggregateFunction(any, Float64),
    high SimpleAggregateFunction(max, Float64),
    low SimpleAggregateFunction(min, Float64),
    close SimpleAggregateFunction(anyLast, Float64),
    volume SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (symbol, time_bucket)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.stock_candles_1min_mv TO warehouse.stock_candles_1min
AS SELECT
    symbol,
    toStartOfMinute(timestamp) AS time_bucket,
    anyState(current_price) AS open,
    maxState(high) AS high,
    minState(low) AS low,
    anyLastState(current_price) AS close,
    sumState(toUInt64(1)) AS volume
FROM warehouse.stock_prices_raw
GROUP BY symbol, time_bucket;

CREATE VIEW IF NOT EXISTS warehouse.stock_candles_view AS
SELECT
    symbol,
    time_bucket,
    anyMerge(open) AS open,
    maxMerge(high) AS high,
    minMerge(low) AS low,
    anyLastMerge(close) AS close,
    sumMerge(volume) AS volume
FROM warehouse.stock_candles_1min
GROUP BY symbol, time_bucket
ORDER BY symbol, time_bucket;