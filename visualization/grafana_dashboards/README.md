## Grafana: Hướng dẫn Kết nối Real-time từ Kafka

Speed: Finnhub -> Postgres -> Debezium -> Kafka -> ClickHouse (Kafka Engine -> MV) -> Grafana

Trong file `kafka/connector_configs/demo_configs.json`, bạn đang dùng Debezium để bắt thay đổi từ bảng Postgres `finnhub_stock_prices` và bắn vào Kafka topic `sourcedb.public.finnhub_stock_prices`.

Để ClickHouse tự động hút dữ liệu này (thay thế script `mock_stream.py`), bạn cần tạo một "đường ống" trong ClickHouse:

#### Bước 1: Tạo bảng Kafka Consumer (Đầu hút)

Bảng này chỉ làm nhiệm vụ nhận tin từ Kafka, không lưu trữ lâu dài.

SQL

```
CREATE TABLE warehouse.kafka_stock_stream
(
    before Tuple(id Int32, price Float64, ...), -- Cấu trúc Debezium
    after Tuple(id Int32, symbol String, price Float64, volume Int64, timestamp Int64),
    op String -- Loại thao tác (c=create, u=update...)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'sourcedb.public.finnhub_stock_prices',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'AvroConfluent', -- Vì config Debezium dùng Avro
         kafka_schema_registry_url = 'http://schema-registry.dqops.svc.cluster.local:8081';
```

*Lưu ý: Nếu gặp lỗi Avro phức tạp, hãy chỉnh config Debezium về JSON (`value.converter`: `JsonConverter`) và đổi `kafka_format` thành `JSONEachRow`.*

#### Bước 2: Tạo bảng đích (Kho chứa)

Đây là bảng `stock_realtime` mà Grafana đang query. Chúng ta tái sử dụng nó nhưng sửa lại cấu trúc cho khớp dữ liệu thật.

SQL

```
CREATE TABLE warehouse.stock_realtime_final
(
    symbol String,
    price Float64,
    volume Float64,
    timestamp DateTime
)
ENGINE = MergeTree()
ORDER BY (symbol, timestamp);
```

#### Bước 3: Tạo Materialized View (Bơm tự động)

MV sẽ tự động chuyển dữ liệu từ Kafka Stream sang bảng đích.

SQL

```
CREATE MATERIALIZED VIEW warehouse.mv_kafka_to_stock
TO warehouse.stock_realtime_final
AS SELECT
    after.symbol AS symbol,
    after.price AS price,
    after.volume AS volume,
    toDateTime(after.timestamp / 1000) AS timestamp -- Chuyển ms sang giây
FROM warehouse.kafka_stock_stream
WHERE op IN ('c', 'r'); -- Chỉ lấy thao tác Create hoặc Read (snapshot)
```

### T





# Old Demo Reference (bỏ qua)

### 1\. Kiến trúc tổng quan

- **Hiện tại (Simulation/Mock):** `Python Script (Loop)` -> `ClickHouse (MergeTree Table)` -> `Grafana` *(Mô phỏng dòng chảy dữ liệu bằng cách insert liên tục mỗi giây).*
- **Tương lai (Production/Real):** `Finnhub API` -> `Postgres` -> `Debezium` -> `Kafka` -> `ClickHouse (Kafka Engine -> MV -> MergeTree)` -> `Grafana` *(Dữ liệu thật chảy qua đường ống, ClickHouse tự động hút từ Kafka).*

---

### 2\. Chi tiết triển khai hiện tại (Mock Data)

#### A. Kho chứa dữ liệu (ClickHouse)

Nơi đón nhận mọi dữ liệu (dù là mock hay thật).

- **Database:** `warehouse`
- **Table:** `stock_realtime`
- **Engine:** `MergeTree` (Engine cơ bản nhất để lưu trữ và truy vấn nhanh).
- **Schema (Cấu trúc):**
	- `symbol` (String): Mã cổ phiếu (VD: MSFT, AAPL).
	- `price` (Float64): Giá tại thời điểm khớp lệnh.
	- `volume` (Float64): Khối lượng khớp.
	- `timestamp` (DateTime): Thời gian ghi nhận (UTC).

#### B. Bộ bơm dữ liệu (Python Mock Script)

Thay thế tạm thời cho toàn bộ cụm Kafka/Debezium phức tạp.

- **Vị trí:** `visualization/mock_stream.py`
- **Cơ chế:**
	1. Khởi tạo giá gốc cho các mã (MSFT: 400, AAPL: 180...).
	2. Vòng lặp vô hạn (`while True`).
	3. Mỗi giây, tạo ngẫu nhiên một biến động giá (`random.uniform`) và khối lượng.
	4. Gắn nhãn thời gian hiện tại (`datetime.utcnow()`).
	5. Dùng thư viện `clickhouse-connect` thực hiện lệnh `INSERT` trực tiếp vào bảng `stock_realtime`.

#### C. Hiển thị (Grafana Dashboard)

Nơi biến dữ liệu thô thành biểu đồ nến.

1. **Kết nối (Connection):**
	- Grafana kết nối tới ClickHouse qua plugin `grafana-clickhouse-datasource`.
	- Address: `clickhouse` (tên service trong Docker) hoặc `host.docker.internal`.
	- Port: `8123` (HTTP).
2. **Biến động (Dynamic Variable):**
	- Tên biến: `${symbol}`.
	- Query lấy danh sách: `SELECT DISTINCT symbol FROM warehouse.stock_realtime`.
	- Giúp người dùng chuyển đổi qua lại giữa các mã cổ phiếu mà không cần sửa code.
3. **Truy vấn vẽ nến (Candlestick Query):** Do dữ liệu đầu vào là từng dòng lệnh (tick data), Grafana cần gộp (aggregate) chúng lại thành nến 1 phút.
	- **Logic:**
		- `toStartOfInterval(timestamp, 1 minute)`: Gom tất cả các giao dịch trong cùng 1 phút lại (Ví dụ: 10:01:05 -> 10:01:00).
		- `argMin(price, timestamp)` -> **Open**: Lấy giá của giao dịch có thời gian sớm nhất trong phút đó.
		- `max(price)` -> **High**: Giá cao nhất trong phút.
		- `min(price)` -> **Low**: Giá thấp nhất trong phút.
		- `argMax(price, timestamp)` -> **Close**: Lấy giá của giao dịch muộn nhất trong phút.
	- **Bộ lọc thời gian:** Dùng biến toàn cục `$__from` và `$__to` của Grafana để cắt dữ liệu đúng khung hình người xem đang nhìn.