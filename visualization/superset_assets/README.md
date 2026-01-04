## Superset: Hướng dẫn Kết nối Gold Layer từ MinIO (Batch)

Batch: Spark -> MinIO (Iceberg) -> ClickHouse (Iceberg Engine) -> Superset

Hiện tại code Spark (`pycode-spark-gold.py`) đang ghi dữ liệu vào MinIO dưới định dạng **Apache Iceberg**. ClickHouse có thể đọc trực tiếp định dạng này.

**Giả định:** Spark lưu bảng Gold vào bucket `gold`, đường dẫn `s3a://gold/lakehouse.db/analytics_summary` (tên bảng ví dụ dựa trên `spark_utils.py`).

Chạy lệnh SQL sau trong ClickHouse để "mount" dữ liệu từ MinIO về:

SQL

```
-- Bước 1: Tạo bảng kết nối trực tiếp vào MinIO (Iceberg)
CREATE TABLE warehouse.gold_analytics_summary
ENGINE = Iceberg('http://minio:9000/gold/lakehouse.db/analytics_summary', 'admin', 'password')
SETTINGS allow_experimental_iceberg_engine = 1;

-- Lưu ý: Nếu ClickHouse phiên bản cũ chưa hỗ trợ Iceberg, dùng S3 Engine đọc file Parquet:
-- CREATE TABLE warehouse.gold_analytics_summary (...)
-- ENGINE = S3('http://minio:9000/gold/lakehouse.db/analytics_summary/data/*.parquet', 'admin', 'password', 'Parquet');
```

**Cập nhật Superset:**

1. Vào Superset -> Datasets.
2. Thêm Dataset mới từ bảng `warehouse.gold_analytics_summary`.
3. Lúc này Superset sẽ hiển thị báo cáo dựa trên dữ liệu Spark đã xử lý xong thay vì file CSV upload tay.



## Old Demo Reference (Bỏ qua)

**Local CSV -> ClickHouse (Storage/Compute) -> Superset (Visualization).**

### Bước 1: Dựng cấu trúc bảng (Schema) trong ClickHouse

Đoạn script bash ông đưa là chuẩn để khởi tạo. Nó làm việc này:

1. Đợi ClickHouse dậy.
2. Tạo DB `warehouse`.
3. Tạo Table `csv_data` với `MergeTree` engine (tối ưu cho truy vấn phân tích, đọc nhanh).

**Lưu ý quan trọng:** Cột trong câu lệnh `CREATE TABLE` của ông đang có thứ tự: `sentiment_avg` rồi đến `sentiment_sum`. Nhưng file CSV lại là `sentiment_sum` rồi đến `sentiment_avg`. => **Bắt buộc** phải dùng định dạng `CSVWithNames` khi import để ClickHouse tự map theo tên cột, nếu không nó sẽ nạp lộn dữ liệu 2 cột này.

### Bước 2: Nạp dữ liệu (Ingest) từ CSV vào ClickHouse

Superset không nên giữ data gốc, nó chỉ query thôi. Data phải nằm trong ClickHouse. Cách "DevOps" nhất để nạp file `data.csv` từ máy host vào trong container ClickHouse mà không cần copy file lòng vòng là dùng `pipe` của Linux.

Chạy lệnh này tại thư mục chứa file `data.csv`:

Bash

```
cat data.csv | docker exec -i clickhouse-server clickhouse-client \
  --user admin --password password123 \
  --query="INSERT INTO warehouse.csv_data FORMAT CSVWithNames"
```

*Giải thích:*

- `cat data.csv`: Đọc file ở máy thật.
- `|`: Chuyền nội dung file vào stdin của lệnh sau.
- `docker exec -i`: Chế độ interactive để nhận dữ liệu từ pipe.
- `FORMAT CSVWithNames`: Bảo ClickHouse là "Dòng đầu tiên là header, hãy map đúng tên cột cho tao, đừng quan tâm thứ tự cột". **Cái này cực quan trọng để fix vụ lệch cột sentiment.**

### Bước 3: Kết nối Superset với ClickHouse

Giả sử ông đã cài driver (như hội thoại trước ông đã làm `pip install clickhouse-connect`).

1. Vào Superset UI -> **Settings** (icon bánh răng) -> **Database Connections**.
2. Bấm **\+ Database**.
3. Chọn **ClickHouse Connect** (hoặc ClickHouse).
4. Điền thông tin:
	- **Display Name:** `Warehouse_Clickhouse` (gì cũng được).
	- **SQLAlchemy URI:**
		Plaintext
		```
		clickhousedb://admin:password123@clickhouse-server:8123/warehouse
		```
	- *Giải thích URI:* `clickhouse-server` là tên service trong `docker-compose.yaml`. `8123` là cổng HTTP của ClickHouse. `warehouse` là tên DB ông tạo ở Bước 1.
5. Bấm **Test Connection**. Nếu hiện xanh là thông. Bấm **Connect**.

### Bước 4: Tạo Dataset (Lớp ngữ nghĩa)

Superset không query thẳng vào bảng thô khi vẽ chart, nó cần một lớp định nghĩa (Dataset) để ông chỉnh sửa kiểu dữ liệu hoặc tạo cột tính toán ảo (Calculated Columns) nếu cần.

1. Vào tab **Datasets** -> **\+ Dataset**.
2. **Database:** Chọn `Warehouse_Clickhouse`.
3. **Schema:** Chọn `warehouse`.
4. **Table:** Chọn `csv_data`.
5. Bấm **Add**.

### Bước 5: Cách Superset lấy data để hiển thị (Under the hood)

Khi ông kéo thả 1 cái chart (ví dụ: Line chart cho `close` price theo `date`):

1. **Frontend:** Ông chọn trục X là `date`, Metric là `AVG(close)`.
2. **Backend Superset:** Nó sẽ dịch cấu hình đó thành câu SQL ClickHouse:
	SQL
	```
	SELECT toStartOfDay(date) as __timestamp,
	       avg(close) as "AVG(close)"
	FROM warehouse.csv_data
	GROUP BY __timestamp
	ORDER BY __timestamp ASC
	LIMIT 10000;
	```
3. **Transport:** Superset bắn query này qua cổng 8123 vào container ClickHouse.
4. **Compute:** ClickHouse dùng engine MergeTree quét cực nhanh trên ổ cứng/RAM, tính toán và trả về vỏn vẹn vài chục dòng kết quả (JSON).
5. **Render:** Superset nhận JSON đó và vẽ lên biểu đồ.