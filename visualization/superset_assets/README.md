### Superset: Kết nối Gold Layer từ MinIO (Batch)

Hiện tại code Spark (`pycode-spark-gold.py`) đang ghi dữ liệu vào MinIO dưới định dạng **Apache Iceberg**. ClickHouse có thể đọc trực tiếp định dạng này.

**Giả định:** Spark lưu bảng Gold vào bucket `gold`, đường dẫn `s3a://gold/lakehouse.db/analytics_summary` (tên bảng ví dụ dựa trên `spark_utils.py`).

Chạy lệnh SQL sau trong ClickHouse để "mount" dữ liệu từ MinIO về:

SQL

```
-- Bước 1: Tạo bảng kết nối trực tiếp vào MinIO (Iceberg)
CREATE TABLE warehouse.gold_ml_features
ENGINE = Iceberg('http://minio:9000/gold/ml_features', 'minio_access_key', 'minio_secret_key')
SETTINGS allow_experimental_iceberg_engine = 1;
```

**Cập nhật Superset:**

1. Vào Superset -> Datasets.
2. Thêm Dataset mới từ bảng `warehouse.gold_ml_features`.
3. Lúc này Superset sẽ hiển thị báo cáo dựa trên dữ liệu Spark đã xử lý xong thay vì file CSV upload tay.

# REFERENCE FOR DEMO (BỎ QUA)

**Local CSV -> ClickHouse (Storage/Compute) -> Superset (Visualization).**

### Bước 1: Dựng cấu trúc bảng (Schema) trong ClickHouse

Hệ thống sử dụng bảng **`gold_stock_analytics`** thay thế cho các file CSV tạm thời để đảm bảo tính đồng nhất với kiến trúc Gold Layer.

Chạy lệnh khởi tạo (đã có trong `clickhouse/init-db.sql` hoặc chạy tay):

```sql
CREATE DATABASE IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.gold_stock_analytics (
    date Date,
    ticker String,
    close Float64,
    ...
)
ENGINE = MergeTree()
ORDER BY (ticker, date);
```

### Bước 2: Nạp dữ liệu (Ingest) vào Serving Layer

Cung cấp dữ liệu từ máy host vào ClickHouse:

Bash

```
cat data.csv | docker exec -i clickhouse-server clickhouse-client \
  --user admin --password password123 \
  --query="INSERT INTO warehouse.gold_stock_analytics FORMAT CSVWithNames"
```

_Giải thích:_

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
   - _Giải thích URI:_ `clickhouse-server` là tên service trong `docker-compose.yaml`. `8123` là cổng HTTP của ClickHouse. `warehouse` là tên DB ông tạo ở Bước 1.
5. Bấm **Test Connection**. Nếu hiện xanh là thông. Bấm **Connect**.

### Bước 4: Tạo Dataset (Lớp ngữ nghĩa)

Superset không query thẳng vào bảng thô khi vẽ chart, nó cần một lớp định nghĩa (Dataset) để ông chỉnh sửa kiểu dữ liệu hoặc tạo cột tính toán ảo (Calculated Columns) nếu cần.

1. Vào tab **Datasets** -> **\+ Dataset**.
2. **Database:** Chọn `Warehouse_Clickhouse`.
3. **Schema:** Chọn `warehouse`.
4. **Table:** Chọn `gold_stock_analytics`.
5. Bấm **Add**.

### Bước 5: Cách Superset lấy data để hiển thị (Under the hood)

Khi ông kéo thả 1 cái chart (ví dụ: Line chart cho `close` price theo `date`):

1. **Frontend:** Ông chọn trục X là `date`, Metric là `AVG(close)`.
2. **Backend Superset:** Nó sẽ dịch cấu hình đó thành câu SQL ClickHouse:
   SQL
   ```
   SELECT toStartOfDay(date) as __timestamp,
          avg(close) as "AVG(close)"
   FROM warehouse.gold_stock_analytics
   GROUP BY __timestamp
   ORDER BY __timestamp ASC
   LIMIT 10000;
   ```
3. **Transport:** Superset bắn query này qua cổng 8123 vào container ClickHouse.
4. **Compute:** ClickHouse dùng engine MergeTree quét cực nhanh trên ổ cứng/RAM, tính toán và trả về vỏn vẹn vài chục dòng kết quả (JSON).
5. **Render:** Superset nhận JSON đó và vẽ lên biểu đồ.
