Tài liệu này hướng dẫn cách khôi phục và sử dụng hệ thống Dashboard trên môi trường mới từ source code.

## 1\. Chuẩn bị môi trường (Prerequisites)

Đảm bảo các service sau đang chạy trong Docker:

- **Superset:** Cổng `8088`.
- **Grafana:** Cổng `3000`.
- **ClickHouse:** Cổng `8123` (HTTP) và `9000` (Native).

---

## 2\. Thiết lập Apache Superset (Batch Analytics)

Superset được sử dụng để hiển thị các báo cáo phân tích chuyên sâu (Gold Layer) từ dữ liệu lịch sử.

### Bước 1: Cài đặt Driver kết nối

Truy cập vào container Superset và cài đặt driver ClickHouse (nếu chưa có trong image):

Bash

```
docker exec -it superset pip install clickhouse-connect
docker exec -it superset superset init
```

### Bước 2: Tạo kết nối Database

1. Truy cập UI: `http://localhost:8088` (Admin/Admin).
2. Vào **Settings** (icon bánh răng) → **Database Connections**.
3. Nhấn **\+ Database** → **ClickHouse Connect**.
4. Điền thông tin kết nối:
	- **Display Name:** `ClickHouse` (Bắt buộc đặt đúng tên này để khớp với file export).
	- **SQLAlchemy URI:** \`\`\` clickhousedb://admin:password123@clickhouse-server:8123/warehouse
	- Nhấn **Test Connection** → **Connect**.

### Bước 3: Import Dashboard

1. Vào **Settings** → **Import Dashboards**.
2. Chọn file: `visualization/superset_assets/dashboard_export_20260103T114154.zip`.
3. Nhập mật khẩu database (nếu được hỏi): `password123`.
4. Sau khi import thành công, truy cập tab **Dashboards** → **Quant Trader Dashboard**.

---

## 3\. Thiết lập Grafana (Real-time Monitoring)

Grafana được sử dụng để theo dõi biến động giá cổ phiếu theo thời gian thực (Real-time Layer).

### Bước 1: Cài đặt Plugin

Đảm bảo biến môi trường `GF_INSTALL_PLUGINS` trong file `docker-compose.yml` có chứa `grafana-clickhouse-datasource`.

### Bước 2: Cấu hình Data Source

1. Truy cập UI: `http://localhost:3000` (admin/admin).
2. Vào **Connections** → **Data Sources** → **Add data source**.
3. Tìm và chọn **ClickHouse**.
4. Cấu hình:
	- **Name:** `ClickHouse-Realtime` (Đặt tên này để khớp với file JSON).
	- **Server Address:** `clickhouse-server` (hoặc `host.docker.internal` nếu chạy local).
	- **Server Port:** `8123`.
	- **Protocol:** `HTTP`.
	- **Username/Password:** `admin` / `password123`.
	- **Default Database:** `warehouse`.
5. Nhấn **Save & Test**. Nếu hiện *"Data source is working"* là thành công.

### Bước 3: Import Dashboard

1. Vào menu **Dashboards** → **New** → **Import**.
2. Cách 1: Upload file JSON từ đường dẫn `visualization/grafana_dashboards/realtime_stock_dashboard.json`.
3. Cách 2: Copy toàn bộ nội dung file JSON và paste vào ô "Import via panel json".
4. Tại mục **Select a ClickHouse data source**, chọn `ClickHouse-Realtime` vừa tạo.
5. Nhấn **Import**.

---

## 4\. Lưu ý khi Demo (Troubleshooting)

- **Trạng thái "No Data":** Nếu pipeline chưa chạy (Spark/Kafka chưa đẩy dữ liệu), các biểu đồ sẽ hiện khung nhưng không có dữ liệu.
- **Khắc phục tạm thời:** Có thể chạy script `visualization/mock_stream.py` (như đã đề cập trong phần Dev) để bơm dữ liệu giả lập vào ClickHouse, giúp Dashboard hiển thị đẹp mắt phục vụ demo giao diện.