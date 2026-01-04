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