# Error Sampler Template Documentation

## 📖 Tổng quan

Template Jinja2 này được thiết kế cho **Data Quality Monitoring** - giúp phát hiện và lấy mẫu các dòng dữ liệu có lỗi để kiểm tra, debug và báo cáo.

**Ngôn ngữ SQL hỗ trợ**: SparkSQL/Databricks

---

## 📋 Mục lục

- [Cú pháp sử dụng](#-cú-pháp-sử-dụng)
- [Tham số của Macro](#-input---tham-số-của-macro)
- [Output và Ý nghĩa](#-output---ý-nghĩa-các-cột-kết-quả)
- [Context Variables](#-context-variables)
- [Macro Helper](#-các-macro-helper-khác)
- [Ví dụ đầy đủ](#-ví-dụ-đầy-đủ)

---

## 🎯 Cú pháp sử dụng

### Cú pháp cơ bản

```jinja2
{% call render_error_sampler() %}
    price < 0
{% endcall %}
```

### Với tham số tùy chỉnh

```jinja2
{% call render_error_sampler(
    wrap_condition='AND',
    render_null_check=false,
    override_samples_limit=50,
    value_order_by='DESC'
) %}
    price < 0 OR price > 1000000
{% endcall %}
```

### Với order by expression

```jinja2
{% call render_error_sampler(
    order_by_expression='ABS({column} - 100)',
    value_order_by='ASC'
) %}
    status = 'invalid'
{% endcall %}
```

---

## 📥 INPUT - Tham số của Macro

### `render_error_sampler()` Parameters

| Tham số | Kiểu | Mặc định | Mô tả |
|---------|------|----------|-------|
| `wrap_condition` | string | `''` | Điều kiện kết nối (AND/OR) trước caller block |
| `render_null_check` | boolean | `true` | Có lọc bỏ NULL trước khi kiểm tra lỗi |
| `override_samples_limit` | int/none | `none` | Ghi đè số mẫu mỗi nhóm |
| `value_order_by` | string | `'ASC'` | Sắp xếp mẫu lỗi (ASC/DESC) |
| `sample_index` | boolean | `true` | Thêm cột đánh số thứ tự mẫu |
| `order_by_value_first` | boolean | `true` | Ưu tiên sắp xếp theo giá trị trước ID |
| `order_by_expression` | string | `''` | Biểu thức sắp xếp tùy chỉnh (dùng `{column}`) |

### Ý nghĩa chi tiết các tham số

#### `wrap_condition`
**Mục đích**: Kết nối điều kiện của bạn với điều kiện khác

```sql
-- Với wrap_condition='AND'
WHERE status = 'active' AND (price < 0)

-- Không có wrap_condition
WHERE (price < 0)
```

**Khi nào dùng**: Khi muốn kiểm tra lỗi chỉ trên subset dữ liệu (VD: chỉ kiểm tra đơn hàng đã hoàn thành)

#### `render_null_check`
**Mục đích**: Có lọc bỏ giá trị NULL trước khi kiểm tra lỗi hay không

```sql
-- render_null_check=true
WHERE price IS NOT NULL AND (price < 0)

-- render_null_check=false
WHERE (price < 0)
```

**Khi nào dùng**:
- ✅ `true`: Khi NULL là giá trị hợp lệ, chỉ tìm lỗi trong các giá trị có dữ liệu
- ✅ `false`: Khi muốn kiểm tra cả NULL (VD: kiểm tra foreign key không tồn tại)

#### `override_samples_limit`
**Mục đích**: Giới hạn số mẫu lỗi lấy về

**Tại sao cần**: 
- Có thể có hàng triệu dòng lỗi
- Chỉ cần 10-50 mẫu để hiểu vấn đề
- Tránh query chậm và tốn bộ nhớ

**Ví dụ**:
```
Có 1 triệu email sai format
→ Chỉ lấy 50 mẫu đầu tiên
→ Đủ để developer hiểu pattern lỗi
```

#### `value_order_by`
**Mục đích**: Sắp xếp các mẫu lỗi theo giá trị tăng/giảm dần

**Use case**:
- `ASC`: Xem các giá trị nhỏ nhất (VD: giá âm thấp nhất)
- `DESC`: Xem các giá trị lớn nhất (VD: tuổi không hợp lý cao nhất)

**Ví dụ**:
```
Lỗi: age > 150
→ ASC: [151, 152, 155, ...] - các giá trị vừa vượt ngưỡng
→ DESC: [999, 888, 500, ...] - các outlier cực đoan
```

#### `sample_index`
**Mục đích**: Thêm cột đánh số thứ tự mẫu trong mỗi nhóm

**Output**:
```
actual_value | sample_index | grouping_country
-------------|--------------|------------------
invalid.com  | 1            | US
bad.email    | 2            | US
wrong.vn     | 1            | VN
```

**Khi nào dùng**: 
- ✅ Khi có grouping - biết đây là mẫu thứ mấy trong nhóm
- ❌ Tắt đi nếu không cần để query nhanh hơn

#### `order_by_value_first`
**Mục đích**: Quyết định sắp xếp theo giá trị hay ID trước

**Ví dụ**:
```sql
-- order_by_value_first=true
ORDER BY price ASC, id ASC

-- order_by_value_first=false  
ORDER BY id ASC, price ASC
```

**Khi nào dùng**:
- ✅ `true`: Muốn nhóm các giá trị giống nhau lại (VD: tất cả giá -100 gần nhau)
- ✅ `false`: Muốn giữ thứ tự thời gian insert (qua ID)

#### `order_by_expression`
**Mục đích**: Sắp xếp theo công thức phức tạp thay vì giá trị trực tiếp

**Use case thực tế**:

**Ví dụ 1**: Tìm outlier gần ngưỡng nhất
```jinja2
order_by_expression='ABS({column} - 0)'
```
→ Tìm giá âm gần 0 nhất (VD: -0.5, -1.2) thay vì xa nhất (-9999)

**Ví dụ 2**: Độ dài string bất thường
```jinja2
order_by_expression='LENGTH({column})'
```
→ Sắp xếp email lỗi theo độ dài (email ngắn/dài bất thường)

---

## 📤 OUTPUT - Ý nghĩa các cột kết quả

### Kịch bản 1: Không có Data Grouping

**SQL Output**:
```sql
SELECT
    analyzed_table.price as actual_value,
    analyzed_table.id AS row_id_1
FROM products AS analyzed_table
WHERE price < 0
LIMIT 10
```

**Kết quả**:
```
actual_value | row_id_1
-------------|----------
-150.00      | 1234
-50.00       | 5678
-10.50       | 9012
```

#### Ý nghĩa các cột:

| Cột | Ý nghĩa | Công dụng |
|-----|---------|-----------|
| `actual_value` | Giá trị lỗi thực tế | Xem dữ liệu lỗi là gì |
| `row_id_1` | ID của dòng lỗi | Trace ngược lại record gốc để sửa |

**Use case**: 
```
→ Thấy giá -150.00 ở row 1234
→ Tra cứu: SELECT * FROM products WHERE id = 1234
→ Sửa lỗi hoặc phân tích nguyên nhân
```

### Kịch bản 2: Có Data Grouping

**SQL Output**:
```sql
SELECT
    sample_table.sample_value AS actual_value,
    sample_table.sample_index AS sample_index,
    sample_table.grouping_country AS grouping_country,
    sample_table.row_id_1 AS row_id_1
FROM (...)
WHERE sample_index <= 5
LIMIT 1000
```

**Kết quả**:
```
actual_value     | sample_index | grouping_country | row_id_1
-----------------|--------------|------------------|----------
invalid.email    | 1            | US               | 100
badformat        | 2            | US               | 250
noatsign         | 3            | US               | 301
wrongemail       | 1            | VN               | 340
notemail         | 2            | VN               | 567
missing.domain   | 1            | JP               | 789
```

#### Ý nghĩa các cột:

| Cột | Ý nghĩa | Công dụng |
|-----|---------|-----------|
| `actual_value` | Email lỗi | Xem pattern lỗi |
| `sample_index` | Mẫu thứ mấy trong nhóm | Biết đây là top 1, 2, 3... trong nhóm |
| `grouping_country` | Nhóm theo quốc gia | Phân tích lỗi theo từng thị trường |
| `row_id_1` | ID gốc | Trace lại record |

**Tại sao cần grouping**:

```
Không group:
→ Lấy 10 mẫu: có thể cả 10 đều từ US
→ Không biết VN, JP có lỗi không

Có group (5 mẫu/nhóm):
→ 5 mẫu từ US
→ 5 mẫu từ VN  
→ 5 mẫu từ JP
→ Thấy được lỗi phân bố ở tất cả thị trường
```

### Về `row_id_1`, `row_id_2`, ...

**Quan trọng**: `row_id_N` **chỉ copy nguyên xi giá trị** từ cột ID mà bạn chỉ định trong `error_sampling.id_columns[N-1]`, không có xử lý hay chuyển đổi gì cả.

**Kiểu dữ liệu**: Giữ nguyên kiểu của cột ID gốc

| Kiểu cột ID | Ví dụ giá trị `row_id_1` |
|-------------|--------------------------|
| INT, BIGINT | `1234`, `5678` |
| VARCHAR, TEXT | `'ORD_2024_1234'`, `'INV_001'` |
| UUID | `'550e8400-e29b-41d4-a716-446655440000'` |

**Composite Key** (nhiều cột ID):
```python
error_sampling = {
    'id_columns': ['customer_id', 'order_date', 'item_id']
}
```

Output sẽ có:
- `row_id_1` = giá trị của `customer_id`
- `row_id_2` = giá trị của `order_date`
- `row_id_3` = giá trị của `item_id`

---

## 🗂️ Context Variables

Template này cần **9 nhóm biến context chính** để hoạt động hiệu quả trong quá trình sinh SQL, lọc dữ liệu, và phân tích theo thời gian.

---

### 1️⃣ `target_table` *(BẮT BUỘC)*

**Mục đích:**
Xác định bảng nguồn cần phân tích.

**Template:**

```python
target_table = {
    'schema_name': str,   # Tên schema hoặc database chứa bảng
    'table_name': str     # Tên bảng dữ liệu nguồn
}
```

**Ví dụ:**

```python
target_table = {
    'schema_name': 'public',
    'table_name': 'orders'
}
```

**Được dùng trong**: 
- `render_target_table()` → `public.orders`

---

### 2️⃣ `table` *(BẮT BUỘC)*

**Mục đích:**
Mô tả metadata của bảng và các cột được dùng trong phân tích.

**Template:**

```python
table = {
    'filter': str | None,    # (Optional) Điều kiện lọc mặc định cho toàn bảng
    
    'columns': {
        '<column_name>': {
            'type_snapshot': {
                'column_type': str   # Kiểu dữ liệu (INT, VARCHAR, DECIMAL, DATE, TIMESTAMP, ...)
            },
            'sql_expression': str | None   # Biểu thức SQL nếu là computed column, None nếu cột thông thường
        },
        # ... các cột khác
    }
}
```

**Ví dụ:**

```python
table = {
    'filter': 'status != "deleted"',
    'columns': {
        'order_id': {
            'type_snapshot': {'column_type': 'VARCHAR'}, 
            'sql_expression': None
        },
        'total_price': {
            'type_snapshot': {'column_type': 'DECIMAL'}, 
            'sql_expression': None
        },
        'created_at': {
            'type_snapshot': {'column_type': 'TIMESTAMP'}, 
            'sql_expression': None
        },
        'customer_region': {
            'type_snapshot': {'column_type': 'VARCHAR'}, 
            'sql_expression': None
        },
        # Ví dụ computed column
        'full_name': {
            'type_snapshot': {'column_type': 'VARCHAR'},
            'sql_expression': "CONCAT({alias}.first_name, ' ', {alias}.last_name)"
        }
    }
}
```

**Lưu ý**:
- `sql_expression`: Dùng cho computed columns, có thể dùng placeholder `{alias}`, `{table}`, `{column}`
- `column_type`: Các giá trị hợp lệ: `'INT'`, `'BIGINT'`, `'VARCHAR'`, `'TEXT'`, `'DECIMAL'`, `'DATE'`, `'TIMESTAMP'`, etc.
- Tất cả các cột được dùng trong `error_sampling.id_columns`, `data_groupings`, `time_series` phải được định nghĩa trong `table.columns`

---

### 3️⃣ `column_name` *(BẮT BUỘC)*

**Mục đích:**
Tên cột đang được phân tích hoặc làm target.

**Template:**

```python
column_name = str   # Tên cột trong bảng
```

**Ví dụ:**

```python
column_name = 'total_price'
```

**Được dùng trong**:
- `render_target_column()` → `analyzed_table.total_price`
- Truy cập metadata: `table.columns[column_name]`

---

### 4️⃣ `error_sampling` *(BẮT BUỘC)*

**Mục đích:**
Cấu hình lấy mẫu lỗi để hiển thị ví dụ khi kiểm tra dữ liệu.

**Template:**

```python
error_sampling = {
    'samples_limit': int,           # Số mẫu tối đa MỖI NHÓM (khi có grouping)
    'total_samples_limit': int,     # Tổng số mẫu tối đa (tránh query quá lớn)
    'id_columns': list[str]         # Danh sách các cột định danh bản ghi
}
```

**Ví dụ:**

```python
# Single column ID
error_sampling = {
    'samples_limit': 5,
    'total_samples_limit': 1000,
    'id_columns': ['order_id']
}

# Composite key
error_sampling = {
    'samples_limit': 10,
    'total_samples_limit': 5000,
    'id_columns': ['customer_id', 'order_date', 'item_id']
}
```

**Lưu ý**:
- Tất cả các cột trong `id_columns` phải được định nghĩa trong `table.columns`
- Output sẽ có `row_id_1`, `row_id_2`, ... tương ứng với từng cột ID

---

### 5️⃣ `data_groupings` *(OPTIONAL)*

**Mục đích:**
Nhóm dữ liệu theo các chiều (dimensions) hoặc giá trị cố định.

**Template:**

```python
data_groupings = dict | None   # None hoặc {} nếu không có grouping

# Khi có grouping:
data_groupings = {
    '<attribute_name>': {
        'source': str,      # 'column_value' | 'tag'
        'column': str,      # (Khi source='column_value') Tên cột để nhóm
        'tag': str          # (Khi source='tag') Giá trị cố định
    },
    # ... các attributes khác
}
```

**Ví dụ:**

```python
# Không có grouping
data_groupings = None

# Grouping theo cột
data_groupings = {
    'region': {
        'source': 'column_value',
        'column': 'customer_region'
    },
    'country': {
        'source': 'column_value',
        'column': 'country_code'
    }
}

# Grouping theo tag (hằng số)
data_groupings = {
    'environment': {
        'source': 'tag',
        'tag': 'production'
    }
}

# Kết hợp cả hai
data_groupings = {
    'region': {
        'source': 'column_value',
        'column': 'customer_region'
    },
    'environment': {
        'source': 'tag',
        'tag': 'production'
    }
}
```

**Lưu ý:**
- Nếu `source = 'column_value'` → thêm cột `grouping_<attribute_name>` lấy từ giá trị cột
- Nếu `source = 'tag'` → gán giá trị cố định cho toàn bộ bản ghi
- Tất cả các cột trong `data_groupings` phải được định nghĩa trong `table.columns`
- Use case của `tag`: Khi merge data từ nhiều environment/source

---

### 6️⃣ `time_series` *(OPTIONAL)*

**Mục đích:**
Cấu hình phân tích dữ liệu theo thời gian.

**Template:**

```python
time_series = dict | None   # None nếu không dùng time series

# Khi dùng:
time_series = {
    'mode': str,                # 'current_time' | 'timestamp_column'
    'timestamp_column': str,    # (Khi mode='timestamp_column') Tên cột thời gian
    'time_gradient': str        # 'hour' | 'day' | 'week' | 'month' | 'quarter' | 'year'
}
```

**Ví dụ:**

```python
# Không dùng time series
time_series = None

# Mode 1: Dùng current_time (snapshot hiện tại)
time_series = {
    'mode': 'current_time',
    'time_gradient': 'day'
}

# Mode 2: Dùng timestamp_column (phổ biến nhất)
time_series = {
    'mode': 'timestamp_column',
    'timestamp_column': 'created_at',
    'time_gradient': 'day'
}
```

**Lưu ý:**
- `current_time`: Dùng khi phân tích snapshot hiện tại, tất cả records có cùng `time_period`
- `timestamp_column`: Dùng khi có cột thời gian rõ ràng, nhóm theo `DATE_TRUNC('day', created_at)`
- Cột `timestamp_column` phải được định nghĩa trong `table.columns` với `column_type` là `'DATE'` hoặc `'TIMESTAMP'`
- Output sẽ có thêm 2 cột: `time_period` và `time_period_utc`

---

### 7️⃣ `time_window_filter` *(OPTIONAL)*

**Mục đích:**
Giới hạn khoảng thời gian được phân tích.

**Template:**

```python
time_window_filter = dict | None   # None nếu không lọc thời gian

# Khi có lọc thời gian:
time_window_filter = {
    # Lọc theo khoảng cố định (chọn 1 trong 3 cặp)
    'from_date': str | None,                     # Ngày bắt đầu (YYYY-MM-DD)
    'to_date': str | None,                       # Ngày kết thúc (YYYY-MM-DD)
    
    'from_date_time': str | None,                # Datetime bắt đầu (YYYY-MM-DD HH:MM:SS)
    'to_date_time': str | None,                  # Datetime kết thúc (YYYY-MM-DD HH:MM:SS)
    
    'from_date_time_offset': str | None,         # Datetime + timezone (ISO 8601)
    'to_date_time_offset': str | None,           # Datetime + timezone (ISO 8601)
    
    # Lọc theo khoảng động
    'daily_partitioning_recent_days': int | None,           # Số ngày gần nhất
    'monthly_partitioning_recent_months': int | None,       # Số tháng gần nhất
    
    # Flags
    'daily_partitioning_include_today': bool,               # Có tính hôm nay không
    'monthly_partitioning_include_current_month': bool      # Có tính tháng hiện tại không
}
```

**Ví dụ:**

```python
# Không lọc thời gian
time_window_filter = None

# Lọc theo khoảng cố định
time_window_filter = {
    'from_date': '2024-01-01',
    'to_date': '2024-12-31'
}

# Lọc theo datetime
time_window_filter = {
    'from_date_time': '2024-01-01 00:00:00',
    'to_date_time': '2024-12-31 23:59:59'
}

# Lọc theo timezone offset
time_window_filter = {
    'from_date_time_offset': '2024-01-01T00:00:00+07:00',
    'to_date_time_offset': '2024-12-31T23:59:59+07:00'
}

# Lọc 7 ngày gần nhất (không tính hôm nay)
time_window_filter = {
    'daily_partitioning_recent_days': 7,
    'daily_partitioning_include_today': False
}

# Lọc 3 tháng gần nhất (không tính tháng hiện tại)
time_window_filter = {
    'monthly_partitioning_recent_months': 3,
    'monthly_partitioning_include_current_month': False
}

# Kết hợp: từ ngày cố định đến hôm nay
time_window_filter = {
    'from_date': '2024-01-01',
    'daily_partitioning_include_today': True
}
```

**Lưu ý:**
- Chỉ hoạt động khi có `time_series.timestamp_column` được định nghĩa
- Ưu tiên: `*_offset` > `*_date_time` > `*_date`
- Với `daily_partitioning_recent_days=7, include_today=False`: Query sẽ lấy từ 7 ngày trước đến hôm qua

---

### 8️⃣ `parameters` *(OPTIONAL)*

**Mục đích:**
Lưu các tham số bổ sung hoặc điều kiện mở rộng.

**Template:**

```python
parameters = {
    'filter': str | None,             # Filter bổ sung ngoài table.filter
    'foreign_table': str | None,      # Tên bảng foreign (nếu có) - có thể là 'table_name' hoặc 'schema.table_name'
    'foreign_column': str | None      # Tên cột trong bảng foreign (không cần định nghĩa trong table.columns)
}
```

**Ví dụ:**

```python
# Chỉ có filter bổ sung
parameters = {
    'filter': 'amount > 0'
}

# Kiểm tra foreign key
parameters = {
    'filter': 'amount > 0',
    'foreign_table': 'categories',
    'foreign_column': 'id'
}

# Foreign table với schema
parameters = {
    'foreign_table': 'public.categories',
    'foreign_column': 'id'
}
```

**Lưu ý:**
- `filter` sẽ được kết hợp với `table.filter` và `additional_filters` bằng AND
- Khi dùng `foreign_table`, template sẽ tự động tạo LEFT JOIN để kiểm tra foreign key constraint
- `foreign_column` không cần phải có trong `table.columns`

---

### 9️⃣ `additional_filters` *(OPTIONAL)*

**Mục đích:**
Thêm nhiều điều kiện lọc động (ngoài filter chính).

**Template:**

```python
additional_filters = list[str]   # Danh sách biểu thức SQL hợp lệ
```

**Ví dụ:**

```python
# Không có additional filters
additional_filters = []

# Có nhiều filters
additional_filters = [
    'price > 0',
    'status IN ("pending", "active")',
    'created_at >= CURRENT_DATE() - 30'
]
```

**Lưu ý:**
- Tất cả filters được kết hợp bằng AND trong WHERE clause
- Thứ tự kết hợp: `table.filter` AND `parameters.filter` AND `additional_filters[0]` AND `additional_filters[1]` ...
- Có thể dùng placeholder `{column}`, `{table}`, `{alias}` trong filter string

---
