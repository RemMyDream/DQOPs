from datetime import timedelta
from feast import Entity, Field, FeatureView, FileSource, ValueType
from feast.types import Float32, Int64

# 1. Định nghĩa Entity (Đối tượng chính: Mã chứng khoán)
stock = Entity(name="stock_symbol", value_type=ValueType.STRING, description="Mã cổ phiếu")

# 2. Định nghĩa Nguồn dữ liệu (File Parquet do Spark tạo ra từ GDELT/Finnhub)
stock_stats_source = FileSource(
    name="stock_stats_source",
    path="/path/to/your/data/stock_features.parquet",  # Đường dẫn file parquet
    timestamp_field="event_timestamp",
)

# 3. Định nghĩa Feature View (Nhóm các features)
stock_features = FeatureView(
    name="stock_features",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_sentiment_score", dtype=Float32),
        Field(name="closing_price", dtype=Float32),
        Field(name="volatility_7d", dtype=Float32),
    ],
    source=stock_stats_source,
    online=True,
)