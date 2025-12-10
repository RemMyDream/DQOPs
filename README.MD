chạy code sau trong wsl để có data trong postgres

```bash
sudo apt update
sudo apt install make
make pipeline
```

Sau đó, có thể truy cập psql bẳng:
```bash
make psql

\dt # dùng để xem ds các bảng
\dt <table_name\> #xem các cột của bảng đó
```


MinIO Buckets:
├── warehouse/              ← Iceberg catalog + all bronze tables
│   └── bronze/
│       ├── gdelt_events/
│       ├── gdelt_gkg/
│       └── finnhub_stock_prices/