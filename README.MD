Quick notes

- Setup once: `make setup` (downloads deps) → `make start` (docker up).
- Main pipeline: `make pipeline` (ingest + bronze + silver + gold).
- Focused runs: `make ingest` (Finnhub → Postgres), `make bronze` (GDELT + Stooq → MinIO), `make silver`, `make gold`.
- Utilities: `make psql` to open Postgres, `make check-pg` to see row counts, `make check-bronze` to list bronze tables.

MinIO layout (Iceberg tables)

- bronze/ → gdelt_events, gdelt_gkg, stooq_stock_prices
- silver/ → gdelt_events, gdelt_gkg, stooq_stock_prices
- gold/ →

Data flow (current)

- Finnhub: API → Postgres via `make ingest` (kept in Postgres; optional `make finnhub-to-minio` later).
- GDELT (events, gkg) + Stooq: API → Spark bronze job (`make bronze`) → MinIO bucket `warehouse/bronze/...`.

helm command:

```bash
# From /mnt/c/Coding/bdsp-test/
helm upgrade --install bdsp ./bdsp-pipeline \
    --create-namespace \
    --namespace dqops \
    --wait \
    --timeout 10m
```
```
