# DQOPs - Codebase Context

## Project Overview

Real-Time Lakehouse Data Pipeline with Data Quality Operations (DQOps) and anomaly/fraud detection. Built around a **medallion architecture** (Bronze → Silver → Gold) using Apache Iceberg on MinIO.

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |V
| Compute | Apache Spark 3.5.1 |
| Storage | MinIO (S3-compatible) + Apache Iceberg |
| Streaming | Kafka (4-node) + Debezium CDC |
| Data Quality | Custom DQOps engine (Jinja2 sensors + Python rules) |
| ML | MLflow, Feast (feature store) |
| Backend API | FastAPI (Python 3.10) |
| Frontend | React 18.2, Tailwind CSS, Recharts |
| BI | Apache Superset, Spark Thrift Server |
| Infra | Docker, Kubernetes (KinD), Helm |
| Monitoring | Prometheus + Grafana |

## Code Organization

```
DQOPs/
├── airflow/              # DAGs & ETL scripts
│   ├── dags/
│   │   ├── etl.py                          # Main ETL DAG
│   │   ├── dq_profiling_dag.py             # DQ Profiling DAG (NEW)
│   │   ├── scripts/
│   │   │   ├── bronze_ingestion.py         # PostgreSQL → Bronze Iceberg
│   │   │   ├── gold_transformation.py      # Silver → Gold with ML features
│   │   │   ├── dq_profiling.py             # DQ Profiling Spark job (NEW)
│   │   │   └── config/
│   │   │       ├── bronze_ingestion.json
│   │   │       ├── gold_transformation.json
│   │   │       └── dq_profiling.json       # (NEW)
│   │   └── spark-apps/
│   │       ├── bronze_ingestion.yaml
│   │       ├── gold_transformation.yaml
│   │       └── dq_profiling.yaml           # (NEW)
│   ├── Dockerfile
│   └── docker-compose.yaml
│
├── backend/              # FastAPI Backend (DDD architecture)
│   ├── main.py           # App entry point, router registration
│   ├── domain/
│   │   ├── entity/
│   │   │   ├── postgres_client.py          # Base DB client (SQLAlchemy)
│   │   │   ├── source_client.py            # Extended with schema inspection
│   │   │   ├── airflow_client.py           # Airflow REST API client
│   │   │   ├── job_client.py               # Job base class + JobType enum
│   │   │   ├── job_schemas.py              # JobVersion, JobHistory, etc.
│   │   │   ├── ingest_job_client.py        # IngestJob entity
│   │   │   ├── rule.py                     # Rule execution models
│   │   │   ├── dq_model.py                 # (empty placeholder)
│   │   │   └── profiling_result.py         # DQDimension + ProfilingResult (NEW)
│   │   └── request/
│   │       ├── table_connection_request.py # DBConfig, DBCredential
│   │       ├── job_request.py              # JobCreateRequest, etc.
│   │       ├── ingest_job_create_request.py
│   │       └── profiling_job_create_request.py  # (NEW)
│   ├── repositories/
│   │   ├── postgres_connection_repository.py
│   │   ├── job_repository.py
│   │   ├── job_version_repository.py
│   │   └── profiling_repository.py         # (NEW)
│   ├── services/
│   │   ├── postgres_connection_service.py  # Connection lifecycle
│   │   ├── job_service.py                  # Job CRUD + versioning
│   │   ├── job_trigger_service.py          # Airflow DAG triggering
│   │   ├── template_engine.py             # Jinja2 SQL renderer
│   │   ├── rule_engine.py                 # Python rule loader/executor
│   │   ├── check_executor.py              # DQ check pipeline orchestrator
│   │   ├── dq_service.py                  # High-level DQ API
│   │   └── profiling_service.py           # (NEW)
│   ├── routers/
│   │   ├── postgres_connection_router.py  # /postgres/* endpoints
│   │   ├── job_trigger_router.py          # /trigger/* endpoints
│   │   ├── job_router.py
│   │   ├── profiling_router.py            # /profiling/* endpoints (NEW)
│   │   └── dependencies.py               # ServiceContainer DI
│   ├── factories/
│   │   └── job_factory.py
│   ├── templates/
│   │   ├── sensors/                       # Jinja2 SQL sensor templates
│   │   │   ├── dialects/spark.sql.jinja2  # Macro library (quoting, filters, grouping)
│   │   │   ├── column/null/               # Completeness sensors
│   │   │   ├── column/uniqueness/         # Uniqueness sensors
│   │   │   ├── column/numeric/            # Validity sensors (numeric)
│   │   │   ├── column/text/               # Validity sensors (text)
│   │   │   ├── table/volume/              # Volume sensors
│   │   │   └── ...                        # 40+ sensor types
│   │   └── rules/                         # Python rule files
│   │       ├── comparison/                # between_percent, between_floats, etc.
│   │       ├── average/                   # moving average rules
│   │       └── change/                    # change detection rules
│   └── image/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── docker-compose.yaml
│
├── FE/                   # React Frontend
├── spark/                # Spark cluster setup
├── kafka/                # Kafka + Debezium CDC
├── minio/                # Object storage
├── mlflow/               # ML lifecycle
├── superset/             # BI dashboards
├── chart/                # Helm chart (K8s)
├── spark-operator/       # Spark K8s operator
└── clickhouse/           # OLAP (optional)
```

## Backend Architecture Patterns

- **Domain-Driven Design**: entity → request → repository → service → router
- **ServiceContainer DI**: Singleton in `routers/dependencies.py`, all repos/services lazy-initialized
- **Repository Pattern**: All repos extend `PostgresConnectionClient`, use `execute_query()` which returns `list[value]` (1 col) or `list[dict]` (many cols)
- **Job Trigger Flow**: Request → `to_dag_conf()` → `trigger_service.trigger(job_type, dag_conf)` → Airflow REST API
- **Soft Delete**: Jobs and connections marked as `'deleted'`, not removed
- **Job Versioning**: `job_versions` table with `is_active` flag, auto-deactivates old versions

## Template Engine Context Structure

```python
{
    'target_table': {'schema_name': 'catalog.database', 'table_name': 'table'},
    'table': {
        'filter': None,
        'columns': {
            'col_name': {'type_snapshot': {'column_type': 'VARCHAR'}, 'sql_expression': None}
        }
    },
    'column_name': 'col_name',
    'error_sampling': {'samples_limit': 10, 'total_samples_limit': 1000, 'id_columns': []},
    'parameters': {},
    'additional_filters': []
}
```

For Bronze Iceberg tables: `target_table.schema_name = 'bronze.data_source'` so `render_target_table()` produces `` `bronze.data_source`.`table_name` ``.

## Database Schema (Internal PostgreSQL)

### postgres_connections
- connection_id, connection_name, host, port, database, username, password, jdbc_properties, status, timestamps

### jobs + job_versions
- Jobs with type/status, versioned configs in JSONB, FK cascade

### profiling_results (NEW)
- result_id, profile_run_id, schema_name, table_name, column_name (nullable for table-level), dimension, metric_name, actual_value, executed_sql, execution_time_ms, error_message, column_type, created_at
- Indices on: profile_run_id, (schema_name, table_name), dimension, created_at DESC

## API Endpoints

### Connections: `/postgres/*`
- POST /connections, GET /connections, GET/DELETE /connections/{name}
- POST /schemas/{name}, POST /tables/preview, POST /tables/columns, POST /tables/primary-keys

### Job Trigger: `/trigger/*`
- POST /trigger/ingest, GET /trigger/status/{dag_id}/{run_id}, GET /trigger/runs/{dag_id}

### Profiling: `/profiling/*` (NEW)
- POST /profiling/trigger
- GET /profiling/results/{run_id}
- GET /profiling/table/{schema}/{table}/latest
- GET /profiling/table/{schema}/{table}/history

## DAG Mapping (job_trigger_service.py)

```python
INGEST   → bronze: ingest_to_bronze, silver: ingest_to_silver
TRANSFORM → silver: transform_to_silver, gold: transform_to_gold
EXPORT   → postgres: export_to_postgres, s3: export_to_s3
QUALITY  → default: dq_profiling  # NEW
```

## DQ Profiling Sensor Mapping

| Dimension | Sensors (template paths) |
|---|---|
| Completeness | null_record_count, null_record_percent, not_null_record_count, not_null_record_percent |
| Uniqueness | distinct_record_count, distinct_record_percent, duplicate_record_count, duplicate_record_percent |
| Validity (numeric) | min, max, mean |
| Validity (text) | text_min_length, text_max_length, text_mean_length |
| Volume | row_count, column_count |

## Environment Variables

```
AIRFLOW_USERNAME, AIRFLOW_PASSWORD, AIRFLOW_URL
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE
```

## Important Notes

- Sensor templates at `backend/templates/sensors/` must be copied/symlinked to `airflow/dags/scripts/templates/sensors/` for the Spark profiling job to access them via the `airflow-dags` PVC.
- The Spark Docker image (`remmydream/spark-image:v3.0`) must include the PostgreSQL JDBC driver for writing profiling results.
- The `dq_profiling_dag.py` generates unique config filenames per run to avoid concurrency issues.

## Current Status (Feb 2026)

- **Part 1 (Profiling)**: Implementation complete - trigger via Airflow, results stored in DB, API endpoints ready
- **Part 2 (Check & Monitoring)**: Not yet implemented - will use rule_engine + check_executor for threshold-based checks

---

## Senior Data Engineer Review Notes (2026-02-23)

### Overall Assessment

**Architecture: Strong foundations.** Medallion architecture with Iceberg, Kafka CDC, Spark, and a custom DQ engine is well-designed. The Jinja2 sensor template system (52 sensors, 540-line macro library) is sophisticated and extensible. DDD backend pattern is clean.

**Production Readiness: NOT production-ready.** Needs hardening in security, error handling, observability, and networking before deployment beyond dev/test.

---

### CRITICAL BUGS (Must Fix)

| # | File | Issue |
|---|------|-------|
| 1 | `services/job_service.py` | `pause_job()` references `JobStatus.PAUSED` which does NOT exist in the enum. Will crash at runtime. |
| 2 | `services/template_engine.py` (lines ~175-219) | Test/debug code left in production module. Remove or move to test file. |
| 3 | `services/rule_engine.py` (line ~102) | Tries to load non-existent file path. Will error on execution. |
| 4 | `routers/job_router.py` | Completely empty - defined but never registered in `main.py`. Dead code. |
| 5 | `dags/etl.py` | Bronze ingestion task is commented out. Gold transformation runs without upstream dependency. DAG is broken for full pipeline. |

---

### SECURITY ISSUES (High Priority)

| # | Issue | Location | Fix |
|---|-------|----------|-----|
| 1 | **Passwords stored plaintext** in DB | `postgres_connection_repository.py` | Encrypt at rest (Fernet/KMS) |
| 2 | **No authentication** on any API endpoint | All routers | Add JWT/OAuth2 middleware |
| 3 | **Hard-coded credentials** in docker-compose files | `airflow/`, `minio/`, `superset/` | Move to `.env` files (gitignored) |
| 4 | **Dynamic code execution** via importlib | `rule_engine.py` | Add sandboxing or allowlist |
| 5 | **Jinja2 sandbox disabled** | `template_engine.py` | Use `SandboxedEnvironment` |
| 6 | **CORS hardcoded** to localhost | `main.py` | Use env var for allowed origins |

---

### ARCHITECTURE ISSUES

#### Backend

| Area | Problem | Recommendation |
|------|---------|----------------|
| **ServiceContainer** | Not thread-safe. Class-level variables have race conditions. `init_table()` runs on first request (blocks). | Add threading locks. Init tables at startup, not on first request. |
| **Repository layer** | Non-atomic operations (deactivate + insert version). No transaction handling. | Wrap multi-step operations in explicit transactions. |
| **Connection cache** | No size limit, no TTL, re-validates on every `get_postgres_client()` call (expensive). | Add TTL-based cache with max size. Validate lazily. |
| **Job factory** | Only INGEST implemented. TRANSFORM/EXPORT/QUALITY raise ValueError. | Implement or stub remaining job types. |
| **Error handling** | Inconsistent: some methods return bool, some raise, some silently catch. | Standardize: raise exceptions in services, catch in routers with proper HTTP status. |
| **Profiling router** | Returns 404 on empty result sets. Should return 200 with empty array. | Fix response handling for empty results. |

#### Airflow / Spark

| Area | Problem | Recommendation |
|------|---------|----------------|
| **DAG orchestration** | Bronze ingestion commented out. DQ profiling is separate DAG with no upstream dependency. | Single DAG chain: `bronze >> gold >> profiling` or use DAG dependencies (TriggerDagRunOperator). |
| **Retries** | `retries: 0` everywhere. Any transient failure kills the job. | Set `retries: 2, retry_delay: timedelta(minutes=5)`. |
| **bronze_ingestion.py** | Partition bounds query scans entire table for MIN/MAX. Expensive on large tables. | Use metadata or LIMIT 1 ORDER BY approach. |
| **gold_transformation.py** | `approxQuantile()` called per column = multiple full table scans. | Batch compute all quantiles in single pass. |
| **gold_transformation.py** | Feature transformation parameters (clip bounds, IQR bounds) not persisted. ML inference can't reproduce. | Save transformation metadata to config table. |
| **dq_profiling.py** | Hard-coded template path (`/opt/airflow/dags/scripts/templates/sensors`). Config `internal_postgres` is loaded but ignored (uses env vars instead). | Accept template_dir as config param. Use config values before falling back to env vars. |
| **K8s manifests** | No resource requests/limits, no restart policy, no liveness probes. Image version mismatch (v3.0 vs v3.1). | Add resource specs, align image versions, add probes. |

#### Infrastructure / Networking

| Area | Problem | Recommendation |
|------|---------|----------------|
| **Docker networks** | Different compose files use different networks: `bigdata-net`, `b`, `data-pipeline`. FastAPI cannot reach Airflow. | Unify to single external network or document explicit setup. |
| **Airflow executor** | LocalExecutor - single-node, not scalable. | Switch to KubernetesExecutor for K8s deployments. |
| **Multiple Postgres** | Airflow, FastAPI, MLflow each have own Postgres instance. | Consider shared instance with separate databases, or document why isolated. |

---

### TEMPLATE ENGINE INVENTORY

**52 Jinja2 SQL sensor templates** organized by dimension:

| Scope | Dimension | Count | Templates |
|-------|-----------|-------|-----------|
| Column | Completeness (null) | 4 | null_record_count/percent, not_null_record_count/percent |
| Column | Uniqueness | 5 | distinct_record_count/percent, duplicate_record_count/percent, duplicate_value_record_count |
| Column | Validity (numeric) | 15 | min, max, mean, sum, percentile, negative_count/percent, non_negative_count/percent, stddev (pop/sample), variance (pop/sample), value_in_range_percent |
| Column | Validity (text) | 5 | text_min/max/mean_length, min/max_word_count |
| Column | Validity (datetime) | 1 | date_in_range_percent |
| Column | Validity (bool) | 2 | true_percent, false_percent |
| Column | Accuracy | 5 | average/min/max/sum/not_null_match |
| Column | Accepted values | 2 | text/number_found_in_set_percent |
| Column | Detection | 2 | sample_value, string_datatype_detect |
| Table | Volume | 2 | row_count, column_count |
| Table | Schema | 3 | column_count, column_list_ordered_hash, column_types_hash |
| Table | Timeliness | 2 | data_freshness, data_ingestion_delay |
| Table | Custom SQL | 6 | import_custom_sql, sql_aggregated_expression, sql_condition_failed/passed_count/percent |

**9 Python rule templates** in 3 categories:
- **Comparison** (6): between_percent, between_floats, between_ints, max_diff_percent_to_expect_value, max_diff_to_expect_value, detected_datatype_equal
- **Average** (1): between_percent_moving_average_n_days
- **Change** (2): between_change, between_percent_change

**Macro library** (`spark.sql.jinja2`, 540 lines): render_target_table, render_target_column, render_where_clause, render_data_grouping_projections, render_time_dimension_projection, render_error_sampler, date_trunc, render_date_range_filters, render_group_by, render_order_by

---

### RECOMMENDED ROADMAP

#### Phase 1: Stabilize (1-2 weeks)
- [ ] Fix critical bugs (pause_job, dead code, broken DAG chain)
- [ ] Unify Docker networks
- [ ] Add `.env` files for credentials (remove from compose)
- [ ] Add retries to DAGs
- [ ] Fix profiling config to use JSON values before env var fallback

#### Phase 2: Harden (2-4 weeks)
- [ ] Add JWT authentication to API endpoints
- [ ] Encrypt passwords at rest in postgres_connections table
- [ ] Add thread safety to ServiceContainer
- [ ] Wrap repository multi-step operations in transactions
- [ ] Add resource requests/limits to K8s manifests
- [ ] Standardize error handling (exceptions in services, HTTP codes in routers)

#### Phase 3: Part 2 - Check & Monitoring (4-6 weeks)
- [ ] Implement check execution pipeline: sensor SQL → actual_value → rule evaluation → pass/fail
- [ ] Add check definition storage (which sensors + rules to run per table/column)
- [ ] Add check scheduling (cron-based recurring checks)
- [ ] Add alerting on check failures (Slack/email integration)
- [ ] Build DQ dashboard (React frontend with Recharts for trend visualization)

#### Phase 4: Production Readiness (ongoing)
- [ ] Switch Airflow to KubernetesExecutor
- [ ] Add Prometheus metrics + Grafana dashboards for DQ trends
- [ ] Add structured logging (structlog) across all services
- [ ] Add pytest coverage for sensors (SQL validation), rules (logic), routers (integration)
- [ ] Save feature transformation metadata for ML inference reproducibility
- [ ] Add data lineage tracking (column-level impact analysis)
- [ ] Batch quantile computation in gold_transformation.py (single Spark action instead of N scans)
