# Session Notes - Feb 11, 2026: DQ Profiling Feature Implementation

## Goal

Build **Part 1** of the DQOps app: **Visualize raw data quality** (no processing) for Bronze Iceberg tables across 4 dimensions: Completeness, Uniqueness, Validity, Volume.

## Design Decisions Made

1. **Data source**: Profile Bronze Iceberg tables (not source PostgreSQL)
2. **Execution**: Via Airflow DAG → Spark job on Kubernetes
3. **Storage**: Results stored in internal PostgreSQL (`profiling_results` table)
4. **Dimensions**: Core 4 — Completeness, Uniqueness, Validity, Volume

## Architecture Flow

```
Frontend → POST /profiling/trigger (ProfilingJobCreateRequest)
    → ProfilingService.trigger_profiling()
        → JobTriggerService.trigger(QUALITY, dag_conf)
            → Airflow REST API → dq_profiling DAG
                → PythonOperator: write config JSON (unique filename per run)
                → SparkKubernetesOperator: run dq_profiling.py
                    → Jinja2 Environment renders sensor SQL
                    → Spark executes SQL on Bronze Iceberg tables
                    → Results written to internal PostgreSQL via JDBC

Frontend → GET /profiling/table/{schema}/{table}/latest
    → ProfilingService.get_latest_profile()
        → ProfilingRepository reads from profiling_results table
        → Returns results grouped by dimension
```

## Files Created

### Backend (6 new files)

#### 1. `backend/domain/entity/profiling_result.py`
- `DQDimension` enum: COMPLETENESS, UNIQUENESS, VALIDITY, VOLUME
- `ProfilingResult` dataclass with to_dict()

#### 2. `backend/domain/request/profiling_job_create_request.py`
- `ProfilingTableInfo`: schema_name, table_name, columns (optional)
- `ProfilingJobCreateRequest`: connection_name, tables[], dimensions[]
- `to_dag_conf()` method converts to Airflow DAG config

#### 3. `backend/repositories/profiling_repository.py`
- Extends `PostgresConnectionClient`
- `init_table()`: creates `profiling_results` table + indices
- `get_results_by_run_id()`: SELECT * WHERE profile_run_id = :run_id
- `get_results_by_table()`: recent results for a table
- `get_latest_run_id()`: most recent run_id (single column → returns raw value)
- `get_run_history()`: aggregated run summaries (GROUP BY profile_run_id)

#### 4. `backend/services/profiling_service.py`
- `trigger_profiling(dag_conf)`: calls trigger_service.trigger(QUALITY, dag_conf)
- `get_latest_profile()`: gets latest run, groups results by `dimension` key
  - Note: `execute_query` returns `list[dict]` for SELECT * (many columns), so `r["dimension"]` works correctly
- `get_run_history()`: delegates to repository

#### 5. `backend/routers/profiling_router.py`
- `POST /profiling/trigger` — trigger profiling run
- `GET /profiling/results/{run_id}` — all metrics for a run
- `GET /profiling/table/{schema}/{table}/latest` — latest profile grouped by dimension
- `GET /profiling/table/{schema}/{table}/history` — run history summaries

### Airflow/Spark (3 new files)

#### 6. `airflow/dags/dq_profiling_dag.py`
- DAG id: `dq_profiling`, schedule: None (on-demand only)
- `prepare_config` PythonOperator: reads dag_run.conf, generates unique run_id, writes config JSON with unique filename
- `run_dq_profiling` SparkKubernetesOperator: runs dq_profiling.yaml

#### 7. `airflow/dags/scripts/dq_profiling.py`
- Spark job that profiles Iceberg tables
- **Sensor mapping**: COLUMN_SENSORS (completeness, uniqueness, validity) + TABLE_SENSORS (volume)
- **Key functions**:
  - `get_table_columns()`: reads Iceberg table schema via `spark.table()`
  - `build_column_context()`: builds Jinja2 context with `schema_name = 'catalog.database'`
  - `select_validity_sensors()`: picks numeric (min/max/mean) or text (length stats) based on column type
  - `run_sensor()`: renders template + executes Spark SQL
  - `profile_table()`: loops columns × dimensions, collects results
  - `write_results_to_postgres()`: writes via Spark JDBC
- **Column type detection**: NUMERIC_TYPES and TEXT_TYPES sets for validity sensor selection

#### 8. `airflow/dags/spark-apps/dq_profiling.yaml`
- SparkApplication manifest, mirrors bronze_ingestion.yaml
- Same Iceberg + MinIO spark config
- Mounts airflow-dags PVC at /opt/airflow/dags

#### 9. `airflow/dags/scripts/config/dq_profiling.json`
- Example config with internal_postgres connection details

## Files Modified

### 1. `backend/services/job_trigger_service.py`
Added to `DAG_MAPPING`:
```python
JobType.QUALITY: {
    "default": "dq_profiling"
}
```

### 2. `backend/routers/dependencies.py`
- Added imports: `ProfilingRepository`, `ProfilingService`
- Added `'profiling': ProfilingRepository` to `repo_map`
- Added `get_profiling_service()` to `ServiceContainer` class
- Added module-level `get_profiling_service()` getter

### 3. `backend/main.py`
- Added `from routers.profiling_router import router as profiling_router`
- Added `app.include_router(profiling_router)`

## Key Technical Details

### How `execute_query()` works (important!)
From `postgres_client.py`:
- 1 column → `list[value]` (raw values, NOT dicts)
- Many columns → `list[dict]` (column names as keys)
- No rows → `[]`

This means:
- `get_latest_run_id()` returns a raw string (single column query)
- `get_results_by_run_id()` returns `list[dict]` with all column names as keys including `dimension`

### Iceberg table naming in template context
- `render_target_table()` macro produces `` `schema_name`.`table_name` ``
- For Iceberg: set `target_table.schema_name = 'bronze.data_source'`
- Result: `` `bronze.data_source`.`transactions` `` — valid Spark SQL

### Sensor template structure
All sensors follow the pattern:
```sql
{% import 'dialects/spark.sql.jinja2' as lib with context -%}
SELECT
    <metric_expression> AS actual_value
FROM {{ lib.render_target_table() }} AS analyzed_table
{{- lib.render_where_clause() -}}
```

## Remaining Work / Next Steps

1. **Copy sensor templates** to `airflow/dags/scripts/templates/sensors/` so the Spark job can access them
2. **Verify PostgreSQL JDBC driver** is in the Spark Docker image
3. **Part 2: DQ Check & Monitoring** — use rule_engine + check_executor with threshold-based rules, alerting
4. **Frontend integration** — React components to visualize profiling results from `/profiling/table/.../latest`

## Example API Usage

### Trigger profiling
```json
POST /profiling/trigger
{
  "connection_name": "my_postgres",
  "tables": [
    {
      "schema_name": "data_source",
      "table_name": "transactions",
      "columns": null
    }
  ],
  "dimensions": ["completeness", "uniqueness", "validity", "volume"]
}
```

### Response from latest profile
```json
GET /profiling/table/data_source/transactions/latest
{
  "profile_run_id": "run_20260211_143022_a1b2c3d4",
  "schema_name": "data_source",
  "table_name": "transactions",
  "dimensions": {
    "completeness": [
      {"column_name": "Transaction_ID", "metric_name": "null_record_percent", "actual_value": 0.0, ...},
      {"column_name": "Amount", "metric_name": "null_record_percent", "actual_value": 2.3, ...}
    ],
    "uniqueness": [...],
    "validity": [...],
    "volume": [
      {"column_name": null, "metric_name": "row_count", "actual_value": 150000, ...}
    ]
  },
  "total_metrics": 42
}
```
