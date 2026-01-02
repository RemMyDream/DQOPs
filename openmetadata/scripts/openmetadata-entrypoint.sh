#!/bin/bash
set -e

LOG_PREFIX="[OM-Entrypoint]"

log_info() {
    echo "$LOG_PREFIX [INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo "$LOG_PREFIX [ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2
}

log_warn() {
    echo "$LOG_PREFIX [WARN] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

DB_HOST="${DB_HOST:-postgres-metadata}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-openmetadata_user}"
DB_PASSWORD="${DB_USER_PASSWORD:-openmetadata_password}"
OM_DATABASE="${OM_DATABASE:-openmetadata_db}"

AIRFLOW_DB="${AIRFLOW_DB:-airflow_db}"
AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-airflow_user}"
AIRFLOW_DB_PASSWORD="${AIRFLOW_DB_PASSWORD:-airflow_pass}"

ES_HOST="${ELASTICSEARCH_HOST:-elasticsearch}"
ES_PORT="${ELASTICSEARCH_PORT:-9200}"
ES_SCHEME="${ELASTICSEARCH_SCHEME:-http}"

MAX_RETRIES="${MAX_RETRIES:-30}"
RETRY_INTERVAL="${RETRY_INTERVAL:-5}"

SKIP_DB_INIT="${SKIP_DB_INIT:-false}"
SKIP_AIRFLOW_INIT="${SKIP_AIRFLOW_INIT:-false}"
SKIP_MIGRATION="${SKIP_MIGRATION:-false}"
SKIP_ES_WAIT="${SKIP_ES_WAIT:-false}"

wait_for_postgres() {
    log_info "Waiting for PostgreSQL at $DB_HOST:$DB_PORT..."
    
    local retries=0
    until nc -z "$DB_HOST" "$DB_PORT" 2>/dev/null || (echo > /dev/tcp/"$DB_HOST"/"$DB_PORT") 2>/dev/null; do
        retries=$((retries + 1))
        if [ $retries -ge $MAX_RETRIES ]; then
            log_error "PostgreSQL not available after $MAX_RETRIES attempts. Exiting."
            exit 1
        fi
        log_info "PostgreSQL not ready. Attempt $retries/$MAX_RETRIES. Retrying in ${RETRY_INTERVAL}s..."
        sleep $RETRY_INTERVAL
    done
    
    log_info "PostgreSQL is ready!"
}

wait_for_elasticsearch() {
    if [ "$SKIP_ES_WAIT" = "true" ]; then
        log_warn "Skipping Elasticsearch wait (SKIP_ES_WAIT=true)"
        return 0
    fi
    
    log_info "Waiting for Elasticsearch at $ES_SCHEME://$ES_HOST:$ES_PORT..."
    
    local retries=0
    until wget -q -O- "$ES_SCHEME://$ES_HOST:$ES_PORT/_cluster/health" 2>/dev/null | grep -qE '"status":"(green|yellow)"'; do
        retries=$((retries + 1))
        if [ $retries -ge $MAX_RETRIES ]; then
            log_error "Elasticsearch not available after $MAX_RETRIES attempts. Exiting."
            exit 1
        fi
        log_info "Elasticsearch not ready. Attempt $retries/$MAX_RETRIES. Retrying in ${RETRY_INTERVAL}s..."
        sleep $RETRY_INTERVAL
    done
    
    log_info "Elasticsearch is ready!"
}

init_openmetadata_db() {
    log_info "Skipping DB init (handled by PostgreSQL container)"
}

init_airflow_db() {
    log_info "Skipping Airflow DB init (handled by PostgreSQL container)"
}

run_migration() {
    if [ "$SKIP_MIGRATION" = "true" ]; then
        log_warn "Skipping migration (SKIP_MIGRATION=true)"
        return 0
    fi
    
    log_info "Running OpenMetadata database migration..."

    if [ -f "/opt/openmetadata/bootstrap/openmetadata-ops.sh" ]; then
        /opt/openmetadata/bootstrap/openmetadata-ops.sh migrate
        log_info "Migration completed successfully."
    else
        log_error "Migration script not found at ./bootstrap/openmetadata-ops.sh"
        exit 1
    fi
}

check_migration_status() {
    log_info "Migration completed, skipping verification (no psql client)"
}

main() {
    log_info "Starting OpenMetadata Unified Entrypoint"

    wait_for_postgres
    wait_for_elasticsearch

    init_openmetadata_db
    init_airflow_db

    run_migration
    check_migration_status

    log_info "Initialization complete. Starting server..."

    if [ $# -gt 0 ]; then
        exec "$@"
    else
        exec /opt/openmetadata/bin/openmetadata-server-start.sh \
            /opt/openmetadata/conf/openmetadata.yaml
    fi
}

main "$@"
