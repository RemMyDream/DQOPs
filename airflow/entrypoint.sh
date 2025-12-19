#!/bin/bash
set -e

export PATH="/home/airflow/.local/bin:$PATH"

# Initialize database
airflow db init

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

exec "$@"