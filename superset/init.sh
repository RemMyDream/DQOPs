#!/bin/bash
set -e

# 1. Upgrade metastore (phải chạy trước)
superset db upgrade

# 2. Tạo admin user
superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname Superset \
    --lastname Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD"

# 3. Setup roles & permissions
superset init

# 4. Start server
exec /usr/bin/run-server.sh