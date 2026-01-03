#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname Superset \
    --lastname Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" || true

superset init

exec /usr/bin/run-server.sh