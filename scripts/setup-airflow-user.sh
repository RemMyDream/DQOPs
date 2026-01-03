#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
sleep 5

echo "Setting up Airflow user and permissions..."
PGPASSWORD=openmetadata_password psql -h postgres-metadata -U openmetadata_user -d postgres <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'airflow_user') THEN
            CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
            RAISE NOTICE 'User airflow_user created';
        ELSE
            RAISE NOTICE 'User airflow_user already exists';
        END IF;
    END
    \$\$;
EOSQL

echo "Granting privileges on database..."
PGPASSWORD=openmetadata_password psql -h postgres-metadata -U openmetadata_user -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;"

echo "Granting privileges on schema..."
PGPASSWORD=openmetadata_password psql -h postgres-metadata -U openmetadata_user -d airflow_db -c "GRANT ALL ON SCHEMA public TO airflow_user;"

echo "Airflow user setup completed successfully!"
