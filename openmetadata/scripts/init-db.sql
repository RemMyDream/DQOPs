DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata_user') THEN
      CREATE ROLE openmetadata_user LOGIN PASSWORD 'openmetadata_password';
   END IF;
END
$$;
CREATE DATABASE openmetadata_db OWNER openmetadata_user;

CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db OWNER airflow_user;
