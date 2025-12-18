from .postgres_client import PostgresConnectionClient
from typing import Dict

class SourceClient(PostgresConnectionClient):
    def has_same_config(self, config: Dict) -> bool:        
        return (
            self.host == config.get('host') and
            self.port == config.get('port') and
            self.database == config.get('database') and
            self.username == config.get('username') and
            self.password == config.get('password') and
            self.jdbc_properties == config.get('jdbc_properties')
        )

    def get_columns(self, table_name: str, schema_name: str = "public"):
        columns = self.inspector.get_columns(table_name, schema=schema_name)
        return [col['name'] for col in columns]
    
    def get_columns_with_types(self, table_name: str, schema: str = "public"):
        query = """
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_schema = :schema
            AND table_name = :table_name;
        """
        res = self.execute_query(query, {"schema": schema, "table_name": table_name})
        return {row['column_name']: row['data_type'] for row in res}

    def get_table_names(self, schema: str = "public"):
        """Get list of table names in schema"""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
        """
        return self.execute_query(query, params={"schema": schema})
        
    def get_primary_keys(self, schema: str, table_name: str) -> list:
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = :table_name
            AND tc.table_schema = :schema;
        """
        return self.execute_query(query, {"schema": schema, "table_name": table_name})

    def detect_primary_keys(self, schema: str, table_name: str, threshold: float = 0.99) -> list:
        query_cols = """
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = :schema
            AND table_name = :table_name;
        """
        
        df_cols = self.execute_query(query_cols, {"schema": schema, "table_name": table_name})
        if not df_cols:
            return []

        result = []
        for col in df_cols:
            query_unique = f"""
                SELECT 
                    COUNT(DISTINCT "{col}")::float 
                    / NULLIF(COUNT("{col}"), 0) AS distinct_percent
                FROM "{schema}"."{table_name}";
            """
            distinct_percent = self.execute_query(query_unique)[0]

            if distinct_percent is not None and distinct_percent >= threshold:
                result.append(col)

        return result

    def get_schema_and_table(self):
        query = """
            SELECT schema_name 
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND schema_name NOT LIKE 'pg_%%'
            ORDER BY schema_name;
        """
        schemas_result = self.execute_query(query)
        if not schemas_result:
            return {"schemas": [], "schema_count": 0}

        schemas = []
        for schema_name in schemas_result:
            tables = self.get_table_names(schema_name)
            table_list = tables if tables else []
            schemas.append({
                "schema_name": schema_name,
                "tables": table_list,
                "table_count": len(table_list)
            })

        return {"schemas": schemas, "schema_count": len(schemas)}