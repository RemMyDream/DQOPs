import pandas as pd
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote

class PostgresSQLClient:
    def __init__(self, 
                 database: str, 
                 user: str, 
                 password: str, 
                 jdbc_properties: dict = None, 
                 host="127.0.0.1", 
                 port = "5432"):
        self.database = database
        self.user = user
        self.password = quote(password)
        self.host = host
        self.port = port
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        self.jdbc_properties = jdbc_properties or {}
        self.inspector = inspect(self.engine)

    def get_jdbc_url(self):
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    def get_jdbc_properties(self):
        props = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        props.update(self.jdbc_properties)
        return props

    def test_connection(self):
        with self.engine.connect() as conn:
            conn.execute("SELECT 1;")
        return True

    def execute_query(self, query: str, params: dict = None):
        with self.engine.begin() as conn:
            if query.strip().lower().startswith("select"):
                result = conn.execute(text(query), params or {})
                return result
            else:
                conn.execute(text(query), params or {})

    def get_columns(self, table_name: str, schema = "public"):
        inspector = self.inspector
        columns = inspector.get_columns(table_name)
        return [col['name'] for col in columns]
    
    def get_columns_with_types(self, table_name: str, schema = "public"):
        query = f"""SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}';"""
        res = self.execute_query(query).fetchall()
        return {row[0]: row[1] for row in res}

    def get_table_name(self, schema="public"):
        inspector = self.inspector
        return inspector.get_table_names()

    def get_primary_keys(self, schema: str, table_name: str) -> list:
        query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = '{table_name}'
            AND tc.table_schema = '{schema}';
        """

        result = self.execute_query(query)
        rows = result.mappings().all()
        if not rows:
            return []
        return [row["column_name"] for row in rows]

    def detect_primary_keys(self, schema: str, table_name: str, threshold: float = 0.99) -> list:
        query_cols = f"""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}';
        """
        
        df_cols = self.execute_query(query_cols)

        column_names = [row['column_name'] for row in df_cols.mappings().all()]
        result = []

        for col in column_names:
            query_unique = f"""
                SELECT 
                    COUNT(DISTINCT "{col}")::float / NULLIF(COUNT("{col}"), 0) AS distinct_percent
                FROM "{schema}"."{table_name}";
            """
            df_unique = self.execute_query(query_unique).fetchone()
            distinct_percent = df_unique[0]

            if distinct_percent is not None and distinct_percent >= threshold:
                result.append(col)
        return result

    def get_schema_and_table(self):
        query = """
            SELECT schema_name 
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND schema_name NOT LIKE 'pg_%'
            ORDER BY schema_name;
        """
        schemas_result = self.execute_query(query)
        
        schemas = [
            {
                "schema_name": row['schema_name'],
                "tables": self.get_table_name(row['schema_name']),
                "table_count": len(self.get_table_name(row['schema_name']))
            }
            for row in schemas_result
        ]
        
        return {"schemas": schemas, "schema_count": len(schemas)}
    
