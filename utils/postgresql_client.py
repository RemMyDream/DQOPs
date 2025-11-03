import pandas as pd
from sqlalchemy import create_engine, inspect, text

class PostgresSQLClient:
    def __init__(self, database, user, password, host="127.0.0.1", port="5432"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        self.inspector = inspect(self.engine)

    def execute_query(self, query: str):
        with self.engine.begin() as conn:
            if query.strip().lower().startswith("select"):
                result = conn.execute(text(query))
                return result  
            else:
                conn.execute(text(query))

    def get_columns(self, table_name, schema = "public"):
        inspector = self.inspector
        columns = inspector.get_columns(table_name)
        return [col['name'] for col in columns]
    
    def get_columns_with_types(self, table_name, schema = "public"):
        query = f"""SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}';"""
        res = self.execute_query(query).fetchall()
        return {row[0]: row[1] for row in res}

    def get_table_name(self, schema="public"):
        inspector = self.inspector
        return inspector.get_table_names()
    