from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, inspect, text
from .table_connection_client import DBConfig
import json

@dataclass
class PostgresConnectionClient:
    connectionName: Optional[str] = None
    host: str = ""
    port: str = ""
    username: str = ""
    password: str = ""
    database: str = ""
    jdbcProperties: Optional[Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        self.jdbcProperties = self.jdbcProperties or {}
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        self.inspector = inspect(self.engine)

    def to_dict(self) -> dict:
        """Convert object to dictionary"""
        data = asdict(self)
        return data

    @classmethod
    def from_dict(cls, data: dict | str):
        """Create PostgresConnectionClient from dictionary safely with key normalization"""
        try:
            valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
            result = {}
            
            # Parse string to dict if needed
            if isinstance(data, str):
                data = json.loads(data)
            
            # Normalize keys for both cases
            if isinstance(data, dict):
                for k, v in data.items():
                    if k == 'connection_name':
                        result['connectionName'] = v
                    elif k == 'jdbc_properties':
                        result['jdbcProperties'] = v
                    elif k in valid_fields:
                        result[k] = v
            
            return cls(**result)
        
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")
        except TypeError as e:
            raise ValueError(f"Invalid data format - expected dict or JSON string: {e}")
        except Exception as e:
            raise ValueError(f"Failed to create {cls.__name__} from data: {e}")

    def get_jdbc_url(self) -> str:
        """Get JDBC connection URL"""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    def get_jdbc_properties(self) -> Dict[str, str]:
        """Get JDBC connection properties"""
        props = {
            "user": self.username,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        if self.jdbcProperties:
            props.update(self.jdbcProperties)
        return props
    
    def has_same_config(self, config: DBConfig) -> bool:        
        return (
            self.host == config.host and
            self.port == config.port and
            self.database == config.database and
            self.username == config.username and
            self.password == config.password and
            self.jdbcProperties == config.jdbcProperties
        )

    def execute_query(self, query: str, params: dict = None):
        """
        Execute query and return results in predictable format:
        - SELECT queries -> always return list (of dicts or values)
        - Non-SELECT or no results -> None
        """
        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})

            if query.strip().lower().startswith("select"):
                rows = result.mappings().all()

                if not rows:
                    return None

                # Convert RowMapping to regular dict
                rows = [dict(row) for row in rows]

                # Single column queries -> list of values
                if len(rows[0]) == 1:
                    col_name = next(iter(rows[0].keys()))
                    return [r[col_name] for r in rows]

                # Multiple columns -> list of dicts
                return rows

            # Non-SELECT query
            return None
    
    def test_connection(self):
        with self.engine.connect() as conn:
            conn.execute("SELECT 1;")
        return True

    def get_columns(self, table_name: str, schema_name: str = "public"):
        inspector = self.inspector
        columns = inspector.get_columns(table_name, schema = schema_name)
        return [col['name'] for col in columns]
    
    def get_columns_with_types(self, table_name: str, schema = "public"):
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

        res = self.execute_query(query)
        return res

    def detect_primary_keys(self, schema: str, table_name: str, threshold: float = 0.99) -> list:
        query_cols = f"""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}';
        """
        
        df_cols = self.execute_query(query_cols)  # list[str]
        if not df_cols:
            return []

        result = []

        for col in df_cols:

            query_unique = f"""
                SELECT 
                    COUNT(DISTINCT {col})::float 
                    / NULLIF(COUNT({col}), 0) AS distinct_percent
                FROM "{schema}"."{table_name}";
            """
            distinct_percent = self.execute_query(query_unique)[0]  # sẽ trả scalar

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
        if not schemas_result:
            return {"schemas": [], "schema_count": 0}

        schemas = []
        for schema_name in schemas_result:
            tables = self.get_table_names('public')
            print(tables)
            print(f"Schema: {schema_name}, Tables: {tables}")  # Debug
            table_list = tables if tables else []
            schemas.append({
                "schema_name": schema_name,
                "tables": tables,
                "table_count": len(table_list)
            })

        return {"schemas": schemas, "schema_count": len(schemas)}