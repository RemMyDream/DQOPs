from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, inspect, text
import json

@dataclass
class PostgresConnectionClient:
    connection_name: Optional[str] = None
    host: str = ""
    port: str = ""
    username: str = ""
    password: str = ""
    database: str = ""
    jdbc_properties: Optional[Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        self.jdbc_properties = self.jdbc_properties or {}
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        self.inspector = inspect(self.engine)

    def to_dict(self) -> dict:
        """Convert object to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict | str):
        """Create PostgresConnectionClient from dictionary"""
        try:
            if isinstance(data, str):
                data = json.loads(data)
            
            valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
            filtered_data = {k: v for k, v in data.items() if k in valid_fields}
            
            return cls(**filtered_data)
        
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
        if self.jdbc_properties:
            props.update(self.jdbc_properties)
        return props

    def execute_query(self, query: str, params: dict = None) -> list:
        """
        Execute query and ALWAYS return list:
        - 1 column  -> list[value]
        - many cols -> list[dict]
        - no rows   -> []
        """
        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})

            if not result.returns_rows:
                return []

            rows = result.mappings().all()

            if not rows:
                return []

            rows = [dict(row) for row in rows]

            # 1 column → list[value]
            if len(rows[0]) == 1:
                col_name = next(iter(rows[0].keys()))
                return [r[col_name] for r in rows]

            # many columns → list[dict]
            return rows

    
    def test_connection(self):
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        return True