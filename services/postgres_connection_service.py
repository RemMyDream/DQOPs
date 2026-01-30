from typing import Optional, List, Dict
from domain.entity.source_client import SourceClient
from domain.entity.airflow_client import Airflow
from repositories.postgres_connection_repository import PostgresConnectionRepository
from domain.request.table_connection_request import DBConfig, DBCredential
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresConnectionService:
    def __init__(self, 
                 repo: PostgresConnectionRepository, 
                 airflow: Airflow
                 ):
        self.repo = repo
        self.airflow = airflow
        self._connection_cache: Dict[str, SourceClient] = {}
    
    def upsert_connection(self, config: DBConfig) -> Dict:
        """Create new connection or update existing one."""
        logger.info(f"Received config: {config}")
        
        connection_name = config.connection_name
        existing_conn = self.repo.get_active_connection(connection_name)
        
        # Test connection first
        pc = SourceClient.from_dict(config.model_dump())
        try:
            pc.test_connection()
        except Exception as e:
            raise ValueError(f"Connection test failed: {e}")
        
        # Sync to Airflow BEFORE saving to database
        airflow_synced = self._sync_to_airflow(config)
        if not airflow_synced:
            raise ValueError(
                "Failed to sync connection to Airflow. "
                "Please check Airflow connectivity and try again."
            )
        
        if existing_conn:
            if not existing_conn.has_same_config(config.model_dump()):
                logger.info("Config changed, creating new connection")
                
                self.repo.soft_delete_connection(connection_name)
                self.clear_connection_cache(connection_name)
                self.repo.insert_connection(pc)
                self._connection_cache[connection_name] = pc

                return {
                    "status": "Success",
                    "message": "Connection updated successfully",
                    "action": "Updated"
                }
            else:                    
                return {
                    "status": "Success",
                    "message": "Using existing connection",
                    "action": "Existed"
                }
        
        self.repo.insert_connection(pc)
        self._connection_cache[connection_name] = pc

        return {
            "status": "Success",
            "message": "Connection established successfully",
            "action": "Created"
        }        

    def get_postgres_client(self, connection_name: str) -> Optional[SourceClient]:
        """Get or create a PostgreSQLClient instance for the given connection name."""
        if connection_name in self._connection_cache:
            cached_client = self._connection_cache[connection_name]
            try:
                cached_client.test_connection()
                return cached_client
            except:
                del self._connection_cache[connection_name]
            
        try:
            pc = self.repo.get_active_connection(connection_name)
            pc.test_connection()
            self._connection_cache[connection_name] = pc
            return pc
        except Exception:
            return None

    def get_all_active_connections(self) -> List[SourceClient]:
        """Get all active connections, skipping ones that cannot connect"""
        res = self.repo.get_all_active_connections()
        connections = []

        for connection in res:
            try:
                connection.test_connection()
                connections.append(connection)
            except Exception as e:
                logger.warning(
                    f"Skipping connection '{connection.connection_name}' "
                    f"because test_connection() failed: {e}"
                )
        return connections

    def delete_connection(self, connection_name: str) -> bool:
        """Soft delete a connection (mark as deleted)"""
        existing_conn = self.get_postgres_client(connection_name)
        if not existing_conn:
            raise ValueError(f"No active connection found with name: {connection_name}")
        
        self.clear_connection_cache(connection_name)
        
        # Delete from Airflow first
        airflow_deleted = self._delete_from_airflow(connection_name)
        if not airflow_deleted:
            logger.warning(f"Failed to delete connection from Airflow: {connection_name}")
        
        existing_conn.engine.dispose()
        return self.repo.soft_delete_connection(connection_name)

    def clear_connection_cache(self, connection_name: Optional[str] = None):
        """Clear connection cache. If connection_name provided, clear only that connection."""
        if connection_name:
            if connection_name in self._connection_cache:
                try:
                    self._connection_cache[connection_name].engine.dispose()
                except:
                    pass
                del self._connection_cache[connection_name]
        else:
            for client in self._connection_cache.values():
                try:
                    client.engine.dispose()
                except:
                    pass
            self._connection_cache.clear()

    def get_schemas_and_tables(self, connection_name: str) -> Dict:
        """Get all schemas and tables for a connection."""
        logger.info(f"Fetching schemas for connection: {connection_name}")

        pc = self.get_postgres_client(connection_name)
        if not pc:
            raise ValueError(f"No active connection found with name: {connection_name}")
    
        return pc.get_schema_and_table()

    def preview_table(self, credential: DBCredential):
        """Preview table data (first 15 rows)."""
        if not credential.schema_name or not credential.table_name:
            raise ValueError("Schema and table names are required")

        pc = self.get_postgres_client(credential.connection_name)
        if not pc:
            raise ValueError(f"No active connection found with name: {credential.connection_name}")
        
        limit = 15
        logger.info(f"Previewing table: {credential.schema_name}.{credential.table_name}")
        
        query = f"""SELECT * FROM "{credential.schema_name}"."{credential.table_name}" LIMIT {limit}"""
        result = pc.execute_query(query)
        
        columns = pc.get_columns(table_name=credential.table_name, schema_name=credential.schema_name)
        
        return {
            "schema": credential.schema_name,
            "table": credential.table_name,
            "columns": columns,
            "rows": result,
            "row_count": len(result) if result else 0
        }            

    def get_table_columns(self, credential: DBCredential):
        """Get column information for a table."""
        if not credential.schema_name or not credential.table_name:
            raise ValueError("Schema and table names are required")

        pc = self.get_postgres_client(credential.connection_name)
        if not pc:
            raise ValueError(f"No active connection found with name: {credential.connection_name}")
        
        columns = pc.get_columns(credential.table_name, credential.schema_name)
        
        return {
            "schema": credential.schema_name,
            "table": credential.table_name,
            "columns": columns,
            "count": len(columns)
        }
    
    def get_primary_keys(self, credential: DBCredential):
        """Get primary keys for a table, with detection if none exist."""
        if not credential.schema_name or not credential.table_name:
            raise ValueError("Schema and table names are required")

        pc = self.get_postgres_client(credential.connection_name)
        if not pc:
            raise ValueError(f"No active connection found with name: {credential.connection_name}")
        
        logger.info(f"Getting primary keys for table: {credential.schema_name}.{credential.table_name}")
        
        existing_pks = pc.get_primary_keys(credential.schema_name, credential.table_name)
        has_pks = bool(existing_pks)
        
        detected_pks = []
        if not has_pks:
            detected_pks = pc.detect_primary_keys(credential.schema_name, credential.table_name)
        
        return {
            "schema": credential.schema_name,
            "table": credential.table_name,
            "primary_keys": existing_pks,
            "detected_keys": detected_pks,
            "has_primary_keys": has_pks
        }
    
    def _sync_to_airflow(self, config: DBConfig) -> bool:
        """Sync connection to Airflow - returns True if successful"""
        if not self.airflow:
            logger.error("Airflow client is not configured!")
            return False
        
        try:
            result = self.airflow.upsert_connection(config.model_dump())
            logger.info(f"Successfully synced connection '{config.connection_name}' to Airflow")
            return result
        except Exception as e:
            logger.error(f"Failed to sync to Airflow: {type(e).__name__}: {e}")
            return False
    
    def _delete_from_airflow(self, connection_name: str) -> bool:
        """Delete connection from Airflow - returns True if successful"""
        if not self.airflow:
            logger.error("Airflow client is not configured!")
            return False
        
        try:
            result = self.airflow.delete_connection(connection_name)
            logger.info(f"Deleted connection '{connection_name}' from Airflow")
            return result
        except Exception as e:
            logger.error(f"Failed to delete from Airflow: {type(e).__name__}: {e}")
            return False
    
    def verify_airflow_connection(self, connection_name: str) -> Dict:
        """Verify if connection exists in Airflow"""
        if not self.airflow:
            return {
                "exists": False,
                "error": "Airflow client not configured"
            }
        
        try:
            conn = self.airflow.get_connection(connection_name)
            return {
                "exists": conn is not None,
                "connection": conn
            }
        except Exception as e:
            return {
                "exists": False,
                "error": str(e)
            }