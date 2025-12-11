from typing import Optional, List, Dict
from backend.domain.entity.postgres_client import PostgresConnectionClient
from repositories.postgres_connection_repository import PostgresConnectionRepository
from backend.domain.request.table_connection_request import (DBConfig, DBCredential)

from utils.helpers import create_logger

logger = create_logger("Postgres Connection Service")

class PostgresConnectionService:
    def __init__(self, repo: PostgresConnectionRepository):
        self.repo = repo
        self._connection_cache: Dict[str, PostgresConnectionClient] = {}
    
    def upsert_connection(self, config: DBConfig) -> Dict:
        """
        Create new connection or update existing one.
        If connection exists with changes, mark old as deleted and create new.
        If unchanged, return existing.
        
        Args:
            config: Config of PostgresConnectionClient
        Returns:
            Dict with status and message
        """     

        # Check if connection already exists
        logger.info(f"Received config: {config}")
        logger.debug(f"config.model_dump() = {config.model_dump()}")
        
        connection_name = config.connectionName
        existing_conn = self.repo.get_active_connection(connection_name)
        
        if existing_conn:
            logger.debug(f"existing_conn found: {existing_conn.to_dict()}")
            is_same = existing_conn.has_same_config(config)
            logger.info(f"is_same = {is_same}")
            
            if not is_same:
                logger.info("Config changed, creating new connection")
                logger.debug(f"About to call from_dict with: {config.model_dump()}")
                
                pc = PostgresConnectionClient.from_dict(config.model_dump())
                # Test connection
                try:
                    pc.test_connection()
                except Exception as e:
                    raise ValueError(f"Connection test failed: {e}")
                
                # Mark old connection as deleted
                self.repo.soft_delete_connection(connection_name)
                # Clear cache
                self.clear_connection_cache(connection_name)
                # Insert new connection
                self.repo.insert_connection(pc)
                # Add to cache
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

        # Create new connection (no existing connection found)
        pc = PostgresConnectionClient.from_dict(config.model_dump())
        # Test connection
        try:
            pc.test_connection()
        except Exception as e:
            raise ValueError(f"Connection test failed: {e}")
        # Save to database
        self.repo.insert_connection(pc)
        # Add to cache
        self._connection_cache[connection_name] = pc      
        return {
            "status": "Success",
            "message": "Connection established successfully",
            "action": "Created"
        }        

    def get_postgres_client(self, connection_name: str) -> Optional[PostgresConnectionClient]:
        """
        Get or create a PostgreSQLClient instance for the given connection name.
        Returns:
            PostgreSQLClient instance if connection exists and is active, None otherwise
        """
        if connection_name in self._connection_cache:
            cached_client = self._connection_cache[connection_name]
            try:
                cached_client.test_connection()
                return cached_client
            except:
                del self._connection_cache[connection_name]
            
        # No connection in cache
        try:
            pc = self.repo.get_active_connection(connection_name)
            pc.test_connection()
            self._connection_cache[connection_name] = pc
            return pc
        except Exception as e:
            return None

    def get_all_active_connections(self) -> List[PostgresConnectionClient]:
        """Get all active connections, skipping ones that cannot connect"""
        res = self.repo.get_all_active_connections()
        connections = []

        for connection in res:
            try:
                connection.test_connection()
                connections.append(connection)
            except Exception as e:
                logger.warning(
                    f"Skipping connection '{connection.connectionName}' "
                    f"because test_connection() failed: {e}"
                )
                continue
        return connections

    def delete_connection(self, connection_name: str) -> bool:
        """
        Soft delete a connection (mark as deleted)
        Args:
            connection_name: Name of the connection to delete 
        Returns:
            bool: True if successful
        """
        existing_conn = self.get_postgres_client(connection_name)
        if not existing_conn:
            raise ValueError(f"No active connection found with name: {connection_name}")
        
        # Clear from cache first
        self.clear_connection_cache(connection_name)
        existing_conn.engine.dispose()
        # Soft clear connection in database
        return self.repo.soft_delete_connection(connection_name)

    def clear_connection_cache(self, connection_name: Optional[str] = None):
        """
        Clear connection cache.
        If connection_name is provided, clear only that connection.
        Otherwise clear all.
        """
        if connection_name:
            if connection_name in self._connection_cache:
                # Close connection before removing
                try:
                    self._connection_cache[connection_name].close()
                except:
                    pass
                del self._connection_cache[connection_name]
        else:
            # Close all connections
            for client in self._connection_cache.values():
                try:
                    client.engine.dispose()
                except:
                    pass
            self._connection_cache.clear()
#============== Metadata ===============
    def get_schemas_and_tables(self, connection_name: str) -> Dict:
        """
        Get all schemas and tables for a connection.
        Args:
            connection_name: Name of the connection
        Returns:
            Dict with schemas and tables 
        Raises:
            ValueError: If connection not found
        """
        logger.info(f"Fetching schemas for connection: {connection_name}")

        pc = self.get_postgres_client(connection_name)
        if not pc:
            raise ValueError(f"No active connection found with name: {connection_name}")
    
        return pc.get_schema_and_table()

    def preview_table(self, credential: DBCredential):
        """
        Preview table data (first 15 rows).       
        Args:
            credential: DBCredential
        Returns:
            Dict with schema, table, columns, rows and number of rows
        Raises:
            ValueError: If connection not found or invalid parameters
        """

        if not credential.schemaName or not credential.tableName:
            raise ValueError("Schema and table names are required")

        pc = self.get_postgres_client(credential.connectionName)
        if not pc:
            raise ValueError(f"No active connection found with name: {credential.connectionName}")
        
        limit = 15
        logger.info(f"Previewing table: {credential.schemaName}.{credential.tableName}")
        
        query = f"""SELECT * FROM "{credential.schemaName}"."{credential.tableName}" LIMIT {limit}"""
        result = pc.execute_query(query)
        
        columns = pc.get_columns(table_name=credential.tableName, schema_name=credential.schemaName)
        
        return {
            "schema": credential.schemaName,
            "table": credential.tableName,
            "columns": columns,
            "rows": result,
            "rowCount": len(result)
        }            
    

    def get_table_columns(self, credential: DBCredential):
        """
        Get column information for a table.
        Args:
            credential: DBCredential
        Returns:
            Dict with column information 
        Raises:
            ValueError: If connection not found or invalid parameters
        """

        if not credential.schemaName or not credential.tableName:
            raise ValueError("Schema and table names are required")

        pc = self.get_postgres_client(credential.connectionName)
        if not pc:
            raise ValueError(f"No active connection found with name: {credential.connectionName}")
        columns = pc.get_columns(credential.schemaName, credential.tableName)
        return {
            "schema": credential.schemaName,
            "table": credential.tableName,
            "columns": columns,
            "count": len(columns)
        }
    
    def get_primary_keys(self, credential: DBCredential):
        """
        Get primary keys for a table, with detection if none exist.
        Args:
            credential: DBCredential
        Returns:
            Dict with primary key information
        Raises:
            ValueError: If connection not found or invalid parameters
        """        
        if not credential.schemaName or not credential.tableName:
            raise ValueError("Schema and table names are required")

        pc = self.get_postgres_client(credential.connectionName)
        if not pc:
            raise ValueError(f"No active connection found with name: {credential.connectionName}")
        logger.info(f"Getting primary keys for table: {credential.schemaName}.{credential.tableName}")
        
        # Get existing primary keys
        existing_pks = pc.get_primary_keys(credential.schemaName, credential.tableName)
        has_pks = True if existing_pks else False
        
        # Detect potential primary keys if none exist
        detected_pks = []
        if not has_pks:
            detected_pks = pc.detect_primary_keys(credential.schemaName, credential.tableName)
            print(detected_pks)
        return {
            "schema": credential.schemaName,
            "table": credential.tableName,
            "primary_keys": existing_pks,
            "detected_keys": detected_pks,
            "has_primary_keys": has_pks
        }
