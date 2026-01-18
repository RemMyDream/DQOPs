import json
from typing import Optional, List
from domain.entity.postgres_client import PostgresConnectionClient
from domain.entity.source_client import SourceClient

logger = create_logger("PostgresConnectionRepository")


class PostgresConnectionRepository(PostgresConnectionClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_table(self):
        """Initialize the postgres_connections table if it doesn't exist"""
        logger.info("Initializing postgres_connections table")
        query = """
            CREATE TABLE IF NOT EXISTS postgres_connections (
                connection_id SERIAL PRIMARY KEY,   
                connection_name VARCHAR(100) NOT NULL,
                host VARCHAR(100) NOT NULL,
                port VARCHAR(50) NOT NULL,
                database VARCHAR(100) NOT NULL,
                username VARCHAR(100) NOT NULL,
                password TEXT NOT NULL,
                jdbc_properties JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'active'
            );
            
            CREATE INDEX IF NOT EXISTS idx_connection_name_status 
            ON postgres_connections(connection_name, status);
            
            CREATE UNIQUE INDEX IF NOT EXISTS idx_connection_name_active
            ON postgres_connections(connection_name) WHERE status = 'active';
        """
        self.execute_query(query)
        logger.info("Table initialization completed")

    def _serialize_params(self, pc: SourceClient) -> dict:
        """Convert SourceClient to database parameters"""
        logger.debug(f"Serializing params for connection: {pc.connection_name}")
        
        data = pc.to_dict()
        
        return {
            "connection_name": data.get("connection_name"),
            "host": data.get("host"),
            "port": data.get("port"),
            "username": data.get("username"),
            "password": data.get("password"),
            "database": data.get("database"),
            "jdbc_properties": json.dumps(data.get("jdbc_properties", {})),
        }

    def insert_connection(self, pc: SourceClient) -> bool:
        """Insert a new connection into the database"""
        logger.info(f"Attempting to insert connection: {pc.connection_name}")
        
        query = """
            INSERT INTO postgres_connections 
            (connection_name, host, port, username, password, database, jdbc_properties, status, created_at, last_update)
            VALUES (:connection_name, :host, :port, :username, :password, :database, :jdbc_properties,
                    'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """

        params = self._serialize_params(pc)

        try:
            self.execute_query(query, params)
            logger.info(f"Successfully inserted connection: {pc.connection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to insert connection {pc.connection_name}: {str(e)}", exc_info=True)
            raise e

    def get_active_connection(self, connection_name: str) -> Optional[SourceClient]:
        """Get an active connection by name"""
        logger.info(f"Fetching active connection: {connection_name}")
        
        query = """
            SELECT connection_name, host, port, username, password, database, jdbc_properties
            FROM postgres_connections
            WHERE connection_name = :connection_name AND status = 'active'
            LIMIT 1
        """
        res = self.execute_query(query, {"connection_name": connection_name})
        
        if not res:
            logger.info(f"No active connection found for: {connection_name}")
            return None

        pc = SourceClient.from_dict(res[0])
        logger.info(f"Successfully retrieved connection: {connection_name}")
        return pc
    
    def get_all_active_connections(self) -> List[SourceClient]:
        """Get all active connections"""
        logger.info("Fetching all active connections")
        
        query = """
            SELECT connection_name, host, port, username, password, database, jdbc_properties
            FROM postgres_connections
            WHERE status = 'active'
            ORDER BY created_at
        """
        rows = self.execute_query(query)
        
        if not rows:
            logger.info("No active connections found")
            return []

        result = [SourceClient.from_dict(row) for row in rows]
        logger.info(f"Successfully retrieved {len(result)} active connections")
        return result
    
    def soft_delete_connection(self, connection_name: str) -> bool:
        """Soft delete a connection by setting status to deleted"""
        logger.info(f"Soft deleting connection: {connection_name}")
        
        query = """
            UPDATE postgres_connections
            SET status = 'deleted', last_update = CURRENT_TIMESTAMP
            WHERE connection_name = :connection_name AND status = 'active'
        """
        try:
            self.execute_query(query, {"connection_name": connection_name})
            logger.info(f"Successfully soft deleted connection: {connection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to soft delete connection {connection_name}: {str(e)}")
            raise e