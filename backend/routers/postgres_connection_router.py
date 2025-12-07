from typing import Dict
from fastapi import APIRouter, HTTPException, Depends
from domain.table_connection_client import DBConfig, DBCredential
from utils.helpers import create_logger
from .dependencies import get_postgres_service, get_config
from services.postgres_connection_service import PostgresConnectionService

logger = create_logger("Postgres Connection Router")


router = APIRouter(
    prefix="/postgres",
    tags=["PostgreSQL Connections"],
    responses={404: {"description": "Not found"}},
)

@router.post("/connections", 
             summary="Create or update PostgreSQL connection",
             response_description="Connection creation/update result")
def create_connection(
    config: DBConfig,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Create or verify a PostgreSQL connection.
    
    - **connectionName**: Unique name for the connection
    - **host**: Database host address
    - **port**: Database port number
    - **database**: Database name
    - **username**: Database username
    - **password**: Database password
    - **jdbcProperties**: Optional JDBC properties

    **Behavior**:
    - If connection exists and unchanged, reuse it
    - If changed, mark old as deleted and create new one
    - If new, create new connection
    """

    try:
        logger.info(f"Attempting to connect to database: {config.database} at {config.host}:{config.port}")

        result = service.upsert_connection(config)
        logger.info(f"Connection operation completed: {result['action']} - {config.connectionName}")
        
        return {
            'status': result['status'],
            'message': result['message'],
            'action': result['action'],
            'connectionName': config.connectionName,
            'database': config.database,
            'host' :config.host,
            'port': config.port
        }
    
    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/connections", 
             summary="List all active connections",
             response_description="List of active connections")
def list_connections(
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Get all active PostgreSQL connections.
    Returns a list of all active connections without password information.
    """
    try:
        connections = service.get_all_active_connections()
        
        # Remove sensitive data (passwords) before returning
        sanitized_connections = []
        for conn in connections:
            print(type(conn))
            conn_info = conn.to_dict()
            sanitized_connection = {}
            for k, v in conn_info.items():
                if k == "password":
                    sanitized_connection[k] = "*******"
                else:
                    sanitized_connection[k] = v
            
            sanitized_connections.append(sanitized_connection)
        
        return {
            "count": len(sanitized_connections),
            "connections": sanitized_connections
        }
    
    except Exception as e:
        logger.error(f"Failed to list connections: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/connections/{connection_name}",
            summary="Get specific connection details",
            response_description="Connection details")
def get_connection(
    connection_name: str,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Get details of a specific PostgreSQL connection by name.
    """
    try:
        existing_conn = service.get_postgres_client(connection_name)
        
        if not existing_conn:
            raise HTTPException(
                status_code=404, 
                detail=f"Connection '{connection_name}' not found"
            )
        conn_info = existing_conn.to_dict()
        # Remove password before returning
        sanitized_conn = {}
        for k, v in conn_info.items():
            if k == 'password':
                sanitized_conn[k] = "*******"
            else:
                sanitized_conn[k] = v
        
        return {
            "status": "success",
            "connection": sanitized_conn
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/connections/{connection_name}",
               summary="Delete a connection",
               response_description="Deletion result")
def delete_connection(
    connection_name: str,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Soft delete a PostgreSQL connection (mark as deleted).
    The connection is marked as deleted but data is preserved in the database.
    """
    try:
        logger.info(f"Attempting to delete connection: {connection_name}")
        
        # Call service to delete connection
        service.delete_connection(connection_name)
        
        logger.info(f"Successfully deleted connection: {connection_name}")
        
        return {
            "status": "success",
            "message": f"Connection '{connection_name}' has been deleted",
            "connectionName": connection_name
        }
        
    except ValueError as ve:
        logger.warning(f"Connection not found: {str(ve)}")
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to delete connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/connection/cache")
def get_connection_cache(service = Depends(get_postgres_service)):
    return service._connection_cache

@router.delete("/connection/cache")
def clear_connection_cache(connection_name, service = Depends(get_postgres_service)):
    return service.clear_connection_cache(connection_name)

# ==================== Metadata Operations ====================

@router.post("/schemas/{connection_name}")
def get_schemas(
    connection_name: str,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
        Get all schemas and tables for a connection.
    """
    try:
        logger.info(f"Fetching schemas for connection: {connection_name}")
        
        result = service.get_schemas_and_tables(connection_name)
        
        logger.info(f"Found {len(result.get('schemas', []))} schemas")
        
        return {
            "status": "success",
            **result
        }
    
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to fetch schemas: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/tables/preview")
def preview_table(
    credential: DBCredential,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Preview table data (first 15 rows).
    """
    try:
        return service.preview_table(credential)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to preview table: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/tables/columns")
def get_columns(
    credential: DBCredential,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Get column information for a table.
    """
    try:
        logger.info(f"Getting columns for: {credential.schemaName}.{credential.tableName}")
        return service.get_table_columns(credential)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get columns: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/tables/primary-keys")
def get_primary_keys(
    credential: DBCredential,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """
    Get primary keys for a table, with detection if none exist.
    
    This analyzes the table structure in the connected database.
    """
    try:
        logger.info(f"Getting primary keys for: {credential.schemaName}.{credential.tableName}")
        return service.get_primary_keys(credential)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get primary keys: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
