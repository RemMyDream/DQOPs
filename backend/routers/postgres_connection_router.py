from fastapi import APIRouter, HTTPException, Depends
from domain.request.table_connection_request import DBConfig, DBCredential
from .dependencies import get_postgres_service
from services.postgres_connection_service import PostgresConnectionService

logger = create_logger("PostgresConnectionRouter")

router = APIRouter(
    prefix="/postgres",
    tags=["PostgreSQL Connections"],
    responses={404: {"description": "Not found"}},
)


def _sanitize_connection(conn_info: dict) -> dict:
    """Remove sensitive data from connection info"""
    return {k: "*******" if k == "password" else v for k, v in conn_info.items()}


@router.post("/connections", 
             summary="Create or update PostgreSQL connection",
             response_description="Connection creation/update result")
def create_connection(
    config: DBConfig,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """Create or verify a PostgreSQL connection."""
    try:
        logger.info(f"Attempting to connect to database: {config.database} at {config.host}:{config.port}")

        result = service.upsert_connection(config)
        logger.info(f"Connection operation completed: {result['action']} - {config.connection_name}")
        
        return {
            'status': result['status'],
            'message': result['message'],
            'action': result['action'],
            'connection_name': config.connection_name,
            'database': config.database,
            'host': config.host,
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
    """Get all active PostgreSQL connections without password information."""
    try:
        connections = service.get_all_active_connections()
        sanitized_connections = [_sanitize_connection(conn.to_dict()) for conn in connections]
        
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
    """Get details of a specific PostgreSQL connection by name."""
    try:
        existing_conn = service.get_postgres_client(connection_name)
        
        if not existing_conn:
            raise HTTPException(
                status_code=404, 
                detail=f"Connection '{connection_name}' not found"
            )
        
        return {
            "status": "success",
            "connection": _sanitize_connection(existing_conn.to_dict())
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
    """Soft delete a PostgreSQL connection (mark as deleted)."""
    try:
        logger.info(f"Attempting to delete connection: {connection_name}")
        service.delete_connection(connection_name)
        logger.info(f"Successfully deleted connection: {connection_name}")
        
        return {
            "status": "success",
            "message": f"Connection '{connection_name}' has been deleted",
            "connection_name": connection_name
        }
        
    except ValueError as ve:
        logger.warning(f"Connection not found: {str(ve)}")
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to delete connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connections/cache")
def get_connection_cache(service: PostgresConnectionService = Depends(get_postgres_service)):
    """Get current connection cache"""
    return service._connection_cache


@router.delete("/connections/cache/{connection_name}")
def clear_connection_cache(
    connection_name: str,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """Clear specific connection from cache"""
    return service.clear_connection_cache(connection_name)


@router.post("/schemas/{connection_name}")
def get_schemas(
    connection_name: str,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """Get all schemas and tables for a connection."""
    try:
        logger.info(f"Fetching schemas for connection: {connection_name}")
        result = service.get_schemas_and_tables(connection_name)
        logger.info(f"Found {len(result.get('schemas', []))} schemas")
        
        return {"status": "success", **result}
    
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
    """Preview table data (first 15 rows)."""
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
    """Get column information for a table."""
    try:
        logger.info(f"Getting columns for: {credential.schema_name}.{credential.table_name}")
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
    """Get primary keys for a table, with detection if none exist."""
    try:
        logger.info(f"Getting primary keys for: {credential.schema_name}.{credential.table_name}")
        return service.get_primary_keys(credential)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Failed to get primary keys: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    
@router.get("/connections/{connection_name}/verify-airflow",
            summary="Verify connection exists in Airflow",
            response_description="Airflow connection status")
def verify_airflow_connection(
    connection_name: str,
    service: PostgresConnectionService = Depends(get_postgres_service)
):
    """Verify if connection exists in Airflow."""
    try:
        result = service.verify_airflow_connection(connection_name)
        
        if result["exists"]:
            return {
                "status": "success",
                "message": f"Connection '{connection_name}' exists in Airflow",
                "airflow_connection": result.get("connection")
            }
        else:
            return {
                "status": "not_found",
                "message": f"Connection '{connection_name}' not found in Airflow",
                "error": result.get("error")
            }
    
    except Exception as e:
        logger.error(f"Failed to verify Airflow connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))