from functools import lru_cache
from typing import Optional, Dict, Any
from utils.helpers import load_cfg, create_logger
from repositories.postgres_connection_repository import PostgresConnectionRepository
from services.postgres_connection_service import PostgresConnectionService

logger = create_logger(name="Dependencies")
config = load_cfg(r"utils/config.yaml")

class ServiceContainer:
    """Container for managing service instances"""

    _instances: Dict[str, Any] = {}

    # Repository instances
    _postgres_repo: Optional[PostgresConnectionRepository] = None
    _table_initialized: bool = False

    # Service instances
    _postgres_service: Optional[PostgresConnectionService] = None
    
    @classmethod
    def get_postgres_repository(cls) -> PostgresConnectionRepository:
        """Get or create PostgresConnectionRepository singleton"""
        if cls._postgres_repo is None:
            logger.info(f"Creating PostgresConnectionRepository")
            cls._postgres_repo = PostgresConnectionRepository.from_dict(config['internal_database'])
            # Initialize database
            if not cls._table_initialized:
                try:
                    cls._postgres_repo.init_table()
                    cls._table_initialized = True
                    logger.info("Internal table initialized successfully")
                except Exception as e:
                    logger.error(f"Failed to initialize PostgreSQL tables: {e}")
                    raise 
        else:
            logger.info("Use existed PostgresConnectionRepository")
        return cls._postgres_repo
    
    @classmethod
    def get_postgres_service(cls) -> PostgresConnectionService:
        """Get or create PostgresConnectionService singleton"""
        if cls._postgres_service is None:
            logger.info("Creating new PostgresConnectionService")
            repo = cls.get_postgres_repository()
            cls._postgres_service = PostgresConnectionService(repo)
            logger.info("PostgresConnectionService initialized")
        else:
            logger.info("Use existing postgres service")
        return cls._postgres_service
    
    @classmethod
    def reset(cls, service_name: str = None):
        """
        Reset one specific service or all services if service_name is None.
        """

        # ----- Reset ALL -----
        if service_name is None:
            logger.info("Resetting ALL services")

            # Postgres service
            if cls._postgres_service:
                try:
                    cls._postgres_service.clear_connection_cache()
                except Exception as e:
                    logger.error(f"Error clearing postgres service cache: {e}")

            if cls._postgres_repo:
                try:
                    cls._postgres_repo.close()
                except Exception as e:
                    logger.error(f"Error closing postgres repository: {e}")

            # Reset everything
            cls._postgres_service = None
            cls._postgres_repo = None
            cls._instances.clear()

            logger.info("All services have been reset")
            return

        # ----- Reset ONE service -----
        logger.info(f"Resetting service '{service_name}'")

        if service_name == "postgres":
            if cls._postgres_service:
                try:
                    cls._postgres_service.clear_connection_cache()
                except Exception:
                    pass

            cls._postgres_service = None
            logger.info("Postgres service reset")
            return

        logger.warning(f"No reset logic for service '{service_name}'")

# ==================== FastAPI Dependency Functions ====================

def get_postgres_service() -> PostgresConnectionService:
    """FastAPI dependency for getting PostgresConnectionService"""
    return ServiceContainer.get_postgres_service()

def get_postgres_repository() -> PostgresConnectionRepository:
    """FastAPI dependency for getting PostgresConnectionRepository"""
    return ServiceContainer.get_postgres_repository()

def get_config() -> Dict[str, Any]:
    """FastAPI dependency for getting config"""
    return config