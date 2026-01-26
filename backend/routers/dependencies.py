# routers/dependencies.py
from typing import Dict, Any

from domain.entity.airflow_client import Airflow
from repositories.postgres_connection_repository import PostgresConnectionRepository
from repositories.job_repository import JobRepository
from repositories.job_version_repository import JobVersionRepository
from services.postgres_connection_service import PostgresConnectionService
from services.job_trigger_service import JobTriggerService
from services.job_service import JobService
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_airflow_config() -> dict:
    return {
        "username": os.environ.get("AIRFLOW_USERNAME", "admin"),
        "password": os.environ.get("AIRFLOW_PASSWORD", "admin"),
        "url": os.environ.get("AIRFLOW_URL", "http://webserver:8080")
    }


def get_postgres_config() -> dict:
    return {
        "host": os.environ.get("POSTGRES_HOST", "fastapi-postgres"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "username": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "database": os.environ.get("POSTGRES_DATABASE", "internal_database")
    }


class ServiceContainer:
    """Container for all repositories and services"""
    _repos: Dict[str, Any] = {}
    _services: Dict[str, Any] = {}
    _initialized: Dict[str, bool] = {}
    _airflow: Airflow = None

# ============== AIRFLOW ===============
    @classmethod
    def get_airflow(cls) -> Airflow:
        """Get singleton Airflow client"""
        if cls._airflow is None:
            cls._airflow = Airflow.from_dict(get_airflow_config())
        return cls._airflow

# ============== REPOSITORY ===============
    @classmethod
    def get_repo(cls, repo_name: str):
        """Get or create a repository"""
        if repo_name in cls._repos:
            return cls._repos[repo_name]
        
        repo_map = {
            'postgres_connection': PostgresConnectionRepository,
            'job': JobRepository,
            'job_version': JobVersionRepository
        }
        
        if repo_name not in repo_map:
            raise ValueError(f"Unknown repository: {repo_name}")
        
        cls._repos[repo_name] = repo_map[repo_name].from_dict(get_postgres_config())
        
        if not cls._initialized.get(repo_name):
            cls._repos[repo_name].init_table()
            cls._initialized[repo_name] = True
        
        return cls._repos[repo_name]

# ============== SERVICE ===============
    @classmethod
    def get_postgres_service(cls) -> PostgresConnectionService:
        if 'postgres' not in cls._services:
            repo = cls.get_repo('postgres_connection')
            airflow = cls.get_airflow()
            cls._services['postgres'] = PostgresConnectionService(repo, airflow)
        return cls._services['postgres']
    
    @classmethod
    def get_job_service(cls) -> JobService:
        if 'job' not in cls._services:
            job_repo = cls.get_repo('job')
            version_repo = cls.get_repo('job_version')
            cls._services['job'] = JobService(job_repo, version_repo)
        return cls._services['job']
    
    @classmethod
    def get_job_trigger_service(cls) -> JobTriggerService:
        if 'job_trigger' not in cls._services:
            airflow = cls.get_airflow()
            cls._services['job_trigger'] = JobTriggerService(airflow)
        return cls._services['job_trigger']

    @classmethod
    def reset(cls, name: str = None):
        """Reset services/repos"""
        if name is None:
            cls._repos.clear()
            cls._services.clear()
            cls._initialized.clear()
            cls._airflow = None
            logger.info("All services reset")
        elif name in cls._services:
            del cls._services[name]
            logger.info(f"Service {name} reset")

# ============== GETTER ===============
def get_postgres_service() -> PostgresConnectionService:
    return ServiceContainer.get_postgres_service()

def get_job_service() -> JobService:
    return ServiceContainer.get_job_service()

def get_job_trigger_service() -> JobTriggerService:
    return ServiceContainer.get_job_trigger_service()

def get_airflow() -> Airflow:
    return ServiceContainer.get_airflow()