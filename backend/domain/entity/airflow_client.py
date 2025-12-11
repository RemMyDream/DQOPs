import requests
from routers.dependencies import get_config
from dataclasses import dataclass, asdict

# ========= AIRFLOW CONFIG ==========

config = get_config()

@dataclass
class Airflow:
    """Service layer for Airflow Trigger Operation"""
    airflow_user: str = config['username']
    airflow_password: str = config['password']
    airflow_url: str = config['url']

    def to_dict(self):
        return asdict(self)
    