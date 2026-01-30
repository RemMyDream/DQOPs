from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

from domain.entity.job_client import JobType
from services.job_service import JobService

from routers.dependencies import get_job_service
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/job",
    tags=["Job Information"],
    responses={404: {"description": "Not found"}},
)


