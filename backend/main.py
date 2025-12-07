from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from utils.helpers import create_logger
from routers.postgres_connection_router import router as postgres_router

logger = create_logger("Main")

# ==================== Create FastAPI Application ====================
app = FastAPI()
# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ==================== Include Routers ====================

app.include_router(postgres_router)
# ServiceContainer.get_postgres_service()  # Init ngay lúc startup

# app.include_router(mysql_router)
# app.include_router(mongodb_router)

logger.info("All routers registered")
