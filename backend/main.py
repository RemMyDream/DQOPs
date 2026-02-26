from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from routers.postgres_connection_router import router as postgres_router
from routers.job_trigger_router import router as job_trigger_router
from routers.profiling_router import router as profiling_router

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

# ==================== Health Check ====================
@app.get("/health")
def health_check():
    return {"status": "healthy"}

# ==================== Include Routers ====================

app.include_router(postgres_router)
app.include_router(job_trigger_router)
app.include_router(profiling_router)

