"""
Health check endpoints
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from pathlib import Path
import time

from ..database.connection import get_db_session
from ..config import settings

router = APIRouter()

# Track startup time
_startup_time = time.time()


@router.get("/health")
async def health_check():
    """
    Basic health check endpoint.
    Returns service status and uptime.
    """
    return {
        "status": "healthy",
        "version": "1.0.0",
        "uptime_seconds": round(time.time() - _startup_time, 2)
    }


@router.get("/health/ready")
async def readiness_check(db: Session = Depends(get_db_session)):
    """
    Readiness check for container orchestration.
    Verifies all dependencies are available.
    """
    checks = {
        "database": False,
        "api_key": False,
        "jobs_dir": False,
        "cache_dir": False
    }

    # Check database connection
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = True
    except Exception:
        pass

    # Check API key configured
    checks["api_key"] = bool(settings.OPENROUTER_API_KEY)

    # Check directories exist
    checks["jobs_dir"] = Path(settings.JOBS_DIR).exists()
    checks["cache_dir"] = Path(settings.CACHE_DIR).exists()

    all_ready = all(checks.values())

    return {
        "ready": all_ready,
        "checks": checks
    }


@router.get("/health/live")
async def liveness_check():
    """
    Liveness check - simple ping to verify service is running.
    """
    return {"alive": True}
