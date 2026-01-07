"""
Dashboard Routes for Admin Panel

Provides HTMX-powered dashboard views for monitoring and debugging.
"""
import json
import logging
from typing import Any, Dict, Optional
from pathlib import Path

from fastapi import APIRouter, Depends, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..database.connection import get_db_session
from ..database.models import Job, JobStatus, JobLineage
from ..services.metrics_service import get_metrics_service
from ..services.lineage_service import get_lineage_service
from ..services.diff_service import get_diff_service
from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# Templates will be set by main.py
templates: Optional[Jinja2Templates] = None


def get_templates() -> Jinja2Templates:
    """Get templates instance"""
    if templates is None:
        raise RuntimeError("Templates not initialized")
    return templates


# ============================================================================
# Main Dashboard Pages
# ============================================================================

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def dashboard_index(request: Request, db: Session = Depends(get_db_session)):
    """Main dashboard page"""
    metrics = get_metrics_service(db)
    summary = metrics.get_dashboard_summary()

    return get_templates().TemplateResponse(
        "dashboard/index.html",
        {
            "request": request,
            "active_page": "dashboard",
            "langwatch_enabled": bool(settings.LANGWATCH_API_KEY),
            "summary": summary,
        }
    )


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: str, db: Session = Depends(get_db_session)):
    """Job detail page with lineage, logs, and LLM metrics"""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get lineage
    lineage_service = get_lineage_service(db)
    lineage = lineage_service.get_lineage(job_id)
    lineage_data = lineage.to_dict() if lineage else None

    # Get LLM metrics (aggregated from lineage)
    llm_metrics = lineage_service.get_llm_metrics(job_id)

    # Get structured logs
    logs = None
    log_file = Path(settings.JOBS_DIR) / job_id / "logs" / "run.json"
    if log_file.exists():
        try:
            with open(log_file, 'r') as f:
                logs = json.load(f)
        except:
            pass

    return get_templates().TemplateResponse(
        "dashboard/job_detail.html",
        {
            "request": request,
            "active_page": "dashboard",
            "langwatch_enabled": bool(settings.LANGWATCH_API_KEY),
            "job": job.to_dict(),
            "lineage": lineage_data,
            "llm_metrics": llm_metrics,
            "logs": logs,
        }
    )


@router.get("/diff", response_class=HTMLResponse)
async def diff_viewer(
    request: Request,
    job1: Optional[str] = None,
    job2: Optional[str] = None,
    stage: str = "silver",
    db: Session = Depends(get_db_session)
):
    """Diff viewer page"""
    metrics = get_metrics_service(db)
    recent_jobs = metrics.get_recent_jobs(20)

    comparison = None
    if job1 and job2:
        diff_service = get_diff_service(settings.JOBS_DIR)
        comparison = diff_service.compare_jobs(job1, job2, stage)

    return get_templates().TemplateResponse(
        "dashboard/diff_viewer.html",
        {
            "request": request,
            "active_page": "diff",
            "langwatch_enabled": bool(settings.LANGWATCH_API_KEY),
            "recent_jobs": recent_jobs,
            "job1_id": job1,
            "job2_id": job2,
            "stage": stage,
            "comparison": comparison,
        }
    )


# ============================================================================
# HTMX Partials
# ============================================================================

@router.get("/health-badge", response_class=HTMLResponse)
async def health_badge(request: Request, db: Session = Depends(get_db_session)):
    """Health status badge partial"""
    metrics = get_metrics_service(db)
    health = metrics.get_system_health()

    status = health.get("status", "unknown")
    color_class = {
        "healthy": "bg-green-100 text-green-700",
        "degraded": "bg-yellow-100 text-yellow-700",
        "unhealthy": "bg-red-100 text-red-700",
    }.get(status, "bg-gray-100 text-gray-600")

    return f'''
    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {color_class}">
        <span class="w-2 h-2 rounded-full mr-1.5 {"bg-green-500" if status == "healthy" else "bg-yellow-500" if status == "degraded" else "bg-red-500"}"></span>
        {status.title()}
    </span>
    '''


@router.get("/jobs-partial", response_class=HTMLResponse)
async def jobs_partial(
    request: Request,
    limit: int = Query(default=10, le=50),
    db: Session = Depends(get_db_session)
):
    """Job list partial for HTMX refresh"""
    metrics = get_metrics_service(db)
    jobs = metrics.get_recent_jobs(limit)

    return get_templates().TemplateResponse(
        "dashboard/partials/job_list.html",
        {
            "request": request,
            "jobs": jobs,
        }
    )


@router.get("/metrics-partial", response_class=HTMLResponse)
async def metrics_partial(request: Request, db: Session = Depends(get_db_session)):
    """Metrics cards partial for HTMX refresh"""
    metrics = get_metrics_service(db)
    stats = metrics.get_job_stats(24)
    queue = metrics.get_queue_status()

    return get_templates().TemplateResponse(
        "dashboard/partials/metrics.html",
        {
            "request": request,
            "stats": stats,
            "queue": queue,
        }
    )


@router.get("/health-partial", response_class=HTMLResponse)
async def health_partial(request: Request, db: Session = Depends(get_db_session)):
    """Health status partial for HTMX refresh"""
    metrics = get_metrics_service(db)
    health = metrics.get_system_health()

    return get_templates().TemplateResponse(
        "dashboard/partials/health_status.html",
        {
            "request": request,
            "health": health,
        }
    )


@router.get("/jobs/{job_id}/logs-partial", response_class=HTMLResponse)
async def logs_partial(request: Request, job_id: str):
    """Logs viewer partial"""
    logs = None
    log_file = Path(settings.JOBS_DIR) / job_id / "logs" / "run.json"
    if log_file.exists():
        try:
            with open(log_file, 'r') as f:
                logs = json.load(f)
        except:
            pass

    return get_templates().TemplateResponse(
        "dashboard/partials/log_viewer.html",
        {
            "request": request,
            "logs": logs,
            "job_id": job_id,
        }
    )


@router.get("/jobs/{job_id}/lineage-partial", response_class=HTMLResponse)
async def lineage_partial(request: Request, job_id: str, db: Session = Depends(get_db_session)):
    """Lineage visualization partial"""
    job = db.query(Job).filter(Job.id == job_id).first()
    lineage_service = get_lineage_service(db)
    lineage = lineage_service.get_lineage(job_id)

    return get_templates().TemplateResponse(
        "dashboard/partials/lineage_flow.html",
        {
            "request": request,
            "job": job.to_dict() if job else None,
            "lineage": lineage.to_dict() if lineage else None,
        }
    )


# ============================================================================
# API Endpoints for Dashboard
# ============================================================================

@router.get("/api/jobs")
async def api_jobs(
    limit: int = Query(default=10, le=100),
    status: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """Get jobs list as JSON"""
    query = db.query(Job)
    if status:
        try:
            status_enum = JobStatus(status)
            query = query.filter(Job.status == status_enum)
        except ValueError:
            pass
    jobs = query.order_by(Job.created_at.desc()).limit(limit).all()
    return [job.to_dict() for job in jobs]


@router.get("/api/jobs/{job_id}/lineage")
async def api_job_lineage(job_id: str, db: Session = Depends(get_db_session)):
    """Get job lineage as JSON"""
    lineage_service = get_lineage_service(db)
    lineage = lineage_service.get_lineage(job_id)
    if not lineage:
        raise HTTPException(status_code=404, detail="Lineage not found")
    return lineage.to_dict()


@router.get("/api/jobs/{job_id}/llm-calls")
async def api_llm_calls(job_id: str, db: Session = Depends(get_db_session)):
    """Get LLM calls for a job"""
    lineage_service = get_lineage_service(db)
    calls = lineage_service.get_llm_calls(job_id)
    return {"job_id": job_id, "llm_calls": calls}


@router.get("/api/jobs/{job_id}/llm-metrics")
async def api_llm_metrics(job_id: str, db: Session = Depends(get_db_session)):
    """Get aggregated LLM metrics for a job"""
    lineage_service = get_lineage_service(db)
    metrics = lineage_service.get_llm_metrics(job_id)
    return {"job_id": job_id, "metrics": metrics}


@router.get("/api/jobs/{job_id}/logs")
async def api_job_logs(job_id: str):
    """Get structured logs for a job"""
    log_file = Path(settings.JOBS_DIR) / job_id / "logs" / "run.json"
    if not log_file.exists():
        raise HTTPException(status_code=404, detail="Logs not found")

    try:
        with open(log_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read logs: {e}")


@router.get("/api/metrics")
async def api_metrics(db: Session = Depends(get_db_session)):
    """Get dashboard metrics as JSON"""
    metrics = get_metrics_service(db)
    return metrics.get_dashboard_summary()


@router.get("/api/diff")
async def api_diff(
    job1: str,
    job2: str,
    stage: str = "silver",
    db: Session = Depends(get_db_session)
):
    """Compare two jobs and return diff"""
    diff_service = get_diff_service(settings.JOBS_DIR)
    result = diff_service.compare_jobs(job1, job2, stage)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
