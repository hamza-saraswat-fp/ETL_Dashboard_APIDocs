"""
App routes - User-facing upload UI
"""
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..database.connection import get_db_session
from ..database.models import Job

router = APIRouter()

# Templates will be set by main.py during startup
templates: Jinja2Templates = None


@router.get("")
async def app_page(request: Request, db: Session = Depends(get_db_session)):
    """
    User-facing upload UI for processing costbooks.

    Provides a clean interface for:
    - Uploading Excel/PDF files
    - Monitoring job progress
    - Downloading results
    - Viewing job history
    """
    # Get recent jobs for history section
    jobs = db.query(Job).order_by(Job.created_at.desc()).limit(10).all()

    return templates.TemplateResponse("app.html", {
        "request": request,
        "jobs": jobs
    })
