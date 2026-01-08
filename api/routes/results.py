"""
Results download endpoints - supports both local and cloud storage
"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, Response, RedirectResponse
from sqlalchemy.orm import Session
from pathlib import Path
import logging

from ..database.connection import get_db_session
from ..database.models import JobStatus
from ..services.job_manager import JobManager
from ..config import settings

# Storage service for cloud downloads
try:
    from ..services.storage_service import get_storage_service
    _storage_available = True
except ImportError:
    _storage_available = False
    def get_storage_service(): return None

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_file_from_storage(job_id: str, stage: str, filename: str):
    """
    Get a file from storage (cloud or local).
    Returns tuple of (content_bytes, found_locally)
    """
    storage = get_storage_service() if _storage_available else None

    # Try cloud storage first if configured
    if storage and storage.use_cloud:
        content = storage.download_file(job_id, stage, filename)
        if content:
            return content, False

    # Fall back to local filesystem
    local_path = Path(settings.JOBS_DIR) / job_id / stage / filename
    if local_path.exists():
        return local_path.read_bytes(), True

    return None, False


@router.get("/jobs/{job_id}/download")
async def download_result(
    job_id: str,
    db: Session = Depends(get_db_session)
):
    """
    Download the Gold Excel result for a completed job.

    Returns the costbook Excel file.
    """
    job = JobManager.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job not completed. Status: {job.status.value}"
        )

    if not job.output_filename:
        raise HTTPException(status_code=404, detail="Output file not found")

    # Try to get file from storage
    content, is_local = _get_file_from_storage(job_id, "gold", job.output_filename)

    if content is None:
        raise HTTPException(status_code=404, detail="Output file not found")

    # If local file exists, use FileResponse for efficiency
    if is_local:
        file_path = Path(settings.JOBS_DIR) / job_id / "gold" / job.output_filename
        return FileResponse(
            path=str(file_path),
            filename=job.output_filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # Return content from cloud storage
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{job.output_filename}"'}
    )


@router.get("/jobs/{job_id}/artifacts/{stage}")
async def download_artifact(
    job_id: str,
    stage: str,
    db: Session = Depends(get_db_session)
):
    """
    Download intermediate artifacts (bronze/silver/gold).

    - bronze: Extracted JSON from Stage 1
    - silver: Transformed JSON from Stage 2
    - gold: Final Excel costbook from Stage 3

    Useful for debugging and inspection.
    """
    if stage not in ["bronze", "silver", "gold", "input"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid stage. Use: input, bronze, silver, gold"
        )

    job = JobManager.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    storage = get_storage_service() if _storage_available else None

    # Try to find and download the artifact
    filename = None
    content = None

    # Try cloud storage first
    if storage and storage.use_cloud:
        files = storage.list_files(job_id, stage)
        if files:
            filename = files[0]
            content = storage.download_file(job_id, stage, filename)

    # Fall back to local filesystem
    if content is None:
        artifact_dir = Path(settings.JOBS_DIR) / job_id / stage
        if artifact_dir.exists():
            local_files = list(artifact_dir.iterdir())
            if local_files:
                file_path = local_files[0]
                filename = file_path.name
                return FileResponse(
                    path=str(file_path),
                    filename=filename,
                    media_type=_get_media_type(filename)
                )

    if content is None or filename is None:
        raise HTTPException(status_code=404, detail=f"No {stage} artifacts found")

    return Response(
        content=content,
        media_type=_get_media_type(filename),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


def _get_media_type(filename: str) -> str:
    """Get media type from filename"""
    suffix = Path(filename).suffix.lower()
    media_types = {
        ".json": "application/json",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".pdf": "application/pdf"
    }
    return media_types.get(suffix, "application/octet-stream")
