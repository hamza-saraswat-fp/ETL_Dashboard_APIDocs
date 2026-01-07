"""
Results download endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from pathlib import Path

from ..database.connection import get_db_session
from ..database.models import JobStatus
from ..services.job_manager import JobManager
from ..config import settings

router = APIRouter()


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

    file_path = Path(settings.JOBS_DIR) / job_id / "gold" / job.output_filename

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found on disk")

    return FileResponse(
        path=str(file_path),
        filename=job.output_filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
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

    # Find artifact file
    artifact_dir = Path(settings.JOBS_DIR) / job_id / stage
    if not artifact_dir.exists():
        raise HTTPException(status_code=404, detail=f"No {stage} artifacts found")

    # Get first file in directory
    files = list(artifact_dir.iterdir())
    if not files:
        raise HTTPException(status_code=404, detail=f"No files in {stage} directory")

    file_path = files[0]

    # Determine media type
    suffix = file_path.suffix.lower()
    media_types = {
        ".json": "application/json",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".pdf": "application/pdf"
    }
    media_type = media_types.get(suffix, "application/octet-stream")

    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type=media_type
    )
