"""
Job management API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import Optional
import json
from pathlib import Path

from ..database.connection import get_db_session
from ..database.models import Job, JobStatus
from ..services.job_manager import JobManager
from ..services.input_handlers import FileUploadHandler, URLDownloadHandler, S3Handler
from ..models.job import (
    JobCreateResponse, JobStatusResponse, JobListResponse,
    JobProgress, JobResult, JobSummary
)
from ..config import settings

router = APIRouter()


@router.post("/jobs", response_model=JobCreateResponse)
async def create_job(
    file: Optional[UploadFile] = File(None),
    url: Optional[str] = Form(None),
    s3_bucket: Optional[str] = Form(None),
    s3_key: Optional[str] = Form(None),
    costbook_title: str = Form("WinSupply"),
    enable_ahri_enrichment: bool = Form(False),
    db: Session = Depends(get_db_session)
):
    """
    Submit a new ETL job.

    Supports three input methods:
    - File upload: Upload Excel/PDF file directly
    - URL: Provide a URL to download the file
    - S3: Provide S3 bucket and key

    Returns a job ID that can be used to check status and download results.
    """

    # Determine input source
    if file and file.filename:
        input_source = "upload"
        input_filename = file.filename
    elif url:
        input_source = "url"
        input_filename = Path(url.split("?")[0]).name or "downloaded_file"
    elif s3_bucket and s3_key:
        input_source = "s3"
        input_filename = Path(s3_key).name
    else:
        raise HTTPException(
            status_code=400,
            detail="Must provide file upload, URL, or S3 bucket/key"
        )

    # Create job record
    job = JobManager.create_job(
        db=db,
        input_source=input_source,
        input_filename=input_filename,
        costbook_title=costbook_title,
        enable_ahri_enrichment=enable_ahri_enrichment,
        input_url=url,
        s3_bucket=s3_bucket,
        s3_key=s3_key
    )

    # Setup input handler and get file
    job_input_dir = Path(settings.JOBS_DIR) / job.id / "input"

    try:
        if input_source == "upload":
            handler = FileUploadHandler(job_input_dir, file.file, file.filename)
        elif input_source == "url":
            handler = URLDownloadHandler(job_input_dir, url)
        else:
            handler = S3Handler(
                job_input_dir, s3_bucket, s3_key,
                settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY,
                settings.AWS_REGION
            )

        input_file_path = handler.get_input_file()
    except Exception as e:
        # Clean up job on input error
        db.delete(job)
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))

    # Submit for background processing
    JobManager.submit_job(job.id, str(input_file_path))

    return JobCreateResponse(
        job_id=job.id,
        status=job.status.value,
        created_at=job.created_at,
        message="Job queued for processing"
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    db: Session = Depends(get_db_session)
):
    """
    Get job status and progress.

    Returns current status, progress information, and results if completed.
    """
    job = JobManager.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Build progress info
    progress = JobProgress(
        current_stage=job.current_stage,
        stage_progress=job.progress_percent,
        message=job.progress_message
    )

    # Build result if completed
    result = None
    if job.status == JobStatus.COMPLETED and job.output_filename:
        result = JobResult(
            output_file=job.output_filename,
            download_url=f"/api/v1/jobs/{job_id}/download",
            stats=json.loads(job.stats_json) if job.stats_json else {}
        )

    return JobStatusResponse(
        job_id=job.id,
        status=job.status.value,
        progress=progress,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error_message if job.status == JobStatus.FAILED else None,
        result=result
    )


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    page: int = 1,
    page_size: int = 20,
    status: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """
    List all jobs with pagination.

    Optional status filter: pending, processing, completed, failed, cancelled
    """
    status_filter = None
    if status:
        try:
            status_filter = JobStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    jobs, total = JobManager.list_jobs(
        db=db,
        page=page,
        page_size=page_size,
        status_filter=status_filter
    )

    return JobListResponse(
        jobs=[
            JobSummary(
                job_id=j.id,
                status=j.status.value,
                input_filename=j.input_filename,
                created_at=j.created_at,
                completed_at=j.completed_at
            ) for j in jobs
        ],
        total=total,
        page=page,
        page_size=page_size
    )


@router.delete("/jobs/{job_id}")
async def cancel_or_delete_job(
    job_id: str,
    db: Session = Depends(get_db_session)
):
    """
    Cancel a pending job or delete a completed/failed job.

    - Pending jobs will be cancelled
    - Completed, failed, or cancelled jobs will be deleted along with their artifacts
    """
    job = JobManager.get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status == JobStatus.PENDING:
        if JobManager.cancel_job(db, job_id):
            return {"message": "Job cancelled", "job_id": job_id}

    if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
        if JobManager.delete_job(db, job_id):
            return {"message": "Job deleted", "job_id": job_id}

    raise HTTPException(
        status_code=400,
        detail=f"Cannot cancel/delete job with status: {job.status.value}. "
               "Only pending jobs can be cancelled, and only completed/failed/cancelled jobs can be deleted."
    )
