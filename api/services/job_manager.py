"""
Job manager - handles job lifecycle and background execution
"""
import uuid
import json
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from pathlib import Path
import logging

from sqlalchemy.orm import Session

from ..database.models import Job, JobStatus
from ..database.connection import SessionLocal
from .pipeline_orchestrator import PipelineOrchestrator
from ..config import settings

logger = logging.getLogger(__name__)

# Thread pool for background job processing
# Max 3 concurrent jobs is plenty for <50 jobs/day
executor = ThreadPoolExecutor(
    max_workers=settings.MAX_CONCURRENT_JOBS,
    thread_name_prefix="etl_worker"
)


class JobManager:
    """Manages job lifecycle and execution"""

    @staticmethod
    def create_job(
        db: Session,
        input_source: str,
        input_filename: str,
        costbook_title: str = "WinSupply",
        enable_ahri_enrichment: bool = False,
        input_url: str = None,
        s3_bucket: str = None,
        s3_key: str = None
    ) -> Job:
        """Create a new job record"""
        job = Job(
            id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            input_source=input_source,
            input_filename=input_filename,
            costbook_title=costbook_title,
            enable_ahri_enrichment=1 if enable_ahri_enrichment else 0,
            input_url=input_url,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            created_at=datetime.utcnow(),
            progress_percent=0,
            progress_message="Job created, waiting to start"
        )

        db.add(job)
        db.commit()
        db.refresh(job)

        logger.info(f"Created job {job.id} for {input_filename}")
        return job

    @staticmethod
    def submit_job(job_id: str, input_file_path: str):
        """Submit job for background processing"""
        executor.submit(JobManager._execute_job, job_id, input_file_path)
        logger.info(f"Submitted job {job_id} to executor")

    @staticmethod
    def _execute_job(job_id: str, input_file_path: str):
        """Execute job in background thread"""
        db = SessionLocal()
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if not job:
                logger.error(f"Job {job_id} not found")
                return

            # Update status to processing
            job.status = JobStatus.PROCESSING
            job.started_at = datetime.utcnow()
            job.progress_message = "Starting pipeline..."
            db.commit()

            # Create progress callback
            def update_progress(stage: str, percent: int, message: str):
                # Refresh the job object
                db.refresh(job)

                job.current_stage = stage
                job.progress_percent = percent
                job.progress_message = message

                # Update status based on stage
                stage_map = {
                    "stage1": JobStatus.STAGE1,
                    "stage2": JobStatus.STAGE2,
                    "stage3": JobStatus.STAGE3
                }
                if stage in stage_map:
                    job.status = stage_map[stage]

                db.commit()

            # Run pipeline
            orchestrator = PipelineOrchestrator(
                job_id=job_id,
                jobs_base_dir=settings.JOBS_DIR,
                openrouter_api_key=settings.OPENROUTER_API_KEY,
                llm_model=settings.LLM_MODEL,
                progress_callback=update_progress
            )

            results = orchestrator.run_pipeline(
                input_file=Path(input_file_path),
                costbook_title=job.costbook_title,
                enable_ahri_enrichment=bool(job.enable_ahri_enrichment)
            )

            # Update job with results
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.output_filename = results["output_file"]
            job.stats_json = json.dumps(results["stats"])
            job.source_type = results["stats"].get("source_type")
            job.systems_count = results["stats"].get("systems_count")
            job.progress_percent = 100
            job.progress_message = "Pipeline completed successfully"
            db.commit()

            logger.info(f"Job {job_id} completed successfully")

        except Exception as e:
            logger.exception(f"Job {job_id} failed")

            job = db.query(Job).filter(Job.id == job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.error_traceback = traceback.format_exc()
                job.completed_at = datetime.utcnow()
                job.progress_message = f"Failed: {str(e)[:100]}"
                db.commit()

        finally:
            db.close()

    @staticmethod
    def get_job(db: Session, job_id: str) -> Optional[Job]:
        """Get job by ID"""
        return db.query(Job).filter(Job.id == job_id).first()

    @staticmethod
    def list_jobs(
        db: Session,
        page: int = 1,
        page_size: int = 20,
        status_filter: Optional[JobStatus] = None
    ) -> tuple:
        """List jobs with pagination"""
        query = db.query(Job)

        if status_filter:
            query = query.filter(Job.status == status_filter)

        total = query.count()
        jobs = query.order_by(Job.created_at.desc()) \
            .offset((page - 1) * page_size) \
            .limit(page_size) \
            .all()

        return jobs, total

    @staticmethod
    def cancel_job(db: Session, job_id: str) -> bool:
        """Cancel a pending job"""
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return False

        if job.status == JobStatus.PENDING:
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.utcnow()
            job.progress_message = "Job cancelled by user"
            db.commit()
            logger.info(f"Job {job_id} cancelled")
            return True

        return False

    @staticmethod
    def delete_job(db: Session, job_id: str) -> bool:
        """Delete a job and its artifacts"""
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return False

        # Only allow deletion of completed, failed, or cancelled jobs
        if job.status not in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            return False

        # Clean up job directory
        job_dir = Path(settings.JOBS_DIR) / job_id
        if job_dir.exists():
            import shutil
            shutil.rmtree(job_dir)

        # Delete from database
        db.delete(job)
        db.commit()
        logger.info(f"Job {job_id} deleted")
        return True
