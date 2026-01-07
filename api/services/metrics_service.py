"""
Metrics Service for Admin Dashboard

Calculates job statistics, queue status, and system health metrics.
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from pathlib import Path

from sqlalchemy import func, and_, text
from sqlalchemy.orm import Session

from ..database.models import Job, JobStatus, JobLineage
from ..config import settings

logger = logging.getLogger(__name__)


class MetricsService:
    """
    Service for calculating dashboard metrics.
    """

    def __init__(self, db: Session):
        """
        Initialize metrics service.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db

    def get_job_counts_by_status(self) -> Dict[str, int]:
        """
        Get count of jobs by status.

        Returns:
            Dict mapping status -> count
        """
        counts = {}
        for status in JobStatus:
            count = self.db.query(Job).filter(Job.status == status).count()
            counts[status.value] = count
        return counts

    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current job queue status.

        Returns:
            Dict with queue metrics
        """
        counts = self.get_job_counts_by_status()

        # Calculate active jobs (not terminal states)
        active_statuses = [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.STAGE1, JobStatus.STAGE2, JobStatus.STAGE3]
        active_count = sum(counts.get(s.value, 0) for s in active_statuses)

        return {
            "pending": counts.get("pending", 0),
            "processing": counts.get("processing", 0) + counts.get("stage1", 0) + counts.get("stage2", 0) + counts.get("stage3", 0),
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "cancelled": counts.get("cancelled", 0),
            "active": active_count,
            "total": sum(counts.values()),
            "max_concurrent": settings.MAX_CONCURRENT_JOBS,
        }

    def get_job_stats(self, period_hours: int = 24) -> Dict[str, Any]:
        """
        Get job statistics for a time period.

        Args:
            period_hours: Number of hours to look back

        Returns:
            Dict with job statistics
        """
        since = datetime.utcnow() - timedelta(hours=period_hours)

        # Jobs in period
        jobs_in_period = self.db.query(Job).filter(Job.created_at >= since).all()

        if not jobs_in_period:
            return {
                "period_hours": period_hours,
                "total_jobs": 0,
                "completed": 0,
                "failed": 0,
                "success_rate": 0.0,
                "avg_duration_seconds": None,
                "total_systems_processed": 0,
            }

        # Count by status
        completed = sum(1 for j in jobs_in_period if j.status == JobStatus.COMPLETED)
        failed = sum(1 for j in jobs_in_period if j.status == JobStatus.FAILED)
        total_terminal = completed + failed

        # Success rate
        success_rate = (completed / total_terminal * 100) if total_terminal > 0 else 0.0

        # Average duration for completed jobs
        durations = []
        for job in jobs_in_period:
            if job.status == JobStatus.COMPLETED and job.started_at and job.completed_at:
                duration = (job.completed_at - job.started_at).total_seconds()
                durations.append(duration)

        avg_duration = sum(durations) / len(durations) if durations else None

        # Total systems processed
        total_systems = sum(j.systems_count or 0 for j in jobs_in_period if j.systems_count)

        return {
            "period_hours": period_hours,
            "total_jobs": len(jobs_in_period),
            "completed": completed,
            "failed": failed,
            "success_rate": round(success_rate, 1),
            "avg_duration_seconds": round(avg_duration, 1) if avg_duration else None,
            "total_systems_processed": total_systems,
        }

    def get_recent_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get most recent jobs.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job dicts
        """
        jobs = self.db.query(Job).order_by(Job.created_at.desc()).limit(limit).all()
        return [job.to_dict() for job in jobs]

    def get_recent_errors(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get recent failed jobs with error details.

        Args:
            limit: Maximum number of errors to return

        Returns:
            List of error details
        """
        failed_jobs = (
            self.db.query(Job)
            .filter(Job.status == JobStatus.FAILED)
            .order_by(Job.completed_at.desc())
            .limit(limit)
            .all()
        )

        errors = []
        for job in failed_jobs:
            errors.append({
                "job_id": job.id,
                "filename": job.input_filename,
                "error_message": job.error_message,
                "failed_at": job.completed_at.isoformat() if job.completed_at else None,
                "stage": job.current_stage,
            })
        return errors

    def get_system_health(self) -> Dict[str, Any]:
        """
        Get system health status.

        Returns:
            Dict with health check results
        """
        health = {
            "status": "healthy",
            "checks": {},
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Check database
        try:
            self.db.execute(text("SELECT 1"))
            health["checks"]["database"] = {"status": "ok", "message": "Connected"}
        except Exception as e:
            health["checks"]["database"] = {"status": "error", "message": str(e)}
            health["status"] = "unhealthy"

        # Check API key
        if settings.OPENROUTER_API_KEY:
            health["checks"]["api_key"] = {"status": "ok", "message": "Configured"}
        else:
            health["checks"]["api_key"] = {"status": "warning", "message": "Not configured"}
            if health["status"] == "healthy":
                health["status"] = "degraded"

        # Check LangWatch
        if settings.LANGWATCH_API_KEY:
            health["checks"]["langwatch"] = {"status": "ok", "message": "Configured"}
        else:
            health["checks"]["langwatch"] = {"status": "info", "message": "Not configured (optional)"}

        # Check directories
        dirs_to_check = [
            ("jobs_dir", settings.JOBS_DIR),
            ("cache_dir", settings.CACHE_DIR),
            ("logs_dir", settings.LOGS_DIR),
        ]
        for name, dir_path in dirs_to_check:
            path = Path(dir_path)
            if path.exists() and path.is_dir():
                health["checks"][name] = {"status": "ok", "message": f"Exists: {dir_path}"}
            else:
                health["checks"][name] = {"status": "warning", "message": f"Missing: {dir_path}"}

        return health

    def get_llm_usage_stats(self, period_hours: int = 24) -> Dict[str, Any]:
        """
        Get LLM usage statistics.

        Args:
            period_hours: Number of hours to look back

        Returns:
            Dict with LLM usage stats
        """
        since = datetime.utcnow() - timedelta(hours=period_hours)

        # Get lineage records with LLM calls
        lineages = (
            self.db.query(JobLineage)
            .filter(JobLineage.created_at >= since)
            .filter(JobLineage.llm_calls_json.isnot(None))
            .all()
        )

        total_calls = 0
        total_tokens = 0
        total_prompt_tokens = 0
        total_completion_tokens = 0
        total_duration_ms = 0

        import json
        for lineage in lineages:
            try:
                calls = json.loads(lineage.llm_calls_json)
                for call in calls:
                    total_calls += 1
                    tokens = call.get("tokens", {})
                    total_tokens += tokens.get("total_tokens", 0) or 0
                    total_prompt_tokens += tokens.get("prompt_tokens", 0) or 0
                    total_completion_tokens += tokens.get("completion_tokens", 0) or 0
                    total_duration_ms += call.get("duration_ms", 0) or 0
            except:
                pass

        avg_duration_ms = total_duration_ms / total_calls if total_calls > 0 else 0

        return {
            "period_hours": period_hours,
            "total_calls": total_calls,
            "total_tokens": total_tokens,
            "total_prompt_tokens": total_prompt_tokens,
            "total_completion_tokens": total_completion_tokens,
            "avg_duration_ms": round(avg_duration_ms, 0),
            "estimated_cost_usd": round(total_tokens * 0.000003, 4),  # Rough estimate
        }

    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Get complete dashboard summary.

        Returns:
            Dict with all dashboard metrics
        """
        return {
            "queue": self.get_queue_status(),
            "stats_24h": self.get_job_stats(24),
            "stats_7d": self.get_job_stats(24 * 7),
            "recent_jobs": self.get_recent_jobs(10),
            "recent_errors": self.get_recent_errors(5),
            "health": self.get_system_health(),
            "llm_usage": self.get_llm_usage_stats(24),
        }


def get_metrics_service(db: Session) -> MetricsService:
    """
    Factory function to get a MetricsService instance.

    Args:
        db: SQLAlchemy database session

    Returns:
        MetricsService instance
    """
    return MetricsService(db)
