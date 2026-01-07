"""
Lineage Tracking Service for ETL Pipeline

Tracks the complete data flow through the pipeline stages
and links to LLM calls and LangWatch traces.
"""
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ..database.models import Job, JobLineage

logger = logging.getLogger(__name__)


class LineageService:
    """
    Service for tracking ETL pipeline lineage.

    Records input/output for each stage and LLM calls.
    """

    def __init__(self, db: Session):
        """
        Initialize lineage service.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db

    def create_lineage(self, job_id: str) -> JobLineage:
        """
        Create a new lineage record for a job.

        Args:
            job_id: Job ID

        Returns:
            New JobLineage instance
        """
        lineage = JobLineage(
            job_id=job_id,
            created_at=datetime.utcnow()
        )
        self.db.add(lineage)
        self.db.commit()
        self.db.refresh(lineage)
        logger.info(f"Created lineage record for job {job_id}")
        return lineage

    def get_lineage(self, job_id: str) -> Optional[JobLineage]:
        """
        Get lineage record for a job.

        Args:
            job_id: Job ID

        Returns:
            JobLineage or None if not found
        """
        return self.db.query(JobLineage).filter(JobLineage.job_id == job_id).first()

    def get_or_create_lineage(self, job_id: str) -> JobLineage:
        """
        Get existing lineage or create new one.

        Args:
            job_id: Job ID

        Returns:
            JobLineage instance
        """
        lineage = self.get_lineage(job_id)
        if not lineage:
            lineage = self.create_lineage(job_id)
        return lineage

    def record_input(
        self,
        job_id: str,
        file_path: str,
        file_size: Optional[int] = None
    ) -> None:
        """
        Record input file information.

        Args:
            job_id: Job ID
            file_path: Path to input file
            file_size: File size in bytes
        """
        lineage = self.get_or_create_lineage(job_id)

        # Calculate file hash
        file_hash = None
        path = Path(file_path)
        if path.exists():
            try:
                with open(path, 'rb') as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
                if file_size is None:
                    file_size = path.stat().st_size
            except Exception as e:
                logger.warning(f"Could not hash file {file_path}: {e}")

        lineage.input_file_hash = file_hash
        lineage.input_file_size = file_size
        self.db.commit()
        logger.debug(f"Recorded input for job {job_id}: hash={file_hash[:16] if file_hash else 'N/A'}...")

    def record_bronze_output(
        self,
        job_id: str,
        output_path: str,
        record_count: int
    ) -> None:
        """
        Record bronze stage output.

        Args:
            job_id: Job ID
            output_path: Path to bronze JSON output
            record_count: Number of records extracted
        """
        lineage = self.get_or_create_lineage(job_id)
        lineage.bronze_output_path = str(output_path)
        lineage.bronze_record_count = record_count
        lineage.bronze_completed_at = datetime.utcnow()
        self.db.commit()
        logger.debug(f"Recorded bronze output for job {job_id}: {record_count} records")

    def record_silver_output(
        self,
        job_id: str,
        output_path: str,
        systems_count: int
    ) -> None:
        """
        Record silver stage output.

        Args:
            job_id: Job ID
            output_path: Path to silver JSON output
            systems_count: Number of systems transformed
        """
        lineage = self.get_or_create_lineage(job_id)
        lineage.silver_output_path = str(output_path)
        lineage.silver_systems_count = systems_count
        lineage.silver_completed_at = datetime.utcnow()
        self.db.commit()
        logger.debug(f"Recorded silver output for job {job_id}: {systems_count} systems")

    def record_gold_output(
        self,
        job_id: str,
        output_path: str,
        row_count: int
    ) -> None:
        """
        Record gold stage output.

        Args:
            job_id: Job ID
            output_path: Path to gold Excel output
            row_count: Number of rows in output
        """
        lineage = self.get_or_create_lineage(job_id)
        lineage.gold_output_path = str(output_path)
        lineage.gold_row_count = row_count
        lineage.gold_completed_at = datetime.utcnow()
        self.db.commit()
        logger.debug(f"Recorded gold output for job {job_id}: {row_count} rows")

    def record_llm_call(
        self,
        job_id: str,
        prompt_hash: str,
        prompt_preview: str,
        response_preview: str,
        tokens: Dict[str, int],
        duration_ms: int,
        model: str,
        trace_id: Optional[str] = None
    ) -> None:
        """
        Record an LLM call in the lineage.

        Args:
            job_id: Job ID
            prompt_hash: Hash of the full prompt
            prompt_preview: Preview of the prompt (first N chars)
            response_preview: Preview of the response (first N chars)
            tokens: Token usage dict
            duration_ms: Call duration in milliseconds
            model: Model identifier
            trace_id: Optional LangWatch trace ID
        """
        lineage = self.get_or_create_lineage(job_id)

        # Load existing calls
        calls = []
        if lineage.llm_calls_json:
            try:
                calls = json.loads(lineage.llm_calls_json)
            except:
                calls = []

        # Add new call
        call_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "prompt_hash": prompt_hash,
            "prompt_preview": prompt_preview[:500] if prompt_preview else None,
            "response_preview": response_preview[:500] if response_preview else None,
            "tokens": tokens,
            "duration_ms": duration_ms,
            "model": model,
            "trace_id": trace_id
        }
        calls.append(call_record)

        lineage.llm_calls_json = json.dumps(calls)
        if trace_id and not lineage.langwatch_trace_id:
            lineage.langwatch_trace_id = trace_id
        self.db.commit()
        logger.debug(f"Recorded LLM call for job {job_id}: {tokens.get('total_tokens', 'N/A')} tokens")

    def record_prompt_version(
        self,
        job_id: str,
        prompt_name: str,
        prompt_hash: str,
        prompt_path: Optional[str] = None
    ) -> None:
        """
        Record prompt version used in transformation.

        Args:
            job_id: Job ID
            prompt_name: Name/identifier of the prompt
            prompt_hash: Hash of the prompt content
            prompt_path: Path to the prompt file
        """
        lineage = self.get_or_create_lineage(job_id)

        # Load existing prompts
        prompts = {}
        if lineage.prompts_json:
            try:
                prompts = json.loads(lineage.prompts_json)
            except:
                prompts = {}

        # Add/update prompt
        prompts[prompt_name] = {
            "hash": prompt_hash,
            "path": prompt_path,
            "recorded_at": datetime.utcnow().isoformat()
        }

        lineage.prompts_json = json.dumps(prompts)
        self.db.commit()
        logger.debug(f"Recorded prompt version for job {job_id}: {prompt_name}")

    def set_langwatch_trace(self, job_id: str, trace_id: str) -> None:
        """
        Set the LangWatch trace ID for a job.

        Args:
            job_id: Job ID
            trace_id: LangWatch trace ID
        """
        lineage = self.get_or_create_lineage(job_id)
        lineage.langwatch_trace_id = trace_id
        self.db.commit()

        # Also update the job record
        job = self.db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.langwatch_trace_id = trace_id
            job.langwatch_trace_url = f"https://app.langwatch.ai/traces/{trace_id}"
            self.db.commit()

        logger.debug(f"Set LangWatch trace for job {job_id}: {trace_id}")

    def get_llm_calls(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get all LLM calls for a job.

        Args:
            job_id: Job ID

        Returns:
            List of LLM call records
        """
        lineage = self.get_lineage(job_id)
        if not lineage or not lineage.llm_calls_json:
            return []

        try:
            return json.loads(lineage.llm_calls_json)
        except:
            return []

    def get_prompts(self, job_id: str) -> Dict[str, Any]:
        """
        Get prompt versions used in a job.

        Args:
            job_id: Job ID

        Returns:
            Dict of prompt name -> version info
        """
        lineage = self.get_lineage(job_id)
        if not lineage or not lineage.prompts_json:
            return {}

        try:
            return json.loads(lineage.prompts_json)
        except:
            return {}

    def get_llm_metrics(self, job_id: str) -> Dict[str, Any]:
        """
        Get aggregated LLM metrics for a job.

        Computes totals from all recorded LLM calls.

        Args:
            job_id: Job ID

        Returns:
            Dict with aggregated metrics:
            - call_count: Number of LLM calls
            - total_prompt_tokens: Total prompt tokens
            - total_completion_tokens: Total completion tokens
            - total_tokens: Total tokens (prompt + completion)
            - total_duration_ms: Total duration in milliseconds
            - avg_duration_ms: Average duration per call
            - estimated_cost: Estimated cost in USD
            - models_used: List of models used
        """
        calls = self.get_llm_calls(job_id)

        if not calls:
            return {
                "call_count": 0,
                "total_prompt_tokens": 0,
                "total_completion_tokens": 0,
                "total_tokens": 0,
                "total_duration_ms": 0,
                "avg_duration_ms": 0,
                "estimated_cost": 0.0,
                "models_used": []
            }

        total_prompt = 0
        total_completion = 0
        total_duration = 0
        models = set()

        for call in calls:
            tokens = call.get("tokens", {})
            total_prompt += tokens.get("prompt_tokens", 0) or 0
            total_completion += tokens.get("completion_tokens", 0) or 0
            total_duration += call.get("duration_ms", 0) or 0
            if call.get("model"):
                models.add(call["model"])

        total_tokens = total_prompt + total_completion
        call_count = len(calls)

        # Cost estimation based on Claude Sonnet pricing via OpenRouter
        # Input: $3/1M tokens, Output: $15/1M tokens (approximate)
        estimated_cost = (total_prompt * 0.000003) + (total_completion * 0.000015)

        return {
            "call_count": call_count,
            "total_prompt_tokens": total_prompt,
            "total_completion_tokens": total_completion,
            "total_tokens": total_tokens,
            "total_duration_ms": total_duration,
            "avg_duration_ms": total_duration // call_count if call_count > 0 else 0,
            "estimated_cost": round(estimated_cost, 4),
            "models_used": list(models)
        }


def get_lineage_service(db: Session) -> LineageService:
    """
    Factory function to get a LineageService instance.

    Args:
        db: SQLAlchemy database session

    Returns:
        LineageService instance
    """
    return LineageService(db)
