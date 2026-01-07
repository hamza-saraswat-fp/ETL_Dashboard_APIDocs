"""
Structured JSON Logger for ETL Pipeline Jobs

Provides per-job logging with timestamps, step tracking, and metadata.
Logs are written to jobs/{job_id}/logs/run.json
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass, asdict
import threading


class LogLevel(str, Enum):
    """Log levels for structured logging"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class LogEntry:
    """A single log entry with timestamp and metadata"""
    timestamp: str
    level: str
    step: str
    message: str
    metadata: Optional[Dict[str, Any]] = None
    duration_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values"""
        result = {
            "timestamp": self.timestamp,
            "level": self.level,
            "step": self.step,
            "message": self.message,
        }
        if self.metadata:
            result["metadata"] = self.metadata
        if self.duration_ms is not None:
            result["duration_ms"] = self.duration_ms
        return result


class JobLogger:
    """
    Per-job structured JSON logger.

    Creates a JSON log file at jobs/{job_id}/logs/run.json with
    timestamped entries for each step of the pipeline.

    Thread-safe for concurrent logging.
    """

    def __init__(self, job_id: str, jobs_dir: str = "./jobs"):
        """
        Initialize job logger.

        Args:
            job_id: Unique job identifier
            jobs_dir: Base directory for job files
        """
        self.job_id = job_id
        self.jobs_dir = Path(jobs_dir)
        self.log_dir = self.jobs_dir / job_id / "logs"
        self.log_file = self.log_dir / "run.json"
        self._lock = threading.Lock()
        self._entries: List[LogEntry] = []
        self._stage_timers: Dict[str, float] = {}

        # Ensure log directory exists
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Initialize log file with job metadata
        self._init_log_file()

    def _init_log_file(self) -> None:
        """Initialize the log file with job metadata"""
        self._write_log({
            "job_id": self.job_id,
            "started_at": self._now(),
            "logs": []
        })

    def _now(self) -> str:
        """Get current timestamp in ISO format with timezone"""
        return datetime.now(timezone.utc).isoformat()

    def _write_log(self, data: Dict[str, Any]) -> None:
        """Write log data to file (thread-safe)"""
        with self._lock:
            with open(self.log_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)

    def _read_log(self) -> Dict[str, Any]:
        """Read current log data from file"""
        try:
            with open(self.log_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"job_id": self.job_id, "started_at": self._now(), "logs": []}

    def _append_entry(self, entry: LogEntry) -> None:
        """Append a log entry to the file"""
        with self._lock:
            data = self._read_log()
            data["logs"].append(entry.to_dict())
            data["last_updated"] = self._now()
            with open(self.log_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)

    def log(
        self,
        level: LogLevel,
        step: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[int] = None
    ) -> None:
        """
        Log a message with timestamp and optional metadata.

        Args:
            level: Log level (debug, info, warning, error)
            step: Step identifier (e.g., "stage1_extraction", "stage2_llm_call")
            message: Human-readable message
            metadata: Optional dictionary of additional data
            duration_ms: Optional duration in milliseconds
        """
        entry = LogEntry(
            timestamp=self._now(),
            level=level.value,
            step=step,
            message=message,
            metadata=metadata,
            duration_ms=duration_ms
        )
        self._append_entry(entry)

    def info(self, step: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Log an info message"""
        self.log(LogLevel.INFO, step, message, metadata)

    def debug(self, step: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Log a debug message"""
        self.log(LogLevel.DEBUG, step, message, metadata)

    def warning(self, step: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Log a warning message"""
        self.log(LogLevel.WARNING, step, message, metadata)

    def error(self, step: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Log an error message"""
        self.log(LogLevel.ERROR, step, message, metadata)

    def stage_start(self, stage_name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Mark the start of a pipeline stage.

        Args:
            stage_name: Name of the stage (e.g., "stage1_extraction")
            metadata: Optional metadata about the stage
        """
        self._stage_timers[stage_name] = time.time()
        self.info(
            step=stage_name,
            message=f"Starting {stage_name}",
            metadata=metadata
        )

    def stage_end(
        self,
        stage_name: str,
        metadata: Optional[Dict[str, Any]] = None,
        success: bool = True
    ) -> None:
        """
        Mark the end of a pipeline stage.

        Args:
            stage_name: Name of the stage
            metadata: Optional metadata about the result
            success: Whether the stage completed successfully
        """
        duration_ms = None
        if stage_name in self._stage_timers:
            duration_ms = int((time.time() - self._stage_timers[stage_name]) * 1000)
            del self._stage_timers[stage_name]

        level = LogLevel.INFO if success else LogLevel.ERROR
        status = "completed" if success else "failed"

        self.log(
            level=level,
            step=stage_name,
            message=f"Stage {stage_name} {status}",
            metadata=metadata,
            duration_ms=duration_ms
        )

    def llm_call(
        self,
        prompt_preview: str,
        response_preview: str,
        tokens: Dict[str, int],
        duration_ms: int,
        model: str,
        trace_id: Optional[str] = None
    ) -> None:
        """
        Log an LLM API call with details.

        Args:
            prompt_preview: First N characters of the prompt
            response_preview: First N characters of the response
            tokens: Token usage dict (prompt_tokens, completion_tokens, total_tokens)
            duration_ms: API call duration in milliseconds
            model: Model identifier
            trace_id: Optional LangWatch trace ID
        """
        metadata = {
            "model": model,
            "tokens": tokens,
            "prompt_preview": prompt_preview[:500] if prompt_preview else None,
            "response_preview": response_preview[:500] if response_preview else None,
        }
        if trace_id:
            metadata["langwatch_trace_id"] = trace_id

        self.log(
            level=LogLevel.INFO,
            step="llm_call",
            message=f"LLM call completed ({tokens.get('total_tokens', 'N/A')} tokens)",
            metadata=metadata,
            duration_ms=duration_ms
        )

    def record_input(self, file_path: str, file_hash: Optional[str] = None, file_size: Optional[int] = None) -> None:
        """Record input file information"""
        self.info(
            step="input",
            message=f"Processing input file: {Path(file_path).name}",
            metadata={
                "file_path": str(file_path),
                "file_hash": file_hash,
                "file_size_bytes": file_size
            }
        )

    def record_output(self, stage: str, file_path: str, record_count: Optional[int] = None) -> None:
        """Record stage output information"""
        metadata = {"file_path": str(file_path)}
        if record_count is not None:
            metadata["record_count"] = record_count

        self.info(
            step=f"{stage}_output",
            message=f"{stage.title()} output generated",
            metadata=metadata
        )

    def finalize(self, success: bool = True, error_message: Optional[str] = None) -> None:
        """
        Finalize the log file with completion status.

        Args:
            success: Whether the job completed successfully
            error_message: Optional error message if failed
        """
        with self._lock:
            data = self._read_log()
            data["completed_at"] = self._now()
            data["success"] = success
            if error_message:
                data["error"] = error_message

            # Calculate total duration
            if "started_at" in data:
                try:
                    start = datetime.fromisoformat(data["started_at"].replace('Z', '+00:00'))
                    end = datetime.now(timezone.utc)
                    data["total_duration_ms"] = int((end - start).total_seconds() * 1000)
                except:
                    pass

            with open(self.log_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)

    def get_logs(self) -> Dict[str, Any]:
        """Get all logs for this job"""
        return self._read_log()


def get_job_logger(job_id: str, jobs_dir: str = "./jobs") -> JobLogger:
    """
    Factory function to get a JobLogger instance.

    Args:
        job_id: Unique job identifier
        jobs_dir: Base directory for job files

    Returns:
        JobLogger instance for the job
    """
    return JobLogger(job_id, jobs_dir)
