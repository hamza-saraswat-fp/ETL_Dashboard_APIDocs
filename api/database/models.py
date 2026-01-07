"""
SQLAlchemy models for the ETL API
"""
import enum
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, Enum, Integer, ForeignKey, create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class JobStatus(enum.Enum):
    """Job status enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    STAGE1 = "stage1"           # Extraction
    STAGE2 = "stage2"           # Transformation
    STAGE3 = "stage3"           # Loading
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Job(Base):
    """Job model for tracking ETL pipeline executions"""
    __tablename__ = "jobs"

    id = Column(String(36), primary_key=True)  # UUID
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)

    # Input details
    input_source = Column(String(20))           # "upload", "url", "s3"
    input_filename = Column(String(255))
    input_url = Column(Text, nullable=True)     # For URL source
    s3_bucket = Column(String(255), nullable=True)
    s3_key = Column(String(512), nullable=True)

    # Processing options
    costbook_title = Column(String(255), default="WinSupply")
    enable_ahri_enrichment = Column(Integer, default=0)  # SQLite boolean

    # Progress tracking
    current_stage = Column(String(50), nullable=True)
    progress_percent = Column(Integer, default=0)
    progress_message = Column(Text, nullable=True)

    # Results
    output_filename = Column(String(255), nullable=True)
    stats_json = Column(Text, nullable=True)    # JSON string

    # Error handling
    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Metadata
    source_type = Column(String(10), nullable=True)  # "excel" or "pdf"
    systems_count = Column(Integer, nullable=True)

    # LangWatch integration
    langwatch_trace_id = Column(String(255), nullable=True)
    langwatch_trace_url = Column(Text, nullable=True)

    # Relationship to lineage
    lineage = relationship("JobLineage", back_populates="job", uselist=False, cascade="all, delete-orphan")

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "id": self.id,
            "status": self.status.value if self.status else None,
            "input_source": self.input_source,
            "input_filename": self.input_filename,
            "input_url": self.input_url,
            "s3_bucket": self.s3_bucket,
            "s3_key": self.s3_key,
            "costbook_title": self.costbook_title,
            "enable_ahri_enrichment": bool(self.enable_ahri_enrichment),
            "current_stage": self.current_stage,
            "progress_percent": self.progress_percent,
            "progress_message": self.progress_message,
            "output_filename": self.output_filename,
            "stats_json": self.stats_json,
            "error_message": self.error_message,
            "source_type": self.source_type,
            "systems_count": self.systems_count,
            "langwatch_trace_id": self.langwatch_trace_id,
            "langwatch_trace_url": self.langwatch_trace_url,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class JobLineage(Base):
    """
    Lineage tracking for ETL pipeline jobs.

    Tracks the complete data flow: input -> bronze -> silver -> gold
    with links to LLM calls and LangWatch traces.
    """
    __tablename__ = "job_lineage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, unique=True)

    # Input tracking
    input_file_hash = Column(String(64), nullable=True)  # SHA256 hash
    input_file_size = Column(Integer, nullable=True)

    # Stage outputs
    bronze_output_path = Column(Text, nullable=True)
    bronze_record_count = Column(Integer, nullable=True)
    bronze_completed_at = Column(DateTime, nullable=True)

    silver_output_path = Column(Text, nullable=True)
    silver_systems_count = Column(Integer, nullable=True)
    silver_completed_at = Column(DateTime, nullable=True)

    gold_output_path = Column(Text, nullable=True)
    gold_row_count = Column(Integer, nullable=True)
    gold_completed_at = Column(DateTime, nullable=True)

    # LLM call tracking (JSON array)
    llm_calls_json = Column(Text, nullable=True)

    # Prompts used (JSON - stores prompt versions/hashes)
    prompts_json = Column(Text, nullable=True)

    # LangWatch trace
    langwatch_trace_id = Column(String(255), nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship back to job
    job = relationship("Job", back_populates="lineage")

    def to_dict(self):
        """Convert to dictionary"""
        import json as json_module

        llm_calls = None
        if self.llm_calls_json:
            try:
                llm_calls = json_module.loads(self.llm_calls_json)
            except:
                llm_calls = self.llm_calls_json

        prompts = None
        if self.prompts_json:
            try:
                prompts = json_module.loads(self.prompts_json)
            except:
                prompts = self.prompts_json

        return {
            "id": self.id,
            "job_id": self.job_id,
            "input_file_hash": self.input_file_hash,
            "input_file_size": self.input_file_size,
            "bronze": {
                "output_path": self.bronze_output_path,
                "record_count": self.bronze_record_count,
                "completed_at": self.bronze_completed_at.isoformat() if self.bronze_completed_at else None,
            },
            "silver": {
                "output_path": self.silver_output_path,
                "systems_count": self.silver_systems_count,
                "completed_at": self.silver_completed_at.isoformat() if self.silver_completed_at else None,
            },
            "gold": {
                "output_path": self.gold_output_path,
                "row_count": self.gold_row_count,
                "completed_at": self.gold_completed_at.isoformat() if self.gold_completed_at else None,
            },
            "llm_calls": llm_calls,
            "prompts": prompts,
            "langwatch_trace_id": self.langwatch_trace_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
