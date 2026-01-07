"""
Pydantic models for job-related API requests and responses
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class JobProgress(BaseModel):
    """Progress information for a running job"""
    current_stage: Optional[str] = Field(None, description="Current pipeline stage (stage1, stage2, stage3)")
    stage_progress: Optional[int] = Field(None, description="Progress percentage (0-100)")
    message: Optional[str] = Field(None, description="Human-readable status message")


class JobResult(BaseModel):
    """Result information for a completed job"""
    output_file: str = Field(..., description="Filename of the output Excel file")
    download_url: str = Field(..., description="URL to download the result")
    stats: Dict[str, Any] = Field(default_factory=dict, description="Processing statistics")


class JobCreateResponse(BaseModel):
    """Response for job creation"""
    job_id: str = Field(..., description="Unique job identifier (UUID)")
    status: str = Field(..., description="Initial job status")
    created_at: datetime = Field(..., description="Job creation timestamp")
    message: str = Field(..., description="Status message")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class JobStatusResponse(BaseModel):
    """Response for job status query"""
    job_id: str = Field(..., description="Unique job identifier")
    status: str = Field(..., description="Current job status")
    progress: JobProgress = Field(..., description="Progress information")
    created_at: datetime = Field(..., description="Job creation timestamp")
    started_at: Optional[datetime] = Field(None, description="Processing start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    error: Optional[str] = Field(None, description="Error message if failed")
    result: Optional[JobResult] = Field(None, description="Result info if completed")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class JobSummary(BaseModel):
    """Summary information for job listing"""
    job_id: str = Field(..., description="Unique job identifier")
    status: str = Field(..., description="Current job status")
    input_filename: str = Field(..., description="Input file name")
    created_at: datetime = Field(..., description="Job creation timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class JobListResponse(BaseModel):
    """Response for job listing"""
    jobs: List[JobSummary] = Field(..., description="List of jobs")
    total: int = Field(..., description="Total number of jobs")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
