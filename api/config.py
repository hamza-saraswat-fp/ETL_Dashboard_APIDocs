"""
Application configuration from environment variables
"""
from pydantic_settings import BaseSettings
from typing import List
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Database
    DATABASE_URL: str = "sqlite:///./data/jobs.db"

    # API Keys
    OPENROUTER_API_KEY: str = ""

    # AWS (optional, for S3 support)
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"

    # Directories
    JOBS_DIR: str = "./jobs"
    CACHE_DIR: str = "./cache"
    LOGS_DIR: str = "./logs"

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    # Limits
    MAX_FILE_SIZE_MB: int = 100
    MAX_CONCURRENT_JOBS: int = 3

    # Cleanup
    JOB_RETENTION_DAYS: int = 7

    # LLM Settings
    LLM_MODEL: str = "anthropic/claude-sonnet-4"

    # LangWatch Integration
    LANGWATCH_API_KEY: str = ""
    LANGWATCH_ENABLED: bool = True

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    def ensure_directories(self):
        """Create required directories if they don't exist"""
        for dir_path in [self.JOBS_DIR, self.CACHE_DIR, self.LOGS_DIR]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)

        # Also ensure data directory for SQLite
        db_path = self.DATABASE_URL.replace("sqlite:///", "")
        if db_path.startswith("./"):
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()
