"""
Input handlers for different source types (file upload, URL, S3)
"""
import os
import shutil
from pathlib import Path
from typing import BinaryIO
import httpx
import logging

logger = logging.getLogger(__name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".xlsm", ".xlsb", ".pdf"}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB


class InputHandler:
    """Base class for input handlers"""

    def __init__(self, job_input_dir: Path):
        self.job_input_dir = Path(job_input_dir)
        self.job_input_dir.mkdir(parents=True, exist_ok=True)

    def get_input_file(self) -> Path:
        """Return path to the input file - implemented by subclasses"""
        raise NotImplementedError

    def validate_extension(self, filename: str) -> None:
        """Validate file extension"""
        suffix = Path(filename).suffix.lower()
        if suffix not in ALLOWED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type: {suffix}. "
                f"Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
            )


class FileUploadHandler(InputHandler):
    """Handle direct file uploads"""

    def __init__(self, job_input_dir: Path, file: BinaryIO, filename: str):
        super().__init__(job_input_dir)
        self.file = file
        self.filename = filename
        self.validate_extension(filename)

    def get_input_file(self) -> Path:
        """Save uploaded file and return path"""
        # Sanitize filename
        safe_filename = Path(self.filename).name
        target_path = self.job_input_dir / safe_filename

        # Write file
        with open(target_path, "wb") as f:
            shutil.copyfileobj(self.file, f)

        file_size = target_path.stat().st_size
        if file_size > MAX_FILE_SIZE:
            target_path.unlink()
            raise ValueError(f"File too large: {file_size / 1024 / 1024:.1f}MB (max: {MAX_FILE_SIZE / 1024 / 1024}MB)")

        logger.info(f"Saved uploaded file: {target_path} ({file_size / 1024:.1f}KB)")
        return target_path


class URLDownloadHandler(InputHandler):
    """Handle URL downloads"""

    def __init__(self, job_input_dir: Path, url: str):
        super().__init__(job_input_dir)
        self.url = url

    def get_input_file(self) -> Path:
        """Download file from URL and return path"""
        # Extract filename from URL
        url_path = self.url.split("?")[0]  # Remove query params
        filename = Path(url_path).name or "downloaded_file.xlsx"

        # Validate extension
        self.validate_extension(filename)

        target_path = self.job_input_dir / filename

        # Download with streaming
        logger.info(f"Downloading file from URL: {self.url}")
        with httpx.Client(timeout=300.0, follow_redirects=True) as client:
            with client.stream("GET", self.url) as response:
                response.raise_for_status()

                # Check content length
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > MAX_FILE_SIZE:
                    raise ValueError(f"File too large: {int(content_length) / 1024 / 1024:.1f}MB")

                total_bytes = 0
                with open(target_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        total_bytes += len(chunk)
                        if total_bytes > MAX_FILE_SIZE:
                            f.close()
                            target_path.unlink()
                            raise ValueError(f"File too large: exceeded {MAX_FILE_SIZE / 1024 / 1024}MB")
                        f.write(chunk)

        logger.info(f"Downloaded file from URL: {target_path} ({total_bytes / 1024:.1f}KB)")
        return target_path


class S3Handler(InputHandler):
    """Handle S3 bucket downloads"""

    def __init__(
        self,
        job_input_dir: Path,
        bucket: str,
        key: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_region: str = None
    ):
        super().__init__(job_input_dir)
        self.bucket = bucket
        self.key = key
        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = aws_region or os.getenv("AWS_REGION", "us-east-1")

        # Validate extension
        filename = Path(key).name
        self.validate_extension(filename)

    def get_input_file(self) -> Path:
        """Download file from S3 and return path"""
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError:
            raise ImportError("boto3 is required for S3 support. Install with: pip install boto3")

        filename = Path(self.key).name
        target_path = self.job_input_dir / filename

        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )

        try:
            logger.info(f"Downloading file from S3: s3://{self.bucket}/{self.key}")
            s3_client.download_file(self.bucket, self.key, str(target_path))
        except ClientError as e:
            raise ValueError(f"Failed to download from S3: {e}")

        file_size = target_path.stat().st_size
        logger.info(f"Downloaded file from S3: {target_path} ({file_size / 1024:.1f}KB)")
        return target_path
