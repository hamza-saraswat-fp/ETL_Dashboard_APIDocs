"""
Storage Service - Abstracts file storage for local and cloud (Supabase)

Automatically uses Supabase Storage when SUPABASE_URL and SUPABASE_KEY are set,
otherwise falls back to local filesystem storage.
"""
import logging
import shutil
from pathlib import Path
from typing import Optional, BinaryIO, Union
import json

from ..config import settings

logger = logging.getLogger(__name__)

# Lazy import supabase to avoid import errors when not installed
_supabase_client = None


def _get_supabase_client():
    """Get or create Supabase client (lazy initialization)"""
    global _supabase_client
    if _supabase_client is None and settings.use_cloud_storage:
        try:
            from supabase import create_client
            _supabase_client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
            logger.info("Supabase client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            return None
    return _supabase_client


class StorageService:
    """
    Unified storage service supporting local filesystem and Supabase Storage.

    Usage:
        storage = StorageService()

        # Upload a file
        storage.upload_file(job_id, "bronze", "data.json", file_content)

        # Download a file
        content = storage.download_file(job_id, "bronze", "data.json")

        # Delete job artifacts
        storage.delete_job(job_id)
    """

    def __init__(self):
        self.use_cloud = settings.use_cloud_storage
        self.bucket = settings.SUPABASE_BUCKET
        self.local_base = Path(settings.JOBS_DIR)

        if self.use_cloud:
            logger.info(f"Storage: Using Supabase Storage (bucket: {self.bucket})")
        else:
            logger.info(f"Storage: Using local filesystem ({self.local_base})")

    def _get_cloud_path(self, job_id: str, stage: str, filename: str) -> str:
        """Get the cloud storage path for a file"""
        return f"{job_id}/{stage}/{filename}"

    def _get_local_path(self, job_id: str, stage: str, filename: str) -> Path:
        """Get the local filesystem path for a file"""
        return self.local_base / job_id / stage / filename

    def upload_file(
        self,
        job_id: str,
        stage: str,
        filename: str,
        content: Union[bytes, str, BinaryIO],
        content_type: str = "application/octet-stream"
    ) -> bool:
        """
        Upload a file to storage.

        Args:
            job_id: The job ID
            stage: The stage (input, bronze, silver, gold, logs)
            filename: The filename
            content: File content (bytes, string, or file-like object)
            content_type: MIME type of the content

        Returns:
            True if successful, False otherwise
        """
        if self.use_cloud:
            return self._upload_to_cloud(job_id, stage, filename, content, content_type)
        else:
            return self._upload_to_local(job_id, stage, filename, content)

    def _upload_to_cloud(
        self,
        job_id: str,
        stage: str,
        filename: str,
        content: Union[bytes, str, BinaryIO],
        content_type: str
    ) -> bool:
        """Upload to Supabase Storage"""
        try:
            client = _get_supabase_client()
            if not client:
                logger.error("Supabase client not available")
                return False

            path = self._get_cloud_path(job_id, stage, filename)

            # Convert content to bytes
            if isinstance(content, str):
                content = content.encode('utf-8')
            elif hasattr(content, 'read'):
                content = content.read()

            # Upload to Supabase
            result = client.storage.from_(self.bucket).upload(
                path=path,
                file=content,
                file_options={"content-type": content_type}
            )

            logger.debug(f"Uploaded to cloud: {path}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload to cloud storage: {e}")
            return False

    def _upload_to_local(
        self,
        job_id: str,
        stage: str,
        filename: str,
        content: Union[bytes, str, BinaryIO]
    ) -> bool:
        """Upload to local filesystem"""
        try:
            path = self._get_local_path(job_id, stage, filename)
            path.parent.mkdir(parents=True, exist_ok=True)

            mode = 'wb' if isinstance(content, bytes) else 'w'

            if hasattr(content, 'read'):
                # File-like object
                with open(path, 'wb') as f:
                    shutil.copyfileobj(content, f)
            else:
                with open(path, mode) as f:
                    f.write(content)

            logger.debug(f"Saved to local: {path}")
            return True

        except Exception as e:
            logger.error(f"Failed to save to local storage: {e}")
            return False

    def download_file(
        self,
        job_id: str,
        stage: str,
        filename: str
    ) -> Optional[bytes]:
        """
        Download a file from storage.

        Returns:
            File content as bytes, or None if not found
        """
        if self.use_cloud:
            return self._download_from_cloud(job_id, stage, filename)
        else:
            return self._download_from_local(job_id, stage, filename)

    def _download_from_cloud(
        self,
        job_id: str,
        stage: str,
        filename: str
    ) -> Optional[bytes]:
        """Download from Supabase Storage"""
        try:
            client = _get_supabase_client()
            if not client:
                return None

            path = self._get_cloud_path(job_id, stage, filename)
            result = client.storage.from_(self.bucket).download(path)
            return result

        except Exception as e:
            logger.error(f"Failed to download from cloud: {e}")
            return None

    def _download_from_local(
        self,
        job_id: str,
        stage: str,
        filename: str
    ) -> Optional[bytes]:
        """Download from local filesystem"""
        try:
            path = self._get_local_path(job_id, stage, filename)
            if not path.exists():
                return None
            return path.read_bytes()

        except Exception as e:
            logger.error(f"Failed to read from local: {e}")
            return None

    def get_download_url(
        self,
        job_id: str,
        stage: str,
        filename: str,
        expires_in: int = 3600
    ) -> Optional[str]:
        """
        Get a signed download URL for a file (cloud storage only).

        Returns:
            Signed URL string, or None if not using cloud storage
        """
        if not self.use_cloud:
            return None

        try:
            client = _get_supabase_client()
            if not client:
                return None

            path = self._get_cloud_path(job_id, stage, filename)
            result = client.storage.from_(self.bucket).create_signed_url(
                path=path,
                expires_in=expires_in
            )
            return result.get('signedURL')

        except Exception as e:
            logger.error(f"Failed to create signed URL: {e}")
            return None

    def file_exists(self, job_id: str, stage: str, filename: str) -> bool:
        """Check if a file exists in storage"""
        if self.use_cloud:
            # For cloud, try to get file info
            try:
                client = _get_supabase_client()
                if not client:
                    return False
                path = self._get_cloud_path(job_id, stage, filename)
                # Try to download metadata (small operation)
                client.storage.from_(self.bucket).download(path)
                return True
            except:
                return False
        else:
            path = self._get_local_path(job_id, stage, filename)
            return path.exists()

    def delete_job(self, job_id: str) -> bool:
        """
        Delete all artifacts for a job.

        Returns:
            True if successful, False otherwise
        """
        if self.use_cloud:
            return self._delete_job_cloud(job_id)
        else:
            return self._delete_job_local(job_id)

    def _delete_job_cloud(self, job_id: str) -> bool:
        """Delete job artifacts from Supabase Storage"""
        try:
            client = _get_supabase_client()
            if not client:
                return False

            # List all files for this job
            files = client.storage.from_(self.bucket).list(path=job_id)

            # Delete each stage folder
            for stage in ['input', 'bronze', 'silver', 'gold', 'logs']:
                try:
                    stage_files = client.storage.from_(self.bucket).list(path=f"{job_id}/{stage}")
                    if stage_files:
                        paths = [f"{job_id}/{stage}/{f['name']}" for f in stage_files]
                        if paths:
                            client.storage.from_(self.bucket).remove(paths)
                except:
                    pass

            logger.info(f"Deleted cloud artifacts for job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete cloud artifacts: {e}")
            return False

    def _delete_job_local(self, job_id: str) -> bool:
        """Delete job artifacts from local filesystem"""
        try:
            job_dir = self.local_base / job_id
            if job_dir.exists():
                shutil.rmtree(job_dir)
                logger.info(f"Deleted local artifacts for job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete local artifacts: {e}")
            return False

    def list_files(self, job_id: str, stage: str) -> list:
        """List files in a job's stage directory"""
        if self.use_cloud:
            try:
                client = _get_supabase_client()
                if not client:
                    return []
                files = client.storage.from_(self.bucket).list(path=f"{job_id}/{stage}")
                return [f['name'] for f in files] if files else []
            except:
                return []
        else:
            path = self.local_base / job_id / stage
            if not path.exists():
                return []
            return [f.name for f in path.iterdir() if f.is_file()]


# Global storage service instance
_storage_service = None


def get_storage_service() -> StorageService:
    """Get the global storage service instance"""
    global _storage_service
    if _storage_service is None:
        _storage_service = StorageService()
    return _storage_service
