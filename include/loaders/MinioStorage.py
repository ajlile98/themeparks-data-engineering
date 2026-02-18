"""
MinIO Storage Helper

Provides utilities for saving and loading DataFrames to/from MinIO
for intermediate storage between Airflow tasks.
"""

import os
import logging
from datetime import datetime
from io import BytesIO
from typing import Dict, Any
import pandas as pd
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinioStorage:
    """
    Helper class for storing intermediate pipeline data in MinIO.
    
    Follows the pattern:
    - Extract task saves DataFrame to MinIO, returns metadata via XCom
    - Transform/Load tasks retrieve DataFrame from MinIO using metadata
    """
    
    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = False,
        bucket: str = "themeparks-pipeline"
    ):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO endpoint (e.g., "localhost:9000"). Defaults to MINIO_ENDPOINT env var.
            access_key: MinIO access key. Defaults to MINIO_ACCESS_KEY env var.
            secret_key: MinIO secret key. Defaults to MINIO_SECRET_KEY env var.
            secure: Use HTTPS. Defaults to False for local dev.
            bucket: S3 bucket name for pipeline data.
        """
        self.endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = bucket
        
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure
        )
        
        # Ensure bucket exists
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created MinIO bucket: {self.bucket}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise
    
    def save_dataframe(
        self,
        df: pd.DataFrame,
        pipeline_name: str,
        stage: str,
        run_timestamp: datetime | None = None
    ) -> Dict[str, Any]:
        """
        Save DataFrame to MinIO and return metadata.
        
        Args:
            df: DataFrame to save
            pipeline_name: Name of pipeline (e.g., "destinations", "entities")
            stage: Pipeline stage (e.g., "raw", "transformed")
            run_timestamp: Timestamp for this run. Defaults to now.
            
        Returns:
            Metadata dict suitable for XCom:
            {
                "path": "s3://bucket/path/to/file.parquet",
                "row_count": 1234,
                "timestamp": "2026-02-03T14:30:00Z",
                "columns": ["col1", "col2", ...],
                "size_bytes": 12345
            }
        """
        if run_timestamp is None:
            run_timestamp = datetime.utcnow()
        
        # Generate object path: pipeline/stage/YYYY-MM-DD/HH-MM-SS.parquet
        timestamp_str = run_timestamp.strftime("%Y-%m-%dT%H-%M-%S")
        date_str = run_timestamp.strftime("%Y-%m-%d")
        object_name = f"{pipeline_name}/{stage}/{date_str}/{timestamp_str}.parquet"
        
        # Convert DataFrame to parquet bytes
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
        parquet_bytes = parquet_buffer.getvalue()
        size_bytes = len(parquet_bytes)
        
        # Upload to MinIO
        try:
            parquet_buffer.seek(0)
            self.client.put_object(
                self.bucket,
                object_name,
                BytesIO(parquet_bytes),
                length=size_bytes,
                content_type='application/octet-stream'
            )
            logger.info(f"Saved {len(df)} rows to MinIO: {object_name} ({size_bytes:,} bytes)")
        except S3Error as e:
            logger.error(f"Error saving to MinIO: {e}")
            raise
        
        # Return metadata for XCom
        return {
            "path": f"s3://{self.bucket}/{object_name}",
            "object_name": object_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
            "timestamp": run_timestamp.isoformat(),
            "size_bytes": size_bytes
        }
    
    def load_dataframe(self, metadata: Dict[str, Any]) -> pd.DataFrame:
        """
        Load DataFrame from MinIO using metadata from XCom.
        
        Args:
            metadata: Metadata dict returned by save_dataframe()
            
        Returns:
            DataFrame loaded from MinIO
        """
        object_name = metadata.get("object_name")
        if not object_name:
            # Try parsing from path
            path = metadata.get("path", "")
            if path.startswith(f"s3://{self.bucket}/"):
                object_name = path.replace(f"s3://{self.bucket}/", "")
            else:
                raise ValueError(f"Invalid metadata, missing 'object_name' or valid 'path': {metadata}")
        
        try:
            # Download from MinIO
            response = self.client.get_object(self.bucket, object_name)
            parquet_bytes = response.read()
            response.close()
            response.release_conn()
            
            # Load DataFrame
            df = pd.read_parquet(BytesIO(parquet_bytes), engine='pyarrow')
            logger.info(f"Loaded {len(df)} rows from MinIO: {object_name}")
            return df
            
        except S3Error as e:
            logger.error(f"Error loading from MinIO: {e}")
            raise
    
    def delete_object(self, metadata: Dict[str, Any]) -> None:
        """
        Delete object from MinIO (for cleanup).
        
        Args:
            metadata: Metadata dict with object_name or path
        """
        object_name = metadata.get("object_name")
        if not object_name:
            path = metadata.get("path", "")
            if path.startswith(f"s3://{self.bucket}/"):
                object_name = path.replace(f"s3://{self.bucket}/", "")
        
        if object_name:
            try:
                self.client.remove_object(self.bucket, object_name)
                logger.info(f"Deleted from MinIO: {object_name}")
            except S3Error as e:
                logger.warning(f"Error deleting from MinIO: {e}")


# Convenience functions for use in Airflow tasks
def save_to_minio(
    df: pd.DataFrame,
    pipeline_name: str,
    stage: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Convenience function for Airflow tasks to save DataFrame.
    
    Returns metadata dict for XCom.
    """
    storage = MinioStorage()
    return storage.save_dataframe(df, pipeline_name, stage)


def load_from_minio(metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Convenience function for Airflow tasks to load DataFrame.
    
    Args:
        metadata: XCom data from previous task
    """
    storage = MinioStorage()
    return storage.load_dataframe(metadata)
