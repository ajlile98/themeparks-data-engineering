"""
Storage Module

Handles saving/loading DataFrames to/from MinIO and local storage.
Used for intermediate data between pipeline stages.
"""

import os
import logging
from datetime import datetime
from io import BytesIO
from typing import Dict, Any
from urllib.parse import urlparse
import pandas as pd

logger = logging.getLogger(__name__)


class StorageManager:
    """
    Manages DataFrame storage to various backends (MinIO, local files).
    
    Supports URIs like:
    - minio://bucket/path/to/file.parquet
    - file:///local/path/to/file.parquet
    - /local/path/to/file.parquet (no scheme = local)
    """
    
    def __init__(self):
        """Initialize storage manager with lazy loading of clients."""
        self._minio_client = None
    
    @property
    def minio_client(self):
        """Lazy load MinIO client."""
        if self._minio_client is None:
            try:
                from minio import Minio
                endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
                access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
                secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
                secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
                
                self._minio_client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure
                )
                logger.info(f"Connected to MinIO at {endpoint}")
            except ImportError:
                logger.error("minio package not installed. Install with: pip install minio")
                raise
        
        return self._minio_client
    
    def save(self, df: pd.DataFrame, uri: str, **kwargs) -> Dict[str, Any]:
        """
        Save DataFrame to URI.
        
        Args:
            df: DataFrame to save
            uri: Destination URI (minio://..., file://..., or local path)
            **kwargs: Additional save parameters
            
        Returns:
            Metadata dict with path, row_count, size_bytes, etc.
        """
        parsed = urlparse(uri)
        
        if parsed.scheme == "minio" or parsed.scheme == "s3":
            return self._save_to_minio(df, parsed, **kwargs)
        else:
            # Local file (file:// or no scheme)
            path = parsed.path if parsed.scheme == "file" else uri
            return self._save_to_local(df, path, **kwargs)
    
    def load(self, uri: str, **kwargs) -> pd.DataFrame:
        """
        Load DataFrame from URI.
        
        Args:
            uri: Source URI (minio://..., file://..., or local path)
            **kwargs: Additional load parameters
            
        Returns:
            DataFrame
        """
        parsed = urlparse(uri)
        
        if parsed.scheme == "minio" or parsed.scheme == "s3":
            return self._load_from_minio(parsed, **kwargs)
        else:
            # Local file (file:// or no scheme)
            path = parsed.path if parsed.scheme == "file" else uri
            return self._load_from_local(path, **kwargs)
    
    def _save_to_minio(self, df: pd.DataFrame, parsed_uri, **kwargs) -> Dict[str, Any]:
        """Save DataFrame to MinIO."""
        bucket = parsed_uri.netloc
        object_name = parsed_uri.path.lstrip("/")
        
        # Ensure bucket exists
        if not self.minio_client.bucket_exists(bucket):
            self.minio_client.make_bucket(bucket)
            logger.info(f"Created MinIO bucket: {bucket}")
        
        # Convert DataFrame to parquet bytes
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
        parquet_bytes = parquet_buffer.getvalue()
        size_bytes = len(parquet_bytes)
        
        # Upload to MinIO
        parquet_buffer.seek(0)
        self.minio_client.put_object(
            bucket,
            object_name,
            BytesIO(parquet_bytes),
            length=size_bytes,
            content_type='application/octet-stream'
        )
        
        logger.info(f"Saved {len(df)} rows to MinIO: minio://{bucket}/{object_name} ({size_bytes:,} bytes)")
        
        return {
            "uri": f"minio://{bucket}/{object_name}",
            "bucket": bucket,
            "object_name": object_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
            "size_bytes": size_bytes,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    def _load_from_minio(self, parsed_uri, **kwargs) -> pd.DataFrame:
        """Load DataFrame from MinIO."""
        bucket = parsed_uri.netloc
        object_name = parsed_uri.path.lstrip("/")
        
        # Download from MinIO
        response = self.minio_client.get_object(bucket, object_name)
        parquet_bytes = response.read()
        response.close()
        response.release_conn()
        
        # Load DataFrame
        df = pd.read_parquet(BytesIO(parquet_bytes), engine='pyarrow')
        logger.info(f"Loaded {len(df)} rows from MinIO: minio://{bucket}/{object_name}")
        return df
    
    def _save_to_local(self, df: pd.DataFrame, path: str, **kwargs) -> Dict[str, Any]:
        """Save DataFrame to local file."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
        # Save as parquet
        df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
        size_bytes = os.path.getsize(path)
        
        logger.info(f"Saved {len(df)} rows to local file: {path} ({size_bytes:,} bytes)")
        
        return {
            "uri": f"file://{os.path.abspath(path)}",
            "path": path,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
            "size_bytes": size_bytes,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    def _load_from_local(self, path: str, **kwargs) -> pd.DataFrame:
        """Load DataFrame from local file."""
        df = pd.read_parquet(path, engine='pyarrow')
        logger.info(f"Loaded {len(df)} rows from local file: {path}")
        return df


# Singleton instance
_storage_manager = None


def get_storage_manager() -> StorageManager:
    """Get global storage manager instance."""
    global _storage_manager
    if _storage_manager is None:
        _storage_manager = StorageManager()
    return _storage_manager


# Convenience functions
def save_dataframe(df: pd.DataFrame, uri: str, **kwargs) -> Dict[str, Any]:
    """Save DataFrame to URI."""
    return get_storage_manager().save(df, uri, **kwargs)


def load_dataframe(uri: str, **kwargs) -> pd.DataFrame:
    """Load DataFrame from URI."""
    return get_storage_manager().load(uri, **kwargs)
