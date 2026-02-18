from typing import override
from pandas import DataFrame
from datetime import datetime

from loaders.Loader import Loader
from loaders.MinioStorage import MinioStorage


class MinioLoader(Loader):
    """Load data to MinIO as Parquet files."""

    def __init__(
        self,
        pipeline_name: str,
        stage: str = "bronze",
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None
    ):
        """
        Initialize MinIO loader.
        
        Args:
            pipeline_name: Name of pipeline (e.g., "destinations", "entities", "live")
            stage: Pipeline stage (e.g., "bronze", "silver", "gold")
            endpoint: MinIO endpoint. Defaults to MINIO_ENDPOINT env var or localhost:9000
            access_key: MinIO access key. Defaults to MINIO_ACCESS_KEY env var
            secret_key: MinIO secret key. Defaults to MINIO_SECRET_KEY env var
        """
        self.pipeline_name = pipeline_name
        self.stage = stage
        self.storage = MinioStorage(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key
        )

    @override
    def load(self, data: DataFrame) -> None:
        """
        Save DataFrame to MinIO as partitioned Parquet.
        
        Args:
            data: DataFrame to save
        """
        metadata = self.storage.save_dataframe(
            df=data,
            pipeline_name=self.pipeline_name,
            stage=self.stage,
            run_timestamp=datetime.utcnow()
        )
        print(f"✓ Loaded {metadata['row_count']} rows to MinIO: {metadata['path']}")
    
    def __repr__(self) -> str:
        return f"MinioLoader({self.pipeline_name}/{self.stage})"
