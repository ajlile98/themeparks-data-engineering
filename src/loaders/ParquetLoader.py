
from typing import override
from pandas import DataFrame

from loaders.Loader import Loader

class ParquetLoader(Loader):
    """Load data to Parquet format with optional partitioning."""

    def __init__(self, path: str, partition_cols: list[str] | None = None):
        self.path = path 
        self.partition_cols = partition_cols

    @override
    def load(self, data: DataFrame) -> None:
        if self.partition_cols:
            data.to_parquet(self.path, partition_cols=self.partition_cols, index=False)
        else:
            data.to_parquet(self.path, index=False)
    
    def __repr__(self) -> str:
        return f"ParquetLoadTarget({self.path})"