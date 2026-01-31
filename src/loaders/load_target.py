

from typing import Union, overload, override
import csv

import pandas as pd


class LoadTarget:

    def __init__(self):
        pass

    @overload
    def load(self, data: list[dict]) -> None: ...
    
    @overload
    def load(self, data: pd.DataFrame) -> None: ...
    
    def load(self, data: Union[list[dict], pd.DataFrame]) -> None:
        pass

class CsvLoadTarget(LoadTarget):

    def __init__(self, filename: str):
        self.filename = filename

    @overload
    def load(self, data: list[dict]) -> None: ...
    
    @overload
    def load(self, data: pd.DataFrame) -> None: ...

    @override
    def load(self, data: Union[list[dict], pd.DataFrame]) -> None:
        if isinstance(data, pd.DataFrame):
            data.to_csv(self.filename, index=False)
            return
        
        # Handle list[dict]
        if not data:
            return
        
        # Collect all possible field names from all records
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
    
        with open(self.filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(all_fields), extrasaction='ignore')
            writer.writeheader()
            for record in data:
                writer.writerow(record)
    
    def __repr__(self) -> str:
        return f"CsvLoadTarget({self.filename})"


class ParquetLoadTarget(LoadTarget):
    """Load data to Parquet format with optional partitioning."""

    def __init__(self, path: str, partition_cols: list[str] | None = None):
        self.path = path 
        self.partition_cols = partition_cols

    @override
    def load(self, data: pd.DataFrame) -> None:
        if self.partition_cols:
            data.to_parquet(self.path, partition_cols=self.partition_cols, index=False)
        else:
            data.to_parquet(self.path, index=False)
    
    def __repr__(self) -> str:
        return f"ParquetLoadTarget({self.path})"