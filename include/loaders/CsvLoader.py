
import csv
from typing import overload, override, Union
from pandas import DataFrame

from .Loader import Loader

class CsvLoader(Loader):

    def __init__(self, filename: str):
        self.filename = filename

    @overload
    def load(self, data: list[dict]) -> None: ...
    
    @overload
    def load(self, data: DataFrame) -> None: ...

    @override
    def load(self, data: Union[list[dict], DataFrame]) -> None:
        if isinstance(data, DataFrame):
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