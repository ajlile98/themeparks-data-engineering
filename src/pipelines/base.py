"""
Base Pipeline

Abstract base class for all ETL pipelines. Provides common structure
for extract, transform, load operations.
"""

from abc import ABC, abstractmethod
import asyncio
from datetime import datetime, timezone
from typing import Any
import pandas as pd


class BasePipeline(ABC):
    """
    Abstract base class for all data pipelines.
    
    Subclasses must implement:
        - name: Pipeline identifier for logging
        - extract(): Async method to fetch data from source
    
    Optionally override:
        - transform(): Modify extracted data
        - load(): Custom loading logic
    """
    
    def __init__(self, target: Any):
        """
        Initialize pipeline with a load target.
        
        Args:
            target: Object with a load(data) method (e.g., CsvLoadTarget)
        """
        self.target = target
        self._run_timestamp: datetime | None = None
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Pipeline name for logging and identification."""
        pass
    
    @property
    def run_timestamp(self) -> datetime:
        """Timestamp when the pipeline run started."""
        if self._run_timestamp is None:
            self._run_timestamp = datetime.now(timezone.utc)
        return self._run_timestamp
    
    @abstractmethod
    async def extract(self) -> pd.DataFrame:
        """
        Extract data from the source.
        
        Returns:
            DataFrame containing raw extracted data
        """
        pass
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform extracted data.
        
        Override this method to add custom transformations.
        Default implementation adds metadata columns.
        
        Args:
            df: Raw extracted DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Add pipeline metadata
        df["_pipeline"] = self.name
        df["_extracted_at"] = self.run_timestamp.isoformat()
        return df
    
    def load(self, df: pd.DataFrame) -> None:
        """
        Load data to target destination.
        
        Args:
            df: Transformed DataFrame to load
        """
        self.target.load(df)
    
    def run(self) -> pd.DataFrame:
        """
        Execute the full ETL pipeline.
        
        Returns:
            The final transformed DataFrame
        """
        self._run_timestamp = datetime.now(timezone.utc)
        
        print(f"[{self.name}] Starting pipeline...")
        
        # Extract
        df = asyncio.run(self.extract())
        print(f"[{self.name}] Extracted {len(df)} records")
        
        if df.empty:
            print(f"[{self.name}] No data extracted, skipping transform/load")
            return df
        
        # Transform
        df = self.transform(df)
        print(f"[{self.name}] Transformed {len(df)} records")
        
        # Load
        self.load(df)
        print(f"[{self.name}] Loaded to {self.target}")
        
        print(f"[{self.name}] Complete")
        return df
    
    async def run_async(self) -> pd.DataFrame:
        """
        Execute pipeline asynchronously (for use within async context).
        
        Returns:
            The final transformed DataFrame
        """
        self._run_timestamp = datetime.now(timezone.utc)
        
        print(f"[{self.name}] Starting pipeline...")
        
        df = await self.extract()
        print(f"[{self.name}] Extracted {len(df)} records")
        
        if df.empty:
            print(f"[{self.name}] No data extracted, skipping transform/load")
            return df
        
        df = self.transform(df)
        print(f"[{self.name}] Transformed {len(df)} records")
        
        self.load(df)
        print(f"[{self.name}] Loaded to {self.target}")
        
        print(f"[{self.name}] Complete")
        return df
