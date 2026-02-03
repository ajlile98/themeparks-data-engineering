"""
Base Pipeline

Abstract base class for all ETL pipelines. Provides common structure
for extract, transform, load operations.
"""

from abc import ABC, abstractmethod
import asyncio
import logging
from datetime import datetime, timezone
from typing import List
import pandas as pd

from loaders import Loader

logger = logging.getLogger(__name__)


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
    
    def __init__(self, targets: List[Loader]):
        """
        Initialize pipeline with a load target.
        
        Args:
            target: Object with a load(data) method (e.g., CsvLoadTarget)
        """
        self.targets = targets
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
        Load data to target destinations.
        
        Args:
            df: Transformed DataFrame to load
        """
        for target in self.targets:
            target.load(df)
    
    def run(self) -> pd.DataFrame:
        """
        Execute the full ETL pipeline.
        
        Returns:
            The final transformed DataFrame
        """
        self._run_timestamp = datetime.now(timezone.utc)
        
        logger.info(f"[{self.name}] Starting pipeline at {self._run_timestamp.isoformat()}")
        
        # Extract
        logger.info(f"[{self.name}] Extracting data...")
        df = asyncio.run(self.extract())
        logger.info(f"[{self.name}] Extracted {len(df)} records")
        
        if df.empty:
            logger.warning(f"[{self.name}] No data extracted, skipping transform/load")
            return df
        
        # Transform
        logger.info(f"[{self.name}] Transforming data...")
        df = self.transform(df)
        logger.info(f"[{self.name}] Transformed {len(df)} records")
        
        # Load
        logger.info(f"[{self.name}] Loading data to {len(self.targets)} target(s)...")
        self.load(df)
        logger.info(f"[{self.name}] Loaded to {self.targets}")
        
        logger.info(f"[{self.name}] Pipeline complete")
        return df
    
    async def run_async(self) -> pd.DataFrame:
        """
        Execute pipeline asynchronously (for use within async context).
        
        Returns:
            The final transformed DataFrame
        """
        self._run_timestamp = datetime.now(timezone.utc)
        
        logger.info(f"[{self.name}] Starting pipeline at {self._run_timestamp.isoformat()}")
        
        logger.info(f"[{self.name}] Extracting data...")
        df = await self.extract()
        logger.info(f"[{self.name}] Extracted {len(df)} records")
        
        if df.empty:
            logger.warning(f"[{self.name}] No data extracted, skipping transform/load")
            return df
        
        logger.info(f"[{self.name}] Transforming data...")
        df = self.transform(df)
        logger.info(f"[{self.name}] Transformed {len(df)} records")
        
        logger.info(f"[{self.name}] Loading data to {len(self.targets)} target(s)...")
        self.load(df)
        logger.info(f"[{self.name}] Loaded to {self.targets}")
        
        logger.info(f"[{self.name}] Pipeline complete")
        return df
