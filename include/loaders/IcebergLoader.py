"""
Iceberg Loader

Loads data to Apache Iceberg tables in MinIO object storage.
Provides ACID transactions, schema evolution, and time travel capabilities.
"""

import os
import logging
from typing import override
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame

from .Loader import Loader

logger = logging.getLogger(__name__)


class IcebergLoader(Loader):
    """
    Load data to Apache Iceberg tables in MinIO.
    
    Features:
    - ACID transactions (atomicity, consistency)
    - Schema evolution (add/rename columns without breaking)
    - Time travel (query historical versions)
    - Efficient upserts and deletes
    - Automatic file compaction
    
    Example:
        loader = IcebergLoader(
            table="live_data",
            mode="append",
            create_table_if_missing=True
        )
        loader.load(pandas_df)
    """
    
    # Class-level Spark session (shared across all instances)
    _spark_session: SparkSession | None = None
    
    def __init__(
        self,
        table: str,
        namespace: str = "default",
        mode: str = "append",
        create_table_if_missing: bool = True,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        warehouse_bucket: str = "themeparks-lakehouse"
    ):
        """
        Initialize Iceberg loader.
        
        Args:
            table: Table name (e.g., "live_data")
            namespace: Namespace/database name (default: "default")
            mode: Write mode - "append" or "overwrite"
            create_table_if_missing: Auto-create table if it doesn't exist
            minio_endpoint: MinIO endpoint (default: from env or localhost:9000)
            minio_access_key: MinIO access key (default: from env or minioadmin)
            minio_secret_key: MinIO secret key (default: from env or minioadmin)
            warehouse_bucket: S3 bucket for warehouse (default: themeparks-lakehouse)
        """
        super().__init__()
        self.table = table
        self.namespace = namespace
        self.mode = mode
        self.create_table_if_missing = create_table_if_missing
        
        # MinIO configuration
        self.minio_endpoint = minio_endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        self.minio_access_key = minio_access_key or os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = minio_secret_key or os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        self.warehouse_bucket = warehouse_bucket
        
        # Full table identifier
        self.full_table_name = f"local.{self.namespace}.{self.table}"
    
    def get_spark(self) -> SparkSession:
        """
        Get or create Spark session with Iceberg configuration.
        
        Reuses the same session across all IcebergLoader instances
        for efficiency (avoids JVM startup overhead).
        
        Returns:
            Configured SparkSession
        """
        if IcebergLoader._spark_session is None:
            logger.info("Creating new Spark session with Iceberg support...")
            
            IcebergLoader._spark_session = SparkSession.builder \
                .appName("ThemeParksIceberg") \
                .config("spark.jars.packages", 
                       "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                       "org.apache.hadoop:hadoop-aws:3.3.4,"
                       "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                .config("spark.sql.extensions", 
                       "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.spark_catalog", 
                       "org.apache.iceberg.spark.SparkSessionCatalog") \
                .config("spark.sql.catalog.local", 
                       "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.local.type", "hadoop") \
                .config("spark.sql.catalog.local.warehouse", 
                       f"s3a://{self.warehouse_bucket}/warehouse") \
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}") \
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            # Set log level to reduce noise
            IcebergLoader._spark_session.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session created successfully")
        
        return IcebergLoader._spark_session
    
    def _table_exists(self, spark: SparkSession) -> bool:
        """Check if table exists."""
        try:
            spark.sql(f"DESCRIBE TABLE {self.full_table_name}")
            return True
        except Exception:
            return False
    
    def _create_table_from_dataframe(self, spark: SparkSession, spark_df: SparkDataFrame) -> None:
        """Create Iceberg table from DataFrame schema."""
        logger.info(f"Creating Iceberg table: {self.full_table_name}")
        
        # Create table using DataFrame's schema
        spark_df.writeTo(self.full_table_name) \
            .using("iceberg") \
            .create()
        
        logger.info(f"✅ Created table: {self.full_table_name}")
    
    @override
    def load(self, data: pd.DataFrame) -> None:
        """
        Load pandas DataFrame to Iceberg table.
        
        Args:
            data: Pandas DataFrame to load
            
        Raises:
            ValueError: If data is empty or invalid
            Exception: If write operation fails
        """
        if data is None or len(data) == 0:
            logger.warning("Received empty DataFrame, skipping load")
            return
        
        logger.info(f"Loading {len(data)} rows to Iceberg table: {self.full_table_name}")
        
        try:
            # Get Spark session
            spark = self.get_spark()
            
            # Convert pandas to Spark DataFrame
            spark_df = spark.createDataFrame(data)
            
            # Check if table exists
            table_exists = self._table_exists(spark)
            
            if not table_exists:
                if self.create_table_if_missing:
                    self._create_table_from_dataframe(spark, spark_df)
                else:
                    raise ValueError(
                        f"Table {self.full_table_name} does not exist. "
                        f"Set create_table_if_missing=True to auto-create."
                    )
            
            # Write data based on mode
            if self.mode == "overwrite":
                logger.info(f"Overwriting table: {self.full_table_name}")
                spark_df.writeTo(self.full_table_name).overwritePartitions()
            elif self.mode == "append":
                logger.info(f"Appending to table: {self.full_table_name}")
                spark_df.writeTo(self.full_table_name).append()
            else:
                raise ValueError(f"Invalid mode: {self.mode}. Use 'append' or 'overwrite'")
            
            logger.info(f"✅ Successfully loaded {len(data)} rows to {self.full_table_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load data to Iceberg: {e}")
            raise
    
    def query(self, limit: int | None = None) -> pd.DataFrame:
        """
        Query data from the Iceberg table.
        
        Args:
            limit: Optional row limit
            
        Returns:
            Pandas DataFrame with query results
        """
        spark = self.get_spark()
        
        query = f"SELECT * FROM {self.full_table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        spark_df = spark.sql(query)
        return spark_df.toPandas()
    
    def get_snapshots(self) -> pd.DataFrame:
        """
        Get all snapshots (versions) of the table for time travel.
        
        Returns:
            DataFrame with snapshot metadata
        """
        spark = self.get_spark()
        snapshots_df = spark.sql(f"SELECT * FROM {self.full_table_name}.snapshots")
        return snapshots_df.toPandas()
    
    @classmethod
    def stop_spark(cls):
        """Stop the shared Spark session (cleanup)."""
        if cls._spark_session is not None:
            logger.info("Stopping Spark session...")
            cls._spark_session.stop()
            cls._spark_session = None
            logger.info("✅ Spark session stopped")
    
    def __repr__(self) -> str:
        return (f"IcebergLoader(table={self.full_table_name}, "
                f"mode={self.mode}, "
                f"warehouse=s3a://{self.warehouse_bucket}/warehouse)")
    
    def __del__(self):
        """Cleanup - note: Spark session is shared, so not stopped here."""
        pass
