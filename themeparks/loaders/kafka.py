"""
Kafka Loader

Loads DataFrames to Kafka topics.
"""

import os
import json
import logging
import pandas as pd
from themeparks.loaders.base import Loader

logger = logging.getLogger(__name__)


class KafkaLoader(Loader):
    """Loads data to Kafka topics."""
    
    def __init__(
        self,
        hostname: str | None = None,
        port: int | None = None,
        topic: str | None = None
    ):
        """
        Initialize Kafka loader.
        
        Args:
            hostname: Kafka broker hostname (defaults to KAFKA_HOST env var)
            port: Kafka broker port (defaults to KAFKA_PORT env var)
            topic: Kafka topic name (defaults to KAFKA_TOPIC env var)
        """
        self.hostname = hostname or os.environ.get("KAFKA_HOST", "localhost")
        self.port = port or int(os.environ.get("KAFKA_PORT", "9092"))
        self.topic = topic or os.environ.get("KAFKA_TOPIC", "themeparks.data")
        
    @property
    def name(self) -> str:
        return f"kafka://{self.hostname}:{self.port}/{self.topic}"
    
    def load(self, df: pd.DataFrame, topic: str | None = None, **kwargs) -> None:
        """
        Load DataFrame to Kafka topic.
        
        Args:
            df: DataFrame to load
            topic: Override topic name
            **kwargs: Additional parameters
        """
        topic = topic or self.topic
        
        logger.info(f"Loading {len(df)} records to Kafka topic: {topic}")
        
        try:
            from kafka import KafkaProducer
        except ImportError:
            logger.error("kafka-python not installed. Install with: pip install kafka-python")
            raise
        
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=f"{self.hostname}:{self.port}",
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send each row as a message
        for _, row in df.iterrows():
            message = row.to_dict()
            producer.send(topic, value=message)
        
        # Ensure all messages are sent
        producer.flush()
        producer.close()
        
        logger.info(f"Successfully loaded {len(df)} records to Kafka")


# Convenience function for CLI usage
def load_to_kafka(
    df: pd.DataFrame,
    hostname: str | None = None,
    port: int | None = None,
    topic: str | None = None,
    **kwargs
) -> None:
    """
    Load DataFrame to Kafka.
    
    Args:
        df: DataFrame to load
        hostname: Kafka hostname
        port: Kafka port
        topic: Kafka topic
        **kwargs: Additional parameters
    """
    loader = KafkaLoader(hostname=hostname, port=port, topic=topic)
    loader.load(df, **kwargs)
