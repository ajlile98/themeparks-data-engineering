"""Loaders Module - Data loading to various targets."""

from themeparks.loaders.base import Loader
from themeparks.loaders.kafka import KafkaLoader, load_to_kafka
from themeparks.loaders.storage import (
    StorageManager,
    get_storage_manager,
    save_dataframe,
    load_dataframe,
)

__all__ = [
    "Loader",
    "KafkaLoader",
    "load_to_kafka",
    "StorageManager",
    "get_storage_manager",
    "save_dataframe",
    "load_dataframe",
]
