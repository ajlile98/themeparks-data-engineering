

import json
from typing import Generator, override

from kafka import KafkaProducer
from pandas import DataFrame
from loaders.Loader import Loader

from pandas.core.common import standardize_mapping
from pandas.core.dtypes.cast import maybe_box_native


class KafkaLoader(Loader):
    """Load Data to Kafka topic for multiple consumer processing"""


    def __init__(self, hostname: str, port: int, topic: str):
        self.hostname = hostname
        self.port = port
        self.topic = topic
    
    @override
    def load(self, data: DataFrame) -> None:
        producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=f"{self.hostname}:{self.port}",
            key_serializer=lambda v: json.dumps(v).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for row in dataframe_records_gen(data):
            producer.send(topic=self.topic, value=row)
        producer.flush()


def dataframe_records_gen(df: DataFrame) -> Generator[dict, None, None]:
    columns = df.columns.tolist()
    into_c = standardize_mapping(dict)

    for row in df.itertuples(index=False, name=None):
        yield into_c(
            (k, maybe_box_native(v)) for k, v in dict(zip(columns, row)).items()
        )