"""
Bronze layer writer.

Writes raw records as gzip-compressed JSONL to MinIO and returns the object storage path string.
The path string is tiny (~80 bytes) and safe to pass through XCom with no backend needed.

Path format: s3://minio_default@airflow-data/bronze/{prefix}/{YYYY/MM/DD/HHMMSS}.jsonl.gz

Compression: gzip (stdlib, no extra dependencies). Typical JSONL compresses 5-10x.
read_bronze() also handles legacy uncompressed .jsonl files transparently.
"""

import gzip
import json
from datetime import datetime, timezone


def write_bronze(
    records: list[dict],
    prefix: str,
    conn_id: str = "minio_default",
    bucket: str = "airflow-data",
) -> str:
    """
    Serialize records as gzip-compressed JSONL and write to the MinIO bronze layer.

    Returns the full ObjectStoragePath string so downstream tasks can
    read the file without passing the records themselves through XCom.
    """
    from airflow.sdk import ObjectStoragePath

    ts = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
    path = ObjectStoragePath(f"s3://{conn_id}@{bucket}/bronze/{prefix}/{ts}.jsonl.gz")

    content = "\n".join(json.dumps(record, default=str) for record in records)
    path.write_bytes(gzip.compress(content.encode("utf-8")))

    print(f"[bronze] wrote {len(records)} records → {path}")
    return str(path)


def read_bronze(path_str: str) -> list[dict]:
    """
    Read a bronze file from object storage and return a list of dicts.

    Accepts the path string returned by write_bronze(), e.g.:
        s3://minio_default@airflow-data/bronze/destinations/2026/02/19/120000.jsonl.gz

    Handles both compressed (.jsonl.gz) and legacy uncompressed (.jsonl) files transparently.
    """
    from airflow.sdk import ObjectStoragePath

    if not isinstance(path_str, str):
        raise TypeError(
            f"read_bronze expected a path string but got {type(path_str).__name__}: {path_str!r}. "
            "This usually means xcom_pull returned a stale list[dict] from before the bronze refactor. "
            "Clear the old XCom values from the Airflow UI (Admin → XCom) and re-run the extractor DAG."
        )

    path = ObjectStoragePath(path_str)
    raw = path.read_bytes()
    content = gzip.decompress(raw).decode("utf-8") if path_str.endswith(".gz") else raw.decode("utf-8")
    return [json.loads(line) for line in content.splitlines() if line.strip()]
