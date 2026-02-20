"""
Shared helper for writing records to object storage as partitioned Parquet files.

Output path pattern:
    {OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH}/bronze/{pipeline_name}/{YYYY-MM-DD}/{HH-MM-SS}.parquet

Configure via environment variables:
    OBJECT_STORAGE_SYSTEM   - Storage scheme (default: "file"). Use "s3" for MinIO/S3.
    OBJECT_STORAGE_CONN_ID  - Airflow connection ID for the storage backend.
    OBJECT_STORAGE_PATH     - Base bucket/prefix (default: "include/themeparks_data").
"""

import io
import os
from datetime import datetime
from typing import Any


OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH = os.getenv("OBJECT_STORAGE_PATH", default="include/themeparks_data")


def normalize_records(xcom_value: Any) -> list[dict]:
    """Normalize possible XCom payload shapes to list[dict]."""
    if xcom_value is None:
        return []
    if isinstance(xcom_value, dict):
        return [row for row in xcom_value.get("destinations", []) if isinstance(row, dict)]
    if isinstance(xcom_value, list):
        if xcom_value and isinstance(xcom_value[0], list):
            xcom_value = xcom_value[0]
        return [row for row in xcom_value if isinstance(row, dict)]
    return []


def write_parquet(records: list[dict], pipeline_name: str) -> dict:
    """Serialize records to Snappy-compressed Parquet and write to object storage.

    Args:
        records: List of dicts to write.
        pipeline_name: Dataset name used as the partition directory (e.g. "destinations").

    Returns:
        Summary dict with status, row_count, and path.
    """
    import pandas as pd
    from airflow.sdk import ObjectStoragePath

    if not records:
        return {
            "status": "skipped",
            "row_count": 0,
            "reason": "No records provided",
        }

    run_ts = datetime.now()
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str = run_ts.strftime("%Y-%m-%dT%H-%M-%S")

    dest_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH}/bronze/{pipeline_name}/{date_str}/{ts_str}.parquet",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )

    df = pd.DataFrame(records)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    buf.seek(0)

    with dest_path.open("wb") as f:
        f.write(buf.read())

    print(f"[parquet_writer] Wrote {len(df)} rows → {dest_path}")

    return {
        "status": "loaded",
        "row_count": len(df),
        "path": str(dest_path),
        "columns": df.columns.tolist(),
    }
