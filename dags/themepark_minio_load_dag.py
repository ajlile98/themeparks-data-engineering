"""
## Theme Park ETL

This asset is triggered by upstream raw assets and loads their latest snapshots
to object storage using Airflow's ObjectStoragePath (decoupled from any specific backend).

Configure via environment variables:
    OBJECT_STORAGE_SYSTEM   - Storage scheme (default: "file"). Use "s3" for MinIO/S3, "gs" for GCS, etc.
    OBJECT_STORAGE_CONN_ID  - Airflow connection ID for the storage backend (default: None)
    OBJECT_STORAGE_PATH     - Base path for output files (default: "include/themeparks_data")
"""

import os
from datetime import datetime
from typing import Any

from airflow.sdk import Asset, asset, ObjectStoragePath, Variable

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH = os.getenv("OBJECT_STORAGE_PATH", default="include/themeparks_data")


def _max_ingest_timestamp(records: list[dict]) -> str | None:
    """Return max ingest_timestamp found in records, if present."""
    timestamps = [
        record.get("ingest_timestamp")
        for record in records
        if isinstance(record, dict) and record.get("ingest_timestamp")
    ]
    return max(timestamps) if timestamps else None


def _normalize_records(xcom_value: Any) -> list[dict]:
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


@asset(
    schedule=[
        Asset("raw_theme_park_destinations"),
        Asset("raw_theme_park_entities"),
        Asset("raw_theme_park_live_data"),
    ]
)
def load_themeparks_to_minio(context: dict) -> dict:
    """Load raw asset snapshots into object storage as partitioned Parquet files."""
    import io
    import pandas as pd

    ti = context["ti"]
    run_ts = datetime.now()
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str = run_ts.strftime("%Y-%m-%dT%H-%M-%S")

    destinations_raw = ti.xcom_pull(
        dag_id="raw_theme_park_destinations",
        task_ids="raw_theme_park_destinations",
        key="return_value",
        include_prior_dates=True,
    )
    entities_raw = ti.xcom_pull(
        dag_id="raw_theme_park_entities",
        task_ids="raw_theme_park_entities",
        key="return_value",
        include_prior_dates=True,
    )
    live_raw = ti.xcom_pull(
        dag_id="raw_theme_park_live_data",
        task_ids="raw_theme_park_live_data",
        key="return_value",
        include_prior_dates=True,
    )

    datasets = {
        "destinations": _normalize_records(destinations_raw),
        "entities": _normalize_records(entities_raw),
        "live": _normalize_records(live_raw),
    }

    load_summary: dict[str, Any] = {}

    for pipeline_name, records in datasets.items():
        if not records:
            load_summary[pipeline_name] = {
                "status": "skipped",
                "row_count": 0,
                "reason": "No records available from upstream assets",
            }
            continue

        latest_ingest_ts = _max_ingest_timestamp(records)
        watermark_key = f"themeparks_minio_last_ingest_ts__{pipeline_name}"
        previous_ingest_ts = Variable.get(watermark_key, default=None)

        if latest_ingest_ts and previous_ingest_ts == latest_ingest_ts:
            load_summary[pipeline_name] = {
                "status": "skipped",
                "row_count": len(records),
                "reason": "No new ingest_timestamp since last load",
                "latest_ingest_timestamp": latest_ingest_ts,
            }
            continue

        # Partitioned path: base/bronze/pipeline/YYYY-MM-DD/HH-MM-SS.parquet
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

        print(f"Wrote {len(df)} rows to {dest_path}")

        if latest_ingest_ts:
            Variable.set(watermark_key, latest_ingest_ts)

        load_summary[pipeline_name] = {
            "status": "loaded",
            "row_count": len(df),
            "path": str(dest_path),
            "columns": df.columns.tolist(),
            "latest_ingest_timestamp": latest_ingest_ts,
        }

    print(f"Load summary: {load_summary}")
    return load_summary
