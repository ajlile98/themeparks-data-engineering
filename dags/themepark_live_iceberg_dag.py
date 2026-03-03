"""
## Theme Park ETL — Live Data → Iceberg (silver)

Triggered by the raw_theme_park_live_data asset (every 5 minutes).
Reads the bronze JSONL file (path from XCom) and appends to the silver
Iceberg table via Nessie.

Bronze records from queue-times.com are already flat — no nested queue struct
to unpack. The silver schema maps directly from bronze fields:

  park_id        int   queue-times park ID
  land_id        int   land the ride belongs to (nullable)
  land_name      str   land name (nullable)
  id             int   ride ID
  name           str   ride name
  entityType     str   always "ATTRACTION" (queue-times only tracks rides)
  is_open        bool  whether the ride is currently open
  status         str   "OPERATING" or "CLOSED" (derived from is_open)
  wait_time        int            current standby wait in minutes
  last_updated     timestamp(UTC) when the source API last updated this ride
  ingest_timestamp timestamp(UTC) when this batch was ingested
"""

from airflow.sdk import Asset, asset


def _parse_ts(value: str | None):
    """Parse an ISO 8601 string (with Z or +00:00) to a timezone-aware datetime."""
    from datetime import datetime
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _normalize_record(record: dict) -> dict:
    """
    Promote bronze live-data records to silver with explicit type enforcement.

    - wait_time cast to int: prevents PyArrow inferring null() on all-None batches
      (which causes Iceberg schema conflicts on subsequent appends with real values).
    - last_updated / ingest_timestamp parsed to datetime: bronze stores these as
      raw strings; silver should hold proper timestamps. append_to_iceberg() will
      cast them to timestamp(us, UTC) via the timestamp_columns parameter.
    """
    return {
        "park_id":    record.get("park_id"),
        "land_id":    record.get("land_id"),
        "land_name":  record.get("land_name"),
        "id":         record.get("id"),
        "name":       record.get("name"),
        "entityType": record.get("entityType", "ATTRACTION"),
        "is_open":    record.get("is_open"),
        "status":     record.get("status"),
        "wait_time":  int(record["wait_time"]) if record.get("wait_time") is not None else None,
        "last_updated":     _parse_ts(record.get("last_updated")),
        "ingest_timestamp": _parse_ts(record.get("ingest_timestamp")),
    }


@asset(schedule=[Asset("raw_theme_park_live_data")])
def iceberg_live_data(context: dict) -> dict:
    """Normalize live data records and append to the Iceberg silver table."""
    from include.writers.bronze_writer import read_bronze
    from include.writers.iceberg_writer import get_catalog, append_to_iceberg

    path = context["ti"].xcom_pull(
        dag_id="raw_theme_park_live_data",
        task_ids="fetch_and_write",
        key="return_value",
        include_prior_dates=True,
    )
    if isinstance(path, list):
        path = path[-1]  # take the most-recent path

    raw_records = read_bronze(path)
    print(f"[live_data] read {len(raw_records)} records from {path}")

    records = [_normalize_record(r) for r in raw_records]

    catalog = get_catalog()
    # allow_schema_migration=True drops and recreates the table if the schema has
    # changed (schema evolves from the old queue-struct columns to the new flat shape).
    # timestamp_columns ensures last_updated and ingest_timestamp are stored as
    # proper Iceberg timestamps rather than strings after the NDJSON round-trip.
    result = append_to_iceberg(
        catalog,
        namespace="silver",
        table_name="live_data",
        records=records,
        allow_schema_migration=True,
        timestamp_columns=["last_updated", "ingest_timestamp"],
    )
    return result
