"""
## Theme Park ETL — Live Data → Iceberg (silver)

Triggered by the raw_theme_park_live_data asset (every 5 minutes).
Reads the bronze JSONL file (path from XCom), flattens the nested queue struct
into dedicated columns, and appends to the silver Iceberg table via Nessie.

Queue fields written (all nullable):
  queue_standby_wait              int   STANDBY.waitTime
  queue_single_rider_wait         int   SINGLE_RIDER.waitTime
  queue_paid_standby_wait         int   PAID_STANDBY.waitTime
  queue_return_time_state         str   RETURN_TIME.state
  queue_return_time_start         str   RETURN_TIME.returnStart  (kept as string)
  queue_return_time_end           str   RETURN_TIME.returnEnd    (kept as string)
  queue_boarding_group_wait       str   BOARDING_GROUP.estimatedWait
  queue_boarding_group_start      str   BOARDING_GROUP.currentGroupStart
  queue_boarding_group_end        str   BOARDING_GROUP.currentGroupEnd
  queue_boarding_group_status     str   BOARDING_GROUP.allocationStatus
  queue_boarding_group_next_alloc str   BOARDING_GROUP.nextAllocationTime
"""

from airflow.sdk import Asset, asset


def _flatten_queue(record: dict) -> dict:
    """
    Replace the nested 'queue' dict with individual typed columns.

    All time-like fields (returnStart, returnEnd) are kept as strings to prevent
    PyArrow from inferring pa.timestamp() on some batches and pa.string() on
    others — the mismatch that causes Iceberg schema conflicts on append.
    """
    q = record.get("queue") or {}
    standby       = q.get("STANDBY") or {}
    single        = q.get("SINGLE_RIDER") or {}
    paid          = q.get("PAID_STANDBY") or {}
    rt            = q.get("RETURN_TIME") or {}
    bg            = q.get("BOARDING_GROUP") or {}

    flat = {
        "park_id":     record.get("park_id"),
        "id":          record.get("id"),
        "name":        record.get("name"),
        "entityType":  record.get("entityType"),
        "status":      record.get("status"),
        "lastUpdated": record.get("lastUpdated"),
        "ingest_timestamp": record.get("ingest_timestamp"),
        # ── STANDBY / SINGLE_RIDER / PAID_STANDBY ────────────────────────────
        "queue_standby_wait":          standby.get("waitTime"),
        "queue_single_rider_wait":     single.get("waitTime"),
        "queue_paid_standby_wait":     paid.get("waitTime"),
        # ── RETURN_TIME ───────────────────────────────────────────────────────
        # Stored as strings to avoid PyArrow timestamp inference instability.
        "queue_return_time_state":     rt.get("state"),
        "queue_return_time_start":     str(rt["returnStart"]) if rt.get("returnStart") else None,
        "queue_return_time_end":       str(rt["returnEnd"])   if rt.get("returnEnd")   else None,
        # ── BOARDING_GROUP ────────────────────────────────────────────────────
        "queue_boarding_group_wait":         bg.get("estimatedWait"),
        "queue_boarding_group_start":        bg.get("currentGroupStart"),
        "queue_boarding_group_end":          bg.get("currentGroupEnd"),
        "queue_boarding_group_status":       bg.get("allocationStatus"),
        "queue_boarding_group_next_alloc":   bg.get("nextAllocationTime"),
    }
    return flat


@asset(schedule=[Asset("raw_theme_park_live_data")])
def iceberg_live_data(context: dict) -> dict:
    """Flatten live data queue struct and append to the Iceberg silver table."""
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

    records = [_flatten_queue(r) for r in raw_records]

    catalog = get_catalog()
    # allow_schema_migration=True drops and recreates the table if the schema has
    # changed (e.g. nested queue → flat columns) rather than raising a type error.
    result = append_to_iceberg(
        catalog,
        namespace="silver",
        table_name="live_data",
        records=records,
        allow_schema_migration=True,
    )
    return result
