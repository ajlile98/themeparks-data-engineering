"""
## Theme Park ETL — Iceberg Maintenance

Runs daily at 03:00 UTC. Expires old Iceberg snapshots and orphaned data files
across all silver and gold tables to prevent unbounded metadata growth.

Uses **dynamic task mapping** — one `expire_snapshots` task instance per entry
in RETENTION_POLICY — so a transient failure on one table retries independently
without blocking the others.

Retention policy:
  Table                     Snapshots retained   Min kept
  ─────────────────────────────────────────────────────────
  silver.live_data          30 days              5
  gold.live_data_current    7 days               2
  silver.destinations       7 days               1
  silver.parks              7 days               1
  silver.entities           7 days               1
"""

from datetime import timedelta
from airflow.sdk import dag, task

# Each dict becomes one mapped task instance.
RETENTION_POLICY = [
    {"table": "silver.live_data",       "retain_days": 30, "min_snapshots": 5},
    {"table": "gold.live_data_current", "retain_days": 7,  "min_snapshots": 2},
    {"table": "silver.destinations",    "retain_days": 7,  "min_snapshots": 1},
    {"table": "silver.parks",           "retain_days": 7,  "min_snapshots": 1},
    {"table": "silver.entities",        "retain_days": 7,  "min_snapshots": 1},
]


@dag(
    schedule="0 3 * * *",  # 03:00 UTC daily — quiet period between live data runs
    catchup=False,
    tags=["themeparks", "maintenance"],
)
def iceberg_maintenance():
    """Expire old Iceberg snapshots — one mapped task per table."""

    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )
    def expire_snapshots(policy: dict) -> dict:
        """Expire snapshots for one Iceberg table per the retention policy."""
        from datetime import datetime, timezone, timedelta
        from include.writers.iceberg_writer import get_catalog

        full_name = policy["table"]
        retain_days = policy["retain_days"]
        min_snapshots = policy["min_snapshots"]

        catalog = get_catalog()

        if not catalog.table_exists(full_name):
            print(f"[maintenance] {full_name} does not exist — skipping")
            return {"table": full_name, "status": "skipped"}

        now = datetime.now(timezone.utc)
        cutoff_ms = int((now - timedelta(days=retain_days)).timestamp() * 1000)
        tbl = catalog.load_table(full_name)
        snapshots_before = len(tbl.metadata.snapshots)

        (
            tbl.expire_snapshots()
            .expire_older_than(cutoff_ms)
            .min_snapshots_to_keep(min_snapshots)
            .commit()
        )

        tbl = catalog.load_table(full_name)
        snapshots_after = len(tbl.metadata.snapshots)
        expired = snapshots_before - snapshots_after

        print(
            f"[maintenance] {full_name}: expired {expired} snapshot(s), "
            f"{snapshots_after} remaining "
            f"(policy: >{retain_days}d, min={min_snapshots})"
        )
        return {
            "table": full_name,
            "snapshots_before": snapshots_before,
            "snapshots_after": snapshots_after,
            "expired": expired,
            "status": "ok",
        }

    expire_snapshots.expand(policy=RETENTION_POLICY)


iceberg_maintenance()
