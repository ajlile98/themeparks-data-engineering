"""
## Theme Park ETL — Iceberg Maintenance

Runs daily. Expires old Iceberg snapshots across all silver and gold tables
to prevent unbounded metadata/data file growth.

Iceberg snapshot expiration works in two phases:
  1. Mark snapshots older than the cutoff as expired (soft delete in metadata)
  2. Delete the orphaned data files that are no longer referenced by any
     retained snapshot

This is the safe alternative to S3 lifecycle rules on the iceberg/ prefix —
lifecycle rules can delete Parquet files still referenced by a valid snapshot,
corrupting the table.

Retention policy:
  Table                     Snapshots retained   Min kept
  ─────────────────────────────────────────────────────────
  silver.live_data          30 days              5
  gold.live_data_current    7 days               2
  silver.destinations       7 days               1
  silver.parks              7 days               1
  silver.entities           7 days               1
"""

from datetime import datetime, timezone, timedelta
from airflow.sdk import asset

# Tables that accumulate snapshots and need periodic expiry.
# Reference tables (destinations, parks, entities) are overwritten each run
# so they rarely have more than 2 snapshots — still worth pruning.
RETENTION_POLICY = [
    # (namespace.table,            retain_days, min_snapshots)
    ("silver.live_data",           30,          5),
    ("gold.live_data_current",     7,           2),
    ("silver.destinations",        7,           1),
    ("silver.parks",               7,           1),
    ("silver.entities",            7,           1),
]


@asset(schedule="0 3 * * *")  # 03:00 UTC daily — quiet period between live data runs
def iceberg_maintenance() -> dict:
    """Expire old Iceberg snapshots and delete orphaned data files."""
    from include.writers.iceberg_writer import get_catalog

    catalog = get_catalog()
    now = datetime.now(timezone.utc)
    results = []

    for full_name, retain_days, min_snapshots in RETENTION_POLICY:
        if not catalog.table_exists(full_name):
            print(f"[maintenance] {full_name} does not exist — skipping")
            continue

        cutoff_ms = int((now - timedelta(days=retain_days)).timestamp() * 1000)
        tbl = catalog.load_table(full_name)

        snapshots_before = len(tbl.metadata.snapshots)

        try:
            (
                tbl.expire_snapshots()
                .expire_older_than(cutoff_ms)
                .min_snapshots_to_keep(min_snapshots)
                .commit()
            )
            # Reload to get updated snapshot count
            tbl = catalog.load_table(full_name)
            snapshots_after = len(tbl.metadata.snapshots)
            expired = snapshots_before - snapshots_after

            print(
                f"[maintenance] {full_name}: "
                f"expired {expired} snapshot(s), "
                f"{snapshots_after} remaining "
                f"(policy: >{retain_days}d, min={min_snapshots})"
            )
            results.append({
                "table": full_name,
                "snapshots_before": snapshots_before,
                "snapshots_after": snapshots_after,
                "expired": expired,
                "status": "ok",
            })
        except Exception as e:
            print(f"[maintenance] {full_name}: ERROR — {e}")
            results.append({
                "table": full_name,
                "status": "error",
                "error": str(e),
            })

    total_expired = sum(r.get("expired", 0) for r in results)
    print(f"[maintenance] done — {total_expired} snapshot(s) expired across {len(results)} table(s)")
    return {"results": results, "total_expired": total_expired}
