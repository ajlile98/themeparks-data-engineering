"""
## Theme Park ETL — Live Data Current → Iceberg (gold)

Triggered by the iceberg_live_data asset (silver layer, every 5 minutes).
Reads the full silver.live_data table, deduplicates to one row per entity
(latest ingest_timestamp wins), and overwrites gold.live_data_current.

This is the "Strategy B" materialisation discussed in the query notebook:
deduplicate once here so that downstream queries can filter cheaply with no
window function. The table is always a complete, current snapshot of every
entity's live state across all parks.
"""

from airflow.sdk import Asset, asset

SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"
SILVER_TABLE = "live_data"
GOLD_TABLE = "live_data_current"


@asset(schedule=[Asset("iceberg_live_data")])
def gold_live_data_current(context: dict) -> dict:
    """
    Deduplicate silver.live_data to one row per entity (latest snapshot)
    and overwrite gold.live_data_current.
    """
    import pyarrow as pa
    from include.writers.iceberg_writer import get_catalog, append_to_iceberg

    catalog = get_catalog()

    # ── Read silver table as Arrow via PyIceberg scan ─────────────────────────
    silver_tbl = catalog.load_table(f"{SILVER_NAMESPACE}.{SILVER_TABLE}")
    arrow_table = silver_tbl.scan().to_arrow()
    total_rows = len(arrow_table)
    print(f"[gold] read {total_rows} rows from {SILVER_NAMESPACE}.{SILVER_TABLE}")

    if total_rows == 0:
        print("[gold] silver table is empty — skipping gold write")
        return {"table": f"{GOLD_NAMESPACE}.{GOLD_TABLE}", "rows_written": 0}

    # ── Deduplicate: keep the row with the latest ingest_timestamp per id ─────
    # Convert to pandas for a clean sort + drop_duplicates, then back to Arrow.
    # ingest_timestamp is a UTC ISO 8601 string — lexicographic sort is correct.
    import pandas as pd
    df = arrow_table.to_pandas()
    df_deduped = (
        df.sort_values("ingest_timestamp", ascending=False)
          .drop_duplicates(subset="id", keep="first")
          .reset_index(drop=True)
    )
    deduped_rows = len(df_deduped)
    print(f"[gold] deduplicated {total_rows} → {deduped_rows} rows (one per entity)")

    # Restore Arrow schema (pandas may widen some types)
    deduped_arrow = pa.Table.from_pandas(df_deduped, preserve_index=False)

    # ── Overwrite gold table ──────────────────────────────────────────────────
    result = append_to_iceberg(
        catalog,
        namespace=GOLD_NAMESPACE,
        table_name=GOLD_TABLE,
        records=df_deduped.to_dict(orient="records"),
        overwrite=True,
    )
    print(f"[gold] overwrote {GOLD_NAMESPACE}.{GOLD_TABLE} with {deduped_rows} rows")
    return result
