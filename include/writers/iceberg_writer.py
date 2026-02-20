"""
Iceberg writer.

Uses PyIceberg with the Nessie REST catalog to create and append to
Iceberg tables stored on MinIO.

Catalog URL:  http://nessie:19120/api/v2
Warehouse:    s3://airflow-data/iceberg
"""

import os

NESSIE_URI = "http://nessie:19120/iceberg"
WAREHOUSE = "themeparks"  # matches nessie.catalog.default-warehouse in docker-compose.override.yml


def get_catalog():
    """
    Return a PyIceberg catalog pointed at the Nessie REST catalog.
    MinIO credentials are read from environment variables set in .env.
    """
    from pyiceberg.catalog import load_catalog

    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": NESSIE_URI,
            # S3-compatible MinIO config for PyIceberg's s3fs backend
            "s3.endpoint": endpoint,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.path-style-access": "true",
        },
    )


def _sanitize_schema(schema):
    """
    Recursively replace pa.null() fields with pa.string() throughout a PyArrow schema.

    Iceberg format version 2 does not support the null type — PyArrow infers null()
    when every value in a column is None (e.g. optional queue fields that are absent
    for all parks in the current batch). Casting to string is safe: downstream readers
    will still see NULL for those rows, but the column has a concrete Iceberg type.
    """
    import pyarrow as pa

    def _fix_type(dtype):
        if dtype == pa.null():
            return pa.string()
        if pa.types.is_struct(dtype):
            new_fields = [
                pa.field(f.name, _fix_type(f.type), nullable=f.nullable)
                for f in dtype
            ]
            return pa.struct(new_fields)
        if pa.types.is_list(dtype):
            return pa.list_(_fix_type(dtype.value_type))
        if pa.types.is_large_list(dtype):
            return pa.large_list(_fix_type(dtype.value_type))
        if pa.types.is_map(dtype):
            return pa.map_(_fix_type(dtype.key_type), _fix_type(dtype.item_type))
        return dtype

    new_fields = [
        pa.field(f.name, _fix_type(f.type), nullable=f.nullable)
        for f in schema
    ]
    return pa.schema(new_fields)


def append_to_iceberg(
    catalog,
    namespace: str,
    table_name: str,
    records: list[dict],
    overwrite: bool = False,
    allow_schema_migration: bool = False,
) -> dict:
    """
    Convert records to a PyArrow table and write to the named Iceberg table.
    Creates the namespace and table on first run.

    overwrite=True: replaces the entire table contents (suitable for reference
    tables like destinations, parks, entities where each run fetches the full
    current dataset).

    overwrite=False (default): appends rows (suitable for time-series tables
    like live_data where history must be preserved).

    allow_schema_migration=True: if the incoming Arrow schema is incompatible
    with the existing Iceberg table schema, drop and recreate the table rather
    than raising. Use this when a schema change is intentional (e.g. a struct
    column has been flattened). Existing data is lost.

    Returns a summary dict with snapshot info.
    """
    import pyarrow as pa
    import pyarrow.json as paj
    import io
    import json
    from pyiceberg.exceptions import ValidationError, ValidationException

    # Build Arrow table from records via NDJSON round-trip so PyArrow
    # infers types rather than defaulting everything to string.
    ndjson_bytes = "\n".join(json.dumps(r, default=str) for r in records).encode()
    arrow_table = paj.read_json(io.BytesIO(ndjson_bytes))

    # Iceberg v2 rejects pa.null() columns (fields all-None in this batch).
    # Replace every null-typed field with string so the schema is concrete.
    clean_schema = _sanitize_schema(arrow_table.schema)
    if clean_schema != arrow_table.schema:
        arrow_table = arrow_table.cast(clean_schema)
        print(f"[iceberg] sanitized null-typed fields in schema")

    full_name = f"{namespace}.{table_name}"

    # Ensure namespace exists
    if not catalog.namespace_exists(namespace):
        catalog.create_namespace(namespace)
        print(f"[iceberg] created namespace '{namespace}'")

    operation = "overwrite" if overwrite else "append"

    if catalog.table_exists(full_name):
        tbl = catalog.load_table(full_name)
        if overwrite:
            tbl.overwrite(arrow_table)
            print(f"[iceberg] overwrote '{full_name}' with {len(records)} rows")
        else:
            try:
                tbl.append(arrow_table)
                print(f"[iceberg] appended {len(records)} rows to '{full_name}'")
            except (ValidationError, ValidationException, ValueError) as e:
                if allow_schema_migration:
                    print(
                        f"[iceberg] schema conflict on '{full_name}': {e}\n"
                        f"[iceberg] allow_schema_migration=True — dropping and recreating table"
                    )
                    catalog.drop_table(full_name)
                    tbl = catalog.create_table(full_name, schema=arrow_table.schema)
                    tbl.append(arrow_table)
                    print(f"[iceberg] recreated '{full_name}' and wrote {len(records)} rows")
                else:
                    raise
    else:
        tbl = catalog.create_table(full_name, schema=arrow_table.schema)
        tbl.append(arrow_table)
        print(f"[iceberg] created table '{full_name}' and wrote {len(records)} rows")

    snapshot = tbl.current_snapshot()
    return {
        "table": full_name,
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "rows_written": len(records),
    }
