#!/usr/bin/env python3
"""Compare DuckDB reads of equivalent Iceberg and DuckLake tables.

The benchmark deliberately uses DuckDB for both readers and the same deterministic source
data.  It reports query wall time (including result materialization), result hashes, and the
physical data size.  "cold" means a fresh DuckDB process/connection; the OS file cache is not
flushed, so it must not be described as a hardware cold-cache result.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import shutil
import statistics
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)


QUERIES = {
    "q1_scan_aggregate": """
        SELECT region_id, status, sum(quantity * unit_price) AS revenue,
               sum(quantity) AS units
          FROM {fact}
         GROUP BY region_id, status
         ORDER BY region_id, status
    """,
    "q2_filtered_group": """
        SELECT event_date, channel_id, count(*) AS rows,
               sum(quantity * unit_price) AS revenue
          FROM {fact}
         WHERE event_date >= DATE '2024-01-01'
           AND event_date < DATE '2025-01-01'
           AND status IN ('P', 'S')
         GROUP BY event_date, channel_id
         ORDER BY event_date, channel_id
    """,
    "q3_wide_column_scan": """
        SELECT count(*) AS rows, sum(length(payload)) AS payload_bytes,
               sum(quantity) AS units
          FROM {fact}
    """,
    "q4_top_customers": """
        SELECT customer_id, sum(quantity * unit_price) AS revenue
          FROM {fact}
         GROUP BY customer_id
         ORDER BY revenue DESC, customer_id
         LIMIT 100
    """,
    "q5_dimension_join": """
        SELECT d.category_id, d.brand_id, count(*) AS rows,
               sum(f.quantity * f.unit_price) AS revenue
          FROM {fact} f
          JOIN {dim} d ON d.product_id = f.product_id
         GROUP BY d.category_id, d.brand_id
         ORDER BY d.category_id, d.brand_id
    """,
    "q6_point_range": """
        SELECT id, event_date, customer_id, product_id, payload
          FROM {fact}
         WHERE id BETWEEN 1000000 AND 1000100
         ORDER BY id
    """,
}


ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "event_date", DateType()),
    NestedField(3, "customer_id", IntegerType()),
    NestedField(4, "product_id", IntegerType()),
    NestedField(5, "region_id", IntegerType()),
    NestedField(6, "channel_id", IntegerType()),
    NestedField(7, "quantity", IntegerType()),
    NestedField(8, "unit_price", DecimalType(12, 2)),
    NestedField(9, "discount", DecimalType(5, 4)),
    NestedField(10, "status", StringType()),
    NestedField(11, "payload", StringType()),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument(
        "--root", type=Path, default=Path("target/duckdb-iceberg-vs-ducklake")
    )
    parser.add_argument("--reuse", action="store_true", help="reuse an existing prepared lake")
    return parser.parse_args()


def path_uri(path: Path) -> str:
    uri = path.resolve().as_uri()
    # PyIceberg 0.11 concatenates URI netloc and path.  On Windows, retaining the drive
    # letter as the netloc avoids producing the invalid local path /C:/... .
    return uri.replace("file:///", "file://", 1)


def sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def duckdb_scan_path(location: str) -> str:
    # DuckDB's Windows Iceberg reader wants C:/... while PyIceberg stores file://C:/....
    return location.removeprefix("file://").replace("'", "''")


def iceberg_data_path(path: Path) -> str:
    """Keep data-file paths relative so DuckDB can resolve them on Windows."""
    absolute = path.resolve()
    try:
        return absolute.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path_uri(absolute)


def parquet_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.int64()),
            ("event_date", pa.date32()),
            ("customer_id", pa.int32()),
            ("product_id", pa.int32()),
            ("region_id", pa.int32()),
            ("channel_id", pa.int32()),
            ("quantity", pa.int32()),
            ("unit_price", pa.decimal128(12, 2)),
            ("discount", pa.decimal128(5, 4)),
            ("status", pa.string()),
            ("payload", pa.string()),
        ]
    )


def prepare_source(root: Path, rows: int, threads: int) -> list[Path]:
    source_db = root / "source.duckdb"
    parquet_dir = root / "source-parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(source_db))
    con.execute(f"PRAGMA threads={threads}")
    con.execute("DROP TABLE IF EXISTS fact_sales")
    con.execute("DROP TABLE IF EXISTS dim_product")
    con.execute(
        """
        CREATE TABLE dim_product AS
        SELECT product_id::INTEGER AS product_id,
               ((product_id * 17) % 200 + 1)::INTEGER AS category_id,
               ((product_id * 29) % 2000 + 1)::INTEGER AS brand_id,
               ('product-' || product_id::VARCHAR)::VARCHAR AS product_name
          FROM range(1, 100001) AS t(product_id)
        """
    )
    con.execute(
        f"""
        CREATE TABLE fact_sales AS
        SELECT id::BIGINT AS id,
               (DATE '2021-01-01' + ((id * 17) % 1826)::INTEGER)::DATE AS event_date,
               ((id * 7919) % 500000 + 1)::INTEGER AS customer_id,
               ((id * 3571) % 100000 + 1)::INTEGER AS product_id,
               ((id * 13) % 32 + 1)::INTEGER AS region_id,
               ((id * 7) % 5 + 1)::INTEGER AS channel_id,
               ((id % 10) + 1)::INTEGER AS quantity,
               (5 + ((id * 19) % 20000) / 100.0)::DECIMAL(12, 2) AS unit_price,
               (((id * 23) % 3000) / 10000.0)::DECIMAL(5, 4) AS discount,
               CASE id % 4 WHEN 0 THEN 'N' WHEN 1 THEN 'P'
                           WHEN 2 THEN 'S' ELSE 'R' END::VARCHAR AS status,
               repeat(md5(id::VARCHAR), 3)::VARCHAR AS payload
          FROM range(1, {rows + 1}) AS t(id)
        """
    )
    con.execute(
        f"COPY fact_sales TO '{sql_path(parquet_dir / 'fact.parquet')}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880, PER_THREAD_OUTPUT true)"
    )
    con.execute(
        f"COPY dim_product TO '{sql_path(parquet_dir / 'dim.parquet')}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
    )
    con.close()
    files = sorted((parquet_dir / "fact.parquet").rglob("*.parquet"))
    if not files:
        raise RuntimeError(f"DuckDB produced no fact parquet files in {parquet_dir}")
    return files


def create_iceberg(root: Path, fact_files: list[Path]) -> tuple[str, str]:
    catalog_path = root / "iceberg-catalog.sqlite"
    warehouse = root / "iceberg-warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "local",
        type="sql",
        uri=f"sqlite:///{catalog_path.resolve().as_posix()}",
        warehouse=iceberg_data_path(warehouse),
    )
    try:
        catalog.create_namespace("analytics")
    except Exception as error:
        if "already exists" not in str(error).lower():
            raise
    identifier = "analytics.fact_sales"
    try:
        catalog.drop_table(identifier)
    except Exception:
        pass
    table = catalog.create_table(identifier, schema=ICEBERG_SCHEMA)
    for file_path in fact_files:
        table.add_files([iceberg_data_path(file_path)])
    dim = catalog.create_table("analytics.dim_product", schema=Schema(
        NestedField(1, "product_id", IntegerType()),
        NestedField(2, "category_id", IntegerType()),
        NestedField(3, "brand_id", IntegerType()),
        NestedField(4, "product_name", StringType()),
    ))
    dim.add_files([iceberg_data_path(fact_files[0].parent.parent / "dim.parquet")])
    return table.metadata_location, dim.metadata_location


def create_ducklake(root: Path, source_dir: Path, threads: int) -> None:
    catalog_path = root / "ducklake-catalog.sqlite"
    data_path = root / "ducklake-data"
    db_path = root / "ducklake.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={threads}")
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(
        f"ATTACH 'ducklake:sqlite:{sql_path(catalog_path)}' AS lake "
        f"(DATA_PATH '{sql_path(data_path)}')"
    )
    con.execute("DROP TABLE IF EXISTS lake.main.fact_sales")
    con.execute("DROP TABLE IF EXISTS lake.main.dim_product")
    # Register the exact source Parquet files. This isolates catalog/scan overhead from the
    # format's writer defaults and makes the Iceberg/DuckLake physical input identical.
    con.execute(
        f"CREATE TABLE lake.main.fact_sales AS SELECT * FROM read_parquet('{sql_path(source_dir / 'fact.parquet' / '*.parquet')}') LIMIT 0"
    )
    con.execute(
        f"CREATE TABLE lake.main.dim_product AS SELECT * FROM read_parquet('{sql_path(source_dir / 'dim.parquet')}') LIMIT 0"
    )
    for file_path in sorted((source_dir / "fact.parquet").rglob("*.parquet")):
        con.execute(
            f"SELECT * FROM ducklake_add_data_files('lake', 'fact_sales', '{sql_path(file_path)}')"
        ).fetchall()
    con.execute(
        f"SELECT * FROM ducklake_add_data_files('lake', 'dim_product', '{sql_path(source_dir / 'dim.parquet')}')"
    ).fetchall()
    con.close()


def file_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def file_count(root: Path, pattern: str = "*") -> int:
    return sum(1 for path in root.rglob(pattern) if path.is_file())


def result_hash(rows: list[tuple]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(repr(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def execute_suite(
    db_path: Path,
    views: tuple[str, str],
    samples: int,
    threads: int,
    cold: bool,
) -> list[dict]:
    fact, dim = views
    output: list[dict] = []
    persistent = duckdb.connect(str(db_path)) if not cold else None
    if persistent is not None and fact.startswith("lake."):
        persistent.execute("INSTALL ducklake; LOAD ducklake")
        persistent.execute(
            f"ATTACH 'ducklake:sqlite:{sql_path(db_path.parent / 'ducklake-catalog.sqlite')}' AS lake "
            f"(DATA_PATH '{sql_path(db_path.parent / 'ducklake-data')}')"
        )
    for name, template in QUERIES.items():
        timings: list[float] = []
        observed_hash = None
        for sample in range(samples + 1):
            con = persistent or duckdb.connect(str(db_path))
            con.execute(f"PRAGMA threads={threads}")
            con.execute("PRAGMA enable_object_cache=false")
            if fact.startswith("iceberg_"):
                con.execute("LOAD iceberg")
            if persistent is None and fact.startswith("lake."):
                con.execute("INSTALL ducklake; LOAD ducklake")
                con.execute(
                    f"ATTACH 'ducklake:sqlite:{sql_path(db_path.parent / 'ducklake-catalog.sqlite')}' AS lake "
                    f"(DATA_PATH '{sql_path(db_path.parent / 'ducklake-data')}')"
                )
            sql = template.format(fact=fact, dim=dim)
            started = time.perf_counter()
            rows = con.execute(sql).fetchall()
            elapsed_ms = (time.perf_counter() - started) * 1000
            digest = result_hash(rows)
            if observed_hash is None:
                observed_hash = digest
            elif observed_hash != digest:
                raise RuntimeError(f"{name} returned different results across samples")
            if sample > 0:
                timings.append(elapsed_ms)
            if persistent is None:
                con.close()
        output.append({
            "query": name,
            "mode": "cold" if cold else "warm",
            "samples": timings,
            "median_ms": statistics.median(timings),
            "min_ms": min(timings),
            "max_ms": max(timings),
            "rows": len(rows),
            "result_sha256": observed_hash,
        })
    if persistent:
        persistent.close()
    return output


def main() -> None:
    args = parse_args()
    if args.rows < 1 or args.samples < 1 or args.threads < 1:
        raise SystemExit("rows, samples and threads must be positive")
    root = args.root.resolve()
    if not args.reuse:
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True)
        fact_files = prepare_source(root, args.rows, args.threads)
        iceberg_location, iceberg_dim_location = create_iceberg(root, fact_files)
        create_ducklake(root, root / "source-parquet", args.threads)
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    "rows": args.rows,
                    "fact_files": [str(p) for p in fact_files],
                    "iceberg_metadata": iceberg_location,
                    "iceberg_dim_metadata": iceberg_dim_location,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    else:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        fact_files = [Path(path) for path in manifest["fact_files"]]
        iceberg_location = manifest["iceberg_metadata"]
        iceberg_dim_location = manifest["iceberg_dim_metadata"]

    # One DuckDB database owns the comparison views; data is read through each format extension.
    runner = root / "runner.duckdb"
    con = duckdb.connect(str(runner))
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL ducklake; LOAD ducklake")
    con.execute(
        f"ATTACH 'ducklake:sqlite:{sql_path(root / 'ducklake-catalog.sqlite')}' AS lake "
        f"(DATA_PATH '{sql_path(root / 'ducklake-data')}')"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW iceberg_fact AS SELECT * FROM iceberg_scan('{duckdb_scan_path(iceberg_location)}')"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW iceberg_dim AS SELECT * FROM iceberg_scan('{duckdb_scan_path(iceberg_dim_location)}')"
    )
    con.close()

    result = {
        "duckdb_version": duckdb.__version__,
        "pyiceberg_version": importlib.metadata.version("pyiceberg"),
        "rows": args.rows,
        "threads": args.threads,
        "samples": args.samples,
        "iceberg_data_bytes": file_bytes(root / "source-parquet"),
        "iceberg_metadata_bytes": file_bytes(root / "iceberg-warehouse")
        + (root / "iceberg-catalog.sqlite").stat().st_size,
        "iceberg_data_files": file_count(root / "source-parquet", "*.parquet"),
        "ducklake_data_bytes": file_bytes(root / "source-parquet"),
        "ducklake_metadata_bytes": (root / "ducklake-catalog.sqlite").stat().st_size,
        "ducklake_data_files": file_count(root / "source-parquet", "*.parquet"),
        "ducklake_data_mode": "external_same_source_parquet",
        "iceberg": execute_suite(runner, ("iceberg_fact", "iceberg_dim"), args.samples, args.threads, False)
        + execute_suite(runner, ("iceberg_fact", "iceberg_dim"), args.samples, args.threads, True),
        "ducklake": execute_suite(runner, ("lake.main.fact_sales", "lake.main.dim_product"), args.samples, args.threads, False)
        + execute_suite(runner, ("lake.main.fact_sales", "lake.main.dim_product"), args.samples, args.threads, True),
    }
    hashes = {
        (engine, row["query"]): row["result_sha256"]
        for engine in ("iceberg", "ducklake")
        for row in result[engine]
    }
    for query in QUERIES:
        if hashes[("iceberg", query)] != hashes[("ducklake", query)]:
            raise RuntimeError(f"result mismatch for {query}")
    out_json = root / "result.json"
    out_csv = root / "result.csv"
    out_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["engine", "mode", "query", "median_ms", "min_ms", "max_ms"])
        writer.writeheader()
        for engine in ("iceberg", "ducklake"):
            for row in result[engine]:
                writer.writerow({"engine": engine, **{key: row[key] for key in writer.fieldnames[1:]}})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
