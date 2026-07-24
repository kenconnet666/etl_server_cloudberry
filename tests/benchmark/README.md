# PostgreSQL vs Cloudberry analytical benchmark

This benchmark compares the analytical read path of PostgreSQL 18 heap tables with the project's
production Cloudberry 2.1 AOCO profile. It is separate from the end-to-end ETL throughput benchmark
in `docs/standalone-benchmark.md`.

The data set contains a wide sales fact table and a product dimension. Both engines receive the
same deterministic rows, types, primary keys, and queries. Cloudberry uses
`ao_column`/ZSTD level 1 and `DISTRIBUTED BY (id)`, matching the project target contract. The suite
contains full and filtered aggregates, a scan that reads the wide payload, high-cardinality
grouping, a dimension join, and an indexed 101-row range lookup. The point/range query is retained
as a deliberate row-store-friendly control.

From WSL, with the integration PostgreSQL and Cloudberry containers running:

```bash
cd /mnt/c/code/etl_server_cloudberry
bash tests/benchmark/cloudberry-vs-postgres.sh setup 5000000
bash tests/benchmark/cloudberry-vs-postgres.sh run
```

`setup` creates a dedicated `analytics_bench` database on each disposable cluster when needed and
replaces only the same-named schema inside it. Keeping the benchmark outside the integration
`source`/`target` databases prevents logical-replication triggers and project metadata from
polluting the native engine comparison. `run` executes one unreported warm-up and five measured
`EXPLAIN ANALYZE` samples per query, then prints the raw execution times and median/min/max summary.
Use `clean` to remove the benchmark schema; the empty database is retained for repeatable runs.

This local cluster has one Cloudberry primary segment. Its result can establish a columnar storage
and execution value signal, but it cannot validate MPP scale-out. A release claim requires the same
suite at multiple scale factors on a production-like multi-segment topology, with concurrency and
cold-cache runs reported separately.

## DuckDB Iceberg vs DuckLake

For a same-reader lake-format comparison, use the Python benchmark from the repository root:

```powershell
python tests/benchmark/duckdb-iceberg-vs-ducklake.py --rows 5000000 --samples 5 --threads 8
```

It requires DuckDB 1.5.4 (the `iceberg` and `ducklake` extensions) and Python packages
`duckdb`, `pyarrow`, `pyiceberg`, and `sqlalchemy`. The setup creates one deterministic wide fact
table and one dimension table, registers the same ZSTD Parquet files in both catalogs, and checks
SHA-256 result equality for every query. The `warm` samples reuse a DuckDB connection. The `cold`
samples use a fresh connection and reload the extension/catalog; the operating-system file cache is
not flushed, so call them process-cold/metadata-cold rather than hardware cold-cache results.

JSON and CSV evidence is written below `target/duckdb-iceberg-vs-ducklake/`. The reported data bytes
are therefore identical by construction; catalog metadata bytes and data-file counts are reported
separately. A separate managed DuckLake write benchmark is needed when comparing writer defaults,
compression, compaction, or update/delete maintenance costs.

For a local four-segment comparison, start a separate container without replacing the integration
target, then point the benchmark at it:

```bash
CBDB_CONTAINER=cbdb-bench CBDB_PORT=55434 CBDB_SEGMENTS=4 \
  bash tests/integration/cloudberry/build-local-image.sh
CLOUDBERRY_CONTAINER=cbdb-bench \
  bash tests/benchmark/cloudberry-vs-postgres.sh setup 5000000
CLOUDBERRY_CONTAINER=cbdb-bench \
  bash tests/benchmark/cloudberry-vs-postgres.sh run
```
