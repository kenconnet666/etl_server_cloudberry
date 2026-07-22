# Standalone AOCO benchmark

Last measured: 2026-07-22.

## Scope and environment

This benchmark measures the release build's complete PostgreSQL 18 logical-replication path into
Apache Cloudberry 2.1. Business tables use the production default `ao_column` access method. PAX
remains experimental; its separate INSERT-only probe is reported below and is not mixed into the
production AOCO results.

| Item | Value |
| --- | --- |
| Host | WSL Debian, local Docker engine |
| Source | PostgreSQL 18, `127.0.0.1:55432` |
| Target | Cloudberry 2.1 single-node demo cluster, `127.0.0.1:55433` |
| Build | `cargo test --release` |
| Row | `bigint` PK plus 64-byte text payload |
| CDC chunk limit | 100,000 rows / 64 MiB |
| Atomic batch delay | 250 ms maximum |
| Latency | 30 sequential idle single-row INSERT samples |

Snapshot is timed from job start until the target row count is visible. Backlog is timed from job
restart with committed WAL already waiting. Streaming, update, and delete include the source SQL
and wait until the target state is visible. Source SQL time is also emitted separately.

Run it with:

```bash
export PG2CB_TEST_SOURCE_DSN='postgres://postgres:pg2cb_test@127.0.0.1:55432/source'
export PG2CB_TEST_TARGET_DSN='postgres://gpadmin@127.0.0.1:55433/target'
export PG2CB_BENCH_ROWS=100000
# Optional: split each INSERT phase into smaller committed source transactions.
export PG2CB_BENCH_TRANSACTION_ROWS=1000
export CARGO_TARGET_DIR=/tmp/pg2cb-target
cargo test --release -p cloudberry-etl-engine \
  --test phase1_recovery_e2e standalone_aoco_throughput_benchmark \
  -- --ignored --nocapture
```

## Current results

The current bounded path locks and checks the target checkpoint before DML, folds all complete
source transactions in the batch by table/key, and commits their COPY/DML plus the final checkpoint
once. A batch is eligible only when it has no schema barrier and stays within both configured row
and byte limits. An oversized source transaction retains the chunk ledger and its bounded restart
position.

| Workload | 100k, one source transaction | 1M, 1000 x 1k source transactions |
| --- | ---: | ---: |
| Initial AOCO snapshot | 52,860 rows/s | 145,910 rows/s |
| WAL backlog INSERT | 29,010 rows/s | 46,753 rows/s |
| Online streaming INSERT | 35,024 rows/s | 46,753 rows/s |
| Warm UPDATE | 20,936 rows/s | 15,378 rows/s |
| DELETE | 27,248 rows/s (50k rows) | 22,712 rows/s (500k rows) |
| Idle INSERT p50 / p95 | 324 / 362 ms | 325 / 347 ms |
| Process RSS after run | 105.4 MB | 111.2 MB |
| Final target relation size | 12.1 MB | 115.9 MB |

The previous 1M oversized-transaction measurement used one source transaction split into ten
100k target ledger chunks: 34,253 backlog and 34,547 streaming INSERT rows/s. Transaction shape is
therefore material: 1000 small source transactions become ten bounded atomic target commits and
reach 46,753/46,753 rows/s in the final run, while one oversized source transaction must preserve
its durable chunk resume state and reaches about 34.5k rows/s. A preceding identical 1M run measured
48,270/47,077 rows/s, putting the observed bounded-batch range at 46.8k-48.3k rows/s.

### Small-transaction batching A/B

The same release test was run before and after the atomic target batch change with 100k rows split
into 100 committed source transactions of 1k rows each.

| Complete-chain INSERT | Before: per-source-tx target commit | After: bounded atomic batch | Change |
| --- | ---: | ---: | ---: |
| WAL backlog | 6,739 rows/s | 35,288 rows/s | +423.6% |
| Online streaming | 7,041 rows/s | 45,212 rows/s | +542.1% |

The after result is the median of three consecutive runs after exact-limit immediate flush was
enabled. Backlog ranged from 32,835 to 41,262 rows/s and streaming from 43,327 to 45,739 rows/s.
Each run used one target commit per bounded group, not one per source transaction. The real runtime
recovery matrix also folds three committed UPDATE transactions over the same five keys into one
target commit and verifies the final source/target rows exactly.

### Repeated-operation folding A/B

The dedicated coalescing benchmark loads 25k keys, stops the pipeline, then generates four UPDATE
passes in 100 source transactions of 1k rows. Every run consumes the same 100k WAL operations and
converges to the same 25k final rows. `batch_max_rows` controls whether updates to the same key can
meet in one target batch.

| Batch rows | Target commits | Estimated target row applications | Backlog drain | Input operations/s |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 100 | 100,000 | 26.800 s | 3,731 |
| 10,000 (old default) | 10 | 100,000 | 5.441 s | 18,380 |
| 25,000 (one pass/commit) | 4 | 100,000 | 4.641 s | 21,545 |
| 100,000 (new default) | 1 | 25,000 | 2.805 s | 35,654 |

Compared with one target commit per complete pass, cross-pass final-state folding reduces estimated
target row applications by 75%, reduces drain time by 39.6%, and raises input-operation throughput
by 65.5%. Compared with the old 10k default it is 94.0% faster; compared with one target commit per
source transaction it is 9.56 times faster. A full batch now flushes immediately when it reaches a
row or byte limit instead of waiting for the batch timer. The production default row limit is 100k;
the independent 16 MiB byte and 250 ms delay limits remain unchanged.

Run the folding and control cases with:

```bash
cargo test --release -p cloudberry-etl-engine \
  --test phase1_recovery_e2e standalone_aoco_operation_coalescing_benchmark \
  -- --ignored --nocapture

PG2CB_COALESCE_BATCH_MAX_ROWS=25000 cargo test --release \
  -p cloudberry-etl-engine --test phase1_recovery_e2e \
  standalone_aoco_operation_coalescing_benchmark -- --ignored --nocapture
```

### Replica identity decision

`REPLICA IDENTITY FULL` is not required for operation folding. The primary key provides lineage,
and pgoutput presence markers let the normalizer retain an unknown unchanged TOAST column until a
later operation supplies a value; target staging preserves any still-unchanged baseline column.
FULL old non-key values therefore do not improve final-state correctness or key-based coalescing.

A PG18 WAL-byte A/B updated identical tables after a checkpoint:

| UPDATE workload | DEFAULT WAL | FULL WAL | FULL overhead |
| --- | ---: | ---: | ---: |
| 25k rows, 64-byte payload | 10,382,744 bytes | 12,388,616 bytes | +19.3% |
| 5k rows, approximately 6 KiB payload | 68,252,256 bytes | 99,184,584 bytes | +45.3% |

The extra old-row WAL also consumes network, decode, spool, and batch byte capacity. DEFAULT remains
the recommended source configuration; FULL may be accepted for an independently required source
policy, but this service does not request it as a throughput optimization.

The 10k diagnostic run after optimization measured 14,247 streaming INSERT, 11,922 UPDATE, and
7,090 DELETE rows/s. Before the ledger-aware INSERT path and homogeneous-batch shortcuts, the same
release workload measured 536, 541, and 760 rows/s respectively, with about 985 ms idle p50.

Pure source INSERT rows are appended with direct target COPY only after either the checkpoint
preflight proves a bounded atomic batch has not committed or the durable chunk ledger proves an
oversized chunk is being applied for the first time. Lost commit responses are resolved before DML
can run again. UPDATE/DELETE/PK move, incomplete TOAST rows, and mixed batches retain the staged
current-state path. Homogeneous complete UPDATE and DELETE batches skip irrelevant SQL branches.

### Experimental PAX INSERT ceiling

The same bounded transaction folding path was run against the opt-in PAX profile
`pax(storage_format='porc', compresstype='zstd', compresslevel=1)`. This is an experimental
INSERT-only measurement, not a support claim. A complete current-state batch UPDATE fails in
Cloudberry 2.1 with `not supported on pax relations: IndexDeleteTuples`; PAX therefore cannot
replace AOCO for this replica while UPDATE/DELETE remain required.

| PAX workload | Snapshot | WAL backlog INSERT | Streaming INSERT | Target relation |
| --- | ---: | ---: | ---: | ---: |
| 100k, 100 x 1k source transactions | 55,214 rows/s | 32,091 rows/s | 44,585 rows/s | 6.9 MiB |
| 1M, 1000 x 1k, run 1 | 140,728 rows/s | 49,637 rows/s | 46,708 rows/s | 68.0 MiB |
| 1M, 1000 x 1k, run 2 | 150,158 rows/s | 49,446 rows/s | 50,012 rows/s | 68.0 MiB |

The observed PAX ceiling is therefore about 50k rows/s. Compared max-to-max with AOCO's previous
48,270 backlog and 47,077 streaming observations, PAX is only 2.8% faster on backlog and 6.2% faster
on streaming. The ranges are close enough that PAX does not materially expand the single-node CDC
ceiling. Its clear result in this workload is storage density: approximately 68.0 MiB versus AOCO's
115.9 MiB at 1M rows, about 41% smaller before any UPDATE/DELETE history.

Run the isolated experimental probe with:

```bash
export PG2CB_TEST_SOURCE_DSN='postgres://postgres:pg2cb_test@127.0.0.1:55432/source'
export PG2CB_TEST_TARGET_DSN='postgres://gpadmin@127.0.0.1:55433/target'
export PG2CB_BENCH_ROWS=1000000
export PG2CB_BENCH_TRANSACTION_ROWS=1000
cargo test --release -p cloudberry-etl-engine \
  --test phase1_recovery_e2e standalone_pax_experimental_throughput_benchmark \
  -- --ignored --nocapture
```

## Stage ceilings and queue decision

| Stage | Measured capacity | Status |
| --- | ---: | --- |
| PostgreSQL source bulk INSERT | historical 220k-344k rows/s | Source-only reference |
| `pgoutput` transport/decode | historical 153k-163k rows/s | DuckLake harness; current Rust path not isolated yet |
| AOCO snapshot COPY | 145,910 rows/s | Current 1M complete snapshot |
| Bounded atomic CDC INSERT | 46,753 rows/s backlog/streaming | Current 1M complete chain; prior run up to 48,270 |
| Oversized ledgered CDC INSERT | about 34,500 rows/s | One 1M source transaction, ten target chunks |

The engine already has the useful part of a local message queue: `Batcher` bounds pending work by
rows, estimated bytes, and delay, while the transaction spool bounds oversized source transactions
on disk. Adding Kafka or another external broker would not raise the single-target steady-state
ceiling; it is justified only for independent retention, replay, fan-out, or operational isolation.

The next local concurrency experiment should be a bounded channel of complete committed
transactions so pgoutput decode/spool can overlap one ordered target apply. It must carry whole
transactions, stop reading at row/byte watermarks, ACK only the final durably applied LSN, and keep
DDL/TRUNCATE as barriers. It should be adopted only after a current Rust decode-only benchmark
shows enough non-target time to overlap.

Client-side columnar conversion is not planned. Cloudberry COPY consumes a row stream and AOCO
performs its own column grouping/compression; transposing rows before COPY would add a second
materialization without removing target work. The useful pre-processing is key-based current-state
folding before COPY, which the bounded atomic path now performs across source transactions.

### Per-table connection parallelism

The standalone one-segment target does not benefit from dispatching independent tables through a
connection pool. A target-only release benchmark pre-opens all connections, pre-encodes the same
400k rows (four AOCO tables x 100k rows), and alternates the order of 1/2/4-connection samples so
connection setup, row generation, and fixed run order are outside the measurement:

| Target connections | Median COPY throughput | Change vs one connection |
| ---: | ---: | ---: |
| 1 | 452,602 rows/s | baseline |
| 2 | 306,164 rows/s | -32.4% |
| 4 | 302,840 rows/s | -33.1% |

The test cluster has one coordinator and one primary segment. Concurrent COPY sessions contend for
the same segment resources; they do not create more target write capacity. This isolated ceiling is
not an end-to-end CDC number, but it is sufficient to reject a per-table target connection pool as
a standalone default.

Run the reproducible probe with:

```bash
export PG2CB_TEST_TARGET_DSN='postgres://gpadmin@127.0.0.1:55433/target'
cargo test --release -p cloudberry-etl-target-cloudberry \
  --test cloudberry21 cloudberry21_parallel_aoco_copy_benchmark \
  -- --ignored --nocapture
```

The existing global `Batcher` remains the bounded accumulation pool: it already groups rows by
table during normalization while retaining cross-table source transaction atomicity. Creating an
unbounded queue per discovered table would weaken memory fairness and DDL drain behavior without
helping this target. A future real multi-segment experiment may revisit table workers. The safe
protocol would require a durable per-table applied LSN, strict in-table ordering, a global row/byte
watermark, draining all workers at DDL/TRUNCATE, and publishing the global checkpoint/ACK only after
every affected table is durable. Merely running the current atomic requests on separate connections
is incorrect: the first request could publish the batch checkpoint before the other tables commit.

## DuckLake comparison

`C:\code\debezium-server-ducklake\README.md` reports a 2026-07-19 PostgreSQL 18 complete-chain
baseline on WSL Debian with 100k INSERT/UPDATE and 50k DELETE rows. It targets DuckLake local files,
whereas this project targets a transactional MPP AOCO table, so this is an engineering comparison,
not an identical target benchmark.

| Complete-chain workload | Cloudberry AOCO | DuckLake README | Difference |
| --- | ---: | ---: | ---: |
| Backlog INSERT | 29,010 | 30,864 | -6.0% |
| Streaming INSERT | 35,024 | 34,542 | +1.4% |
| Warm UPDATE | 20,936 | 31,055 | -32.6% |
| DELETE | 27,248 | 37,037 | -26.4% |

At 1M rows with 1k-row source transactions, AOCO streaming and backlog INSERT both reach 46,753
rows/s in the final run. UPDATE and DELETE remain slower because current-state changes on a growing
AOCO relation carry column-store delete/update work that DuckLake's local mirror path does not share.

DuckLake's `153k-163k rows/s` pgoutput number is a historical protocol-only harness and must not be
compared with either complete chain. Its historical 172-280 ms idle latency is also not a current
p50/p95 measurement. The current AOCO single-row p50/p95 range is 324-325/347-362 ms; Cloudberry
fixed distributed transaction cost still dominates an idle one-row batch.

## Correctness gates used with the benchmark

The optimized build also passed the real PG18/Cloudberry recovery matrix after the 100k default and
exact-limit flush changes (32.04 seconds test time, about 5.39 MiB large-transaction RSS growth): source read and spool
faults, target commit before/after ambiguity, bounded atomic-batch commit-response loss, cross-source
transaction key folding, bounded snapshot restart, disk high-water recovery, 32 MiB transaction
spill, table-local DDL reload, schema-scoped enum evolution, DDL snapshot-page failure with a fresh
source boundary, PK move, and exact source/target comparison. Performance shortcuts do not change
checkpoint, ACK, generation, DDL, or reconciliation authority.
