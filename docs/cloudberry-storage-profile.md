# Cloudberry Storage Profiles

Replicated business tables are an analytical current-state replica, not the target adapter's
control plane. Their physical layout is selected explicitly and is part of the managed-table
fingerprint. A table and every shadow used to load or replace it always have the same profile.

## Profiles

| Profile | Cloudberry 2.1 DDL | Intended workload |
| --- | --- | --- |
| `ao_column` (default) | `USING ao_column WITH (compresstype='zstd', compresslevel=1)` | Production analytical current-state replica. |
| `pax_experimental` | `USING pax WITH (storage_format='porc', compresstype='zstd', compresslevel=1)` | Opt-in benchmark and compatibility evaluation only. |

The target metadata schema (`pg2cb_meta`) and transaction-local staging tables always use heap.
They are mutable coordination structures, not analytical data, and are deliberately outside this
setting.

Heap is not a valid business-table profile. Existing managed heap tables are treated as physical
storage drift and require a fresh snapshot generation before CDC can resume. Row storage remains
appropriate for the adapter's staging and control plane, not for the analytical replica.

PAX is deliberately experimental. Cloudberry 2.1 leaves important table-access-method paths and
concurrency tests incomplete, and does not support speculative insert (`ON CONFLICT`). The project
does not claim complete SQL, type, concurrency, recovery, or operational compatibility for PAX.
It is also compile-time optional; when explicitly selected, the runtime verifies that `pax` exists
in `pg_am` before starting the pipeline.

## Configuration

Target profile JSON selects the default for every mapped business table:

```json
{
  "default_table_storage": "ao_column"
}
```

An explicit pipeline mapping may override only that table:

```json
{
  "table_mappings": [
    {
      "source": {"schema": "public", "name": "pax_candidate"},
      "target": {"schema": "analytics", "name": "pax_candidate"},
      "storage": "pax_experimental"
    }
  ]
}
```

Values are a closed enum: `ao_column` or `pax_experimental`. The values `heap` and `pax` are
rejected so a business table cannot accidentally become row storage or silently opt into an
experimental feature. Compression is intentionally not an arbitrary per-pipeline SQL option.
`zstd` level 1 is a bounded baseline; changing compression, clustering, or PAX statistics requires
a versioned profile backed by benchmark evidence.

Changing a selected profile changes the physical fingerprint. On resume the runtime reads the
active relation's access method from `pg_class`/`pg_am`; a mismatch requests a fresh snapshot
generation before any CDC write. The current coordinator performs this as a full pipeline rebuild.
The Phase 2 table transition handler will narrow the same mechanism to the affected table or
dependency closure without changing the storage contract.

## Type And Distribution Rules

Storage selection never changes source semantics. Keep exact mapped types: `numeric` remains
`numeric`, `timestamp with time zone` remains zoned, and JSON, binary values, arrays, UUID and
network values are not downgraded for compression. The Cloudberry integration suite verifies these
mappings on the production AOCO profile. The PAX smoke test intentionally covers only create,
insert, update and delete and is not a complete type claim.

Business tables stay `DISTRIBUTED BY` the complete source primary key. That keeps the current
staging join, upsert/delete path and primary-key movement colocated. Do not introduce time
partitioning or a different distribution key automatically: they require an explicit source query
and retention contract plus distribution-skew benchmarks.

## Verification

The Cloudberry 2.1 integration job runs the project's typed staging/COPY/current-state apply
sequence on AOCO. PAX has a separate ignored experimental smoke test. Before enabling
`pax_experimental`, measure at least these workloads against AOCO on the actual segment topology:

- snapshot `COPY` throughput and final table size;
- CDC batches with the production insert/update/delete ratio and p95 apply lag;
- representative filtered aggregate and projection scans;
- post-update maintenance cost and recovery after a target restart.

Keep AOCO unless PAX preserves the exact source/target reconciliation result, materially improves
the representative analytical workload, and passes extended concurrency, recovery and soak
testing. Passing the smoke test alone is not a production-support signal.
