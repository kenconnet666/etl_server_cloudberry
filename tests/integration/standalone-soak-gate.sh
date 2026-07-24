#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: standalone-soak-gate.sh <soak-result.json> [minimum-duration-seconds]

The result is emitted by standalone_process_e2e with PG2CB_SOAK_RESULT_FILE.
The sibling .csv file is required. The gate checks complete, monotonic samples,
final source/target equality, drained spool, repaired reconciliation, and a run
duration at least as long as the requested production observation window.
EOF
}

[[ $# -ge 1 && $# -le 2 ]] || { usage; exit 2; }
result_file=$1
minimum_duration=${2:-86400}

[[ -r "$result_file" ]] || {
  echo "soak result is not readable: $result_file" >&2
  exit 2
}
[[ "$minimum_duration" =~ ^[0-9]+$ && "$minimum_duration" -ge 30 ]] || {
  echo "minimum duration must be an integer of at least 30 seconds" >&2
  exit 2
}

command -v python3 >/dev/null || {
  echo "python3 is required to validate soak evidence" >&2
  exit 2
}

python3 - "$result_file" "$minimum_duration" <<'PY'
import json
import csv
import sys
from pathlib import Path

path = sys.argv[1]
minimum_duration = int(sys.argv[2])
with open(path, encoding="utf-8") as source:
    result = json.load(source)

csv_path = Path(path).with_suffix(".csv")
if not csv_path.is_file():
    print(f"soak sample CSV is not readable: {csv_path}", file=sys.stderr)
    raise SystemExit(2)

required_sample_fields = {
    "timestamp_unix_ms",
    "elapsed_ms",
    "source_rows",
    "target_rows",
    "lag_bytes",
    "rss_bytes",
    "spool_bytes",
    "retained_wal_bytes",
    "active_tables",
    "quarantined_tables",
    "snapshot_groups",
    "schema_events",
    "table_transitions",
    "reconciliation_runs",
    "reconciliation_log_rows",
}
with csv_path.open(encoding="utf-8", newline="") as source:
    reader = csv.DictReader(source)
    missing_headers = required_sample_fields.difference(reader.fieldnames or ())
    samples = list(reader)

sample_failures = []
if missing_headers:
    sample_failures.append("csv_headers")
if len(samples) != result.get("sample_count"):
    sample_failures.append("csv_sample_count")

previous_elapsed = -1
for index, sample in enumerate(samples, start=1):
    if any(sample.get(field, "") == "" for field in required_sample_fields):
        sample_failures.append(f"csv_sample_{index}_complete")
        continue
    try:
        elapsed = int(sample["elapsed_ms"])
        for field in required_sample_fields - {"timestamp_unix_ms", "elapsed_ms"}:
            int(sample[field])
    except ValueError:
        sample_failures.append(f"csv_sample_{index}_numeric")
        continue
    if elapsed <= previous_elapsed:
        sample_failures.append(f"csv_sample_{index}_elapsed")
    previous_elapsed = elapsed

final_sample = samples[-1] if samples else {}
try:
    csv_final_source_rows = int(final_sample.get("source_rows", ""))
    csv_final_target_rows = int(final_sample.get("target_rows", ""))
    csv_final_spool_bytes = int(final_sample.get("spool_bytes", ""))
except ValueError:
    csv_final_source_rows = None
    csv_final_target_rows = None
    csv_final_spool_bytes = None

source_target_equal = result.get("source_target_equal")
if source_target_equal is None:
    source_target_equal = (
        csv_final_source_rows is not None
        and csv_final_source_rows == csv_final_target_rows == result.get("final_rows")
    )
spool_drained = result.get("spool_drained")
if spool_drained is None:
    spool_drained = csv_final_spool_bytes == 0

checks = {
    "duration_seconds": isinstance(result.get("duration_seconds"), (int, float))
    and result["duration_seconds"] >= minimum_duration,
    "sample_count": isinstance(result.get("sample_count"), (int, float))
    and result["sample_count"] > 0,
    "sample_query_errors": result.get("sample_query_errors") == 0,
    "csv_samples": not sample_failures,
    "source_target_equal": source_target_equal is True,
    "spool_drained": spool_drained is True,
    "lag_p95_bytes": isinstance(result.get("lag_p95_bytes"), (int, float))
    and result["lag_p95_bytes"] >= 0,
    "lag_p99_bytes": isinstance(result.get("lag_p99_bytes"), (int, float))
    and result["lag_p99_bytes"] >= 0,
    "max_rss_bytes": isinstance(result.get("max_rss_bytes"), (int, float))
    and result["max_rss_bytes"] > 0,
    "max_retained_wal_bytes": isinstance(
        result.get("max_retained_wal_bytes"), (int, float)
    )
    and result["max_retained_wal_bytes"] >= 0,
    "reconciliation_corruption_repaired": result.get(
        "reconciliation_corruption_repaired"
    )
    is True,
    "reconciliation_recovery_seconds": isinstance(
        result.get("reconciliation_recovery_seconds"), (int, float)
    )
    and result["reconciliation_recovery_seconds"] >= 0,
    "active_tables": isinstance(
        result.get("metadata_end", {}).get("active_tables"), (int, float)
    )
    and result["metadata_end"]["active_tables"] >= 1,
}
failed = [name for name, passed in checks.items() if not passed]
failed.extend(sample_failures)
if failed:
    print(
        f"soak evidence failed the production gate ({', '.join(failed)}): {path}",
        file=sys.stderr,
    )
    raise SystemExit(1)

summary_fields = [
    "duration_seconds",
    "sample_count",
    "sample_query_errors",
    "lag_p95_bytes",
    "lag_p99_bytes",
    "max_rss_bytes",
    "max_spool_bytes",
    "max_retained_wal_bytes",
    "reconciliation_recovery_seconds",
    "metadata_delta",
]
print(json.dumps({field: result.get(field) for field in summary_fields}, separators=(",", ":")))
PY
