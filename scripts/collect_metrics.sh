#!/bin/bash
# Dump key system.* tables to CSV for a given container, per the task's
# "dumping system.* tables to CSV is enough" instruction.
# Usage: collect_metrics.sh <container> <output_dir>
set -euo pipefail

NODE="${1:?Usage: collect_metrics.sh <container> <output_dir>}"
OUTDIR="${2:?}"
TS=$(date +%Y%m%d_%H%M%S)

mkdir -p "$OUTDIR"

docker exec "$NODE" clickhouse-client --query "SELECT * FROM system.replicas FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_replicas_${TS}.csv" 2>/dev/null || true

docker exec "$NODE" clickhouse-client --query "SELECT * FROM system.replication_queue FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_replication_queue_${TS}.csv" 2>/dev/null || true

docker exec "$NODE" clickhouse-client --query "SELECT * FROM system.mutations FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_mutations_${TS}.csv" 2>/dev/null || true

docker exec "$NODE" clickhouse-client --query "SELECT * FROM system.distribution_queue FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_distribution_queue_${TS}.csv" 2>/dev/null || true

docker exec "$NODE" clickhouse-client --query "SELECT * FROM system.backup_log FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_backup_log_${TS}.csv" 2>/dev/null || true

docker exec "$NODE" clickhouse-client --query "
SELECT metric, value FROM system.metrics FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_metrics_${TS}.csv" 2>/dev/null || true

docker exec "$NODE" clickhouse-client --query "
SELECT event, value FROM system.events FORMAT CSVWithNames" \
  > "$OUTDIR/${NODE}_events_${TS}.csv" 2>/dev/null || true

echo "Metrics collected for $NODE at $TS -> $OUTDIR"
