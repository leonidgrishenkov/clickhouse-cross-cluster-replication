#!/bin/bash
# Atomic FETCH + ATTACH cycle for S7. Must be run as a single unit -- if this
# script dies between FETCH and ATTACH, the next run will fail with
# PARTITION_ALREADY_EXISTS. This script's cleanup trap handles that case.
set -euo pipefail

PARTITION_ID="${1:?Usage: fetch_partition_cycle.sh <partition_id>}"
ZK_PATH="main_keeper:/clickhouse/tables/1/testgame/events"
EXT_NODE="${EXT_NODE:-s7-ch-ext}"

cleanup_stale_detached() {
  docker exec "$EXT_NODE" clickhouse-client --query \
    "ALTER TABLE testgame.events DROP DETACHED PARTITION '$PARTITION_ID' SETTINGS allow_drop_detached=1" \
    2>/dev/null || true
}

# Clean up any part left over from a previous interrupted cycle before fetching.
cleanup_stale_detached

docker exec "$EXT_NODE" clickhouse-client --query \
  "ALTER TABLE testgame.events FETCH PARTITION '$PARTITION_ID' FROM '$ZK_PATH'"

docker exec "$EXT_NODE" clickhouse-client --query \
  "ALTER TABLE testgame.events ATTACH PARTITION '$PARTITION_ID'"

echo "Partition $PARTITION_ID refreshed."
