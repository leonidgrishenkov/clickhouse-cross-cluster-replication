#!/bin/bash
# Chaos operations, applied per-container. See report.md/LOG.md for the exact
# interactive test transcripts these formalize.
# Usage: chaos.sh <stop|start|net_disconnect|net_reconnect|query_bomb> <container> [network_name] [query]
set -euo pipefail

OP="${1:?Usage: chaos.sh <stop|start|net_disconnect|net_reconnect|query_bomb> <container> [network] [query]}"
NODE="${2:?}"

case "$OP" in
  stop)
    docker stop "$NODE"
    ;;
  start)
    docker start "$NODE"
    sleep 8
    docker exec "$NODE" clickhouse-client --query "SELECT 1"
    ;;
  net_disconnect)
    NET="${3:?net_disconnect requires a network name}"
    docker network disconnect "$NET" "$NODE"
    ;;
  net_reconnect)
    NET="${3:?net_reconnect requires a network name}"
    docker network connect "$NET" "$NODE"
    sleep 5
    ;;
  query_bomb)
    QUERY="${3:-SELECT count() FROM testgame.events}"
    N="${4:-8}"
    for i in $(seq 1 "$N"); do
      docker exec "$NODE" clickhouse-client --query "$QUERY" > "/tmp/chaos_bomb_${NODE}_${i}.log" 2>&1 &
    done
    wait
    echo "query bomb complete: $N concurrent queries against $NODE"
    ;;
  *)
    echo "Unknown chaos operation: $OP" >&2
    exit 1
    ;;
esac
