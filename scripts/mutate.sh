#!/bin/bash
# Apply mutations/DDL/TTL operations for chaos/propagation testing.
# Usage: mutate.sh <container> <operation>
# operations: update | delete | optimize_final | add_column | drop_partition <partition_id>
set -euo pipefail

NODE="${1:?Usage: mutate.sh <container> <update|delete|optimize_final|add_column|drop_partition> [partition_id]}"
OP="${2:?}"

case "$OP" in
  update)
    docker exec "$NODE" clickhouse-client --query "ALTER TABLE testgame.events UPDATE country='CA' WHERE user_id % 1000 = 0"
    ;;
  delete)
    docker exec "$NODE" clickhouse-client --query "ALTER TABLE testgame.events DELETE WHERE event_type='ad_watched' AND user_id % 500 = 0"
    ;;
  optimize_final)
    docker exec "$NODE" clickhouse-client --query "OPTIMIZE TABLE testgame.events FINAL"
    ;;
  add_column)
    COLNAME="added_$(date +%s)"
    docker exec "$NODE" clickhouse-client --query "ALTER TABLE testgame.events ADD COLUMN $COLNAME String DEFAULT ''"
    echo "added column: $COLNAME"
    ;;
  drop_partition)
    PART="${3:?drop_partition requires a partition id argument}"
    docker exec "$NODE" clickhouse-client --query "ALTER TABLE testgame.events DROP PARTITION '$PART'"
    ;;
  *)
    echo "Unknown operation: $OP" >&2
    exit 1
    ;;
esac

echo "mutation '$OP' issued on $NODE"
