#!/bin/bash
# Continuous insert load generator for mobile game telemetry test data.
# Usage: load.sh <container_name> <rows_per_batch> <num_batches> [sleep_seconds]
set -euo pipefail

NODE="${1:?Usage: load.sh <container> <rows_per_batch> <num_batches> [sleep_seconds]}"
ROWS="${2:-2000}"
BATCHES="${3:-10}"
SLEEP="${4:-1}"

for i in $(seq 1 "$BATCHES"); do
  docker exec "$NODE" clickhouse-client --query "
  INSERT INTO testgame.events (app_id,event_type,user_id,event_time,session_id,device_os,country,revenue_usd,props)
  SELECT
    'game1',
    ['level_up','purchase','session_start','ad_watched','level_fail'][1+number%5],
    number % 100000,
    now() - INTERVAL (number % 10) SECOND,
    generateUUIDv4(),
    ['ios','android'][1+number%2],
    ['US','DE','FR','BR','JP','KR'][1+number%6],
    if(number%10=0, round(number%200 + 0.99, 2), NULL),
    map('client_version', '1.2.3', 'ab_group', ['A','B'][1+number%2])
  FROM numbers($ROWS)"
  echo "batch $i/$BATCHES inserted ($ROWS rows) into $NODE"
  sleep "$SLEEP"
done
