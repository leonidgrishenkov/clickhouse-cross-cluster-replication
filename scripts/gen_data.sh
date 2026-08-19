#!/bin/bash
# Backfill historical partitions for testing the S1/S3/S6/S7 backfill gap behavior.
# Usage: gen_data.sh <container> <num_days_back> <rows_per_day>
set -euo pipefail

NODE="${1:?Usage: gen_data.sh <container> <num_days_back> <rows_per_day>}"
DAYS="${2:-7}"
ROWS_PER_DAY="${3:-50000}"

for d in $(seq 0 "$DAYS"); do
  docker exec "$NODE" clickhouse-client --query "
  INSERT INTO testgame.events (app_id,event_type,user_id,event_time,session_id,device_os,country,revenue_usd,props)
  SELECT
    'game1',
    ['level_up','purchase','session_start'][1+number%3],
    number % 50000,
    now() - INTERVAL $d DAY - INTERVAL (number % 86400) SECOND,
    generateUUIDv4(),
    ['ios','android'][1+number%2],
    ['US','DE','FR','BR'][1+number%4],
    if(number%10=0, round(number%100 + 0.99, 2), NULL),
    map('k','v')
  FROM numbers($ROWS_PER_DAY)"
  echo "backfilled day -$d ($ROWS_PER_DAY rows) into $NODE"
done
