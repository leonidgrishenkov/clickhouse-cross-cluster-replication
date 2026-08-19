#!/bin/bash
set -euo pipefail
docker exec s7-ch-ext clickhouse-client --multiquery --query "
CREATE DATABASE IF NOT EXISTS testgame ENGINE=Atomic;
CREATE TABLE IF NOT EXISTS testgame.events
(
    app_id LowCardinality(String), event_type LowCardinality(String), user_id UInt64,
    event_time DateTime, session_id UUID, device_os LowCardinality(String),
    country LowCardinality(String), revenue_usd Nullable(Decimal(10,2)),
    props Map(String, String), ingested_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/testgame/events', '{replica}')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (app_id, event_type, user_id, event_time)
TTL event_time + INTERVAL 90 DAY;
"
docker exec s7-ch-main clickhouse-client --multiquery --query "
CREATE DATABASE IF NOT EXISTS testgame ENGINE=Atomic;
CREATE TABLE IF NOT EXISTS testgame.events
(
    app_id LowCardinality(String), event_type LowCardinality(String), user_id UInt64,
    event_time DateTime, session_id UUID, device_os LowCardinality(String),
    country LowCardinality(String), revenue_usd Nullable(Decimal(10,2)),
    props Map(String, String), ingested_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/testgame/events', '{replica}')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (app_id, event_type, user_id, event_time)
TTL event_time + INTERVAL 90 DAY;
"
echo "== S7 bootstrap complete. Use scripts/fetch_partition_cycle.sh <partition_id> to pull. =="
