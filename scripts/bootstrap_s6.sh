#!/bin/bash
# Bootstrap S6: create schema on ch-main only. RESTORE happens explicitly
# as part of the test (not automated here) since the whole point of this
# scenario is to exercise BACKUP/RESTORE as the propagation mechanism.
set -euo pipefail

MAIN_NODE=s6-ch-main

docker exec "$MAIN_NODE" clickhouse-client --multiquery --query "
CREATE DATABASE IF NOT EXISTS testgame ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS testgame.events
(
    app_id            LowCardinality(String),
    event_type        LowCardinality(String),
    user_id           UInt64,
    event_time        DateTime,
    session_id        UUID,
    device_os         LowCardinality(String),
    country           LowCardinality(String),
    revenue_usd       Nullable(Decimal(10,2)),
    props             Map(String, String),
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/testgame/events', '{replica}')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (app_id, event_type, user_id, event_time)
TTL event_time + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS testgame.users_profile
(
    user_id           UInt64,
    app_id            LowCardinality(String),
    first_seen        DateTime,
    last_seen         DateTime,
    country           LowCardinality(String),
    total_revenue_usd Decimal(12,2),
    level             UInt32,
    version           UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/testgame/users_profile', '{replica}', version)
ORDER BY (app_id, user_id);
"

docker exec s6-mc mc alias set localminio http://minio:9000 minioadmin minioadmin123 2>&1 || true
docker exec s6-mc mc mb localminio/ch-backups 2>&1 || true

echo "== S6 bootstrap complete. Use BACKUP/RESTORE queries manually per report.md recipes. =="
