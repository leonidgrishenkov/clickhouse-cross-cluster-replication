#!/bin/bash
# Bootstrap S3: create schema on both main and ext, plus the push MV + Distributed
# table on main. See report.md for the POPULATE/backfill caveats before using
# this in anything beyond a fresh empty table.
set -euo pipefail

docker exec s3-ch-ext clickhouse-client --multiquery --query "
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
"

docker exec s3-ch-main clickhouse-client --multiquery --query "
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

CREATE TABLE IF NOT EXISTS testgame.events_push_dist AS testgame.events
ENGINE = Distributed(ext_push_cluster, testgame, events, rand())
SETTINGS bytes_to_throw_insert = 2000000000, bytes_to_delay_insert = 500000000, max_delay_to_insert = 5;

CREATE MATERIALIZED VIEW IF NOT EXISTS testgame.events_push_mv TO testgame.events_push_dist AS
SELECT * FROM testgame.events;
"

echo "== S3 bootstrap complete. Production-realistic bytes_to_throw_insert/delay thresholds set (2GB/500MB); tune down for testing chaos scenarios. =="
