#!/bin/bash
# Bootstrap S9 (anti-pattern baseline): schema + Distributed table only.
set -euo pipefail

docker exec s9-ch-main clickhouse-client --multiquery --query "
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

docker exec s9-ch-ext clickhouse-client --multiquery --query "
CREATE DATABASE IF NOT EXISTS testgame ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS testgame.events_dist
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
ENGINE = Distributed(main_from_ext, testgame, events, rand());
"

echo "== S9 bootstrap complete. ext has ZERO local data by design. =="
