#!/bin/bash
# Bootstrap S1: create schema on main cluster, then add ext nodes as additional
# replicas of the SAME table path (not part of remote_servers on main side).
#
# Recipe used (the "robust" path, recommended in report.md):
#   ReplicatedMergeTree with EXPLICIT zoo_path/replica_name arguments.
#   This sidesteps the {uuid} pitfall entirely because the replica path is
#   fully controlled by the DDL text, not derived from the table's Atomic-DB
#   UUID. The {uuid}-based pitfall (bare `ReplicatedMergeTree` with no args,
#   which uses default_replica_path=/clickhouse/tables/{uuid}/{shard}) is
#   tested SEPARATELY in scripts/uuid_pitfall_demo.sh and documented as its
#   own experiment in report.md, because it requires either:
#     (a) CREATE TABLE ... UUID '<uuid-copied-from-main>' on each ext replica, or
#     (b) ON CLUSTER over a cluster definition that names both groups (breaks
#         the "ext nodes absent from main remote_servers" isolation property
#         for the duration of that one DDL statement).
set -euo pipefail

MAIN_NODE=s1-ch-main-s1r1
EXT_S1=s1-ch-ext-s1r1
EXT_S2=s1-ch-ext-s2r1

echo "== Creating database + tables on main cluster (ON CLUSTER main_cluster) =="
docker exec "$MAIN_NODE" clickhouse-client --multiquery --query "
CREATE DATABASE IF NOT EXISTS testgame ON CLUSTER main_cluster ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS testgame.events ON CLUSTER main_cluster
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

CREATE TABLE IF NOT EXISTS testgame.users_profile ON CLUSTER main_cluster
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

echo "== Adding ext nodes as ADDITIONAL REPLICAS of the same shard paths =="
echo "   (ext_s1r1 joins shard 1 path; ext_s2r1 joins shard 2 path)"
echo "   ext nodes are NOT in main_cluster's remote_servers -- they only know"
echo "   the same Keeper znode path via matching zoo_path + their own macros."

docker exec "$EXT_S1" clickhouse-client --multiquery --query "
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

docker exec "$EXT_S2" clickhouse-client --multiquery --query "
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

echo "== Verifying replica membership from system.replicas on main and ext =="
docker exec "$MAIN_NODE" clickhouse-client --query "
SELECT database, table, replica_name, is_leader, is_readonly
FROM system.replicas WHERE database='testgame' ORDER BY table, replica_name"

docker exec "$EXT_S1" clickhouse-client --query "
SELECT database, table, replica_name, is_leader, is_readonly
FROM system.replicas WHERE database='testgame' ORDER BY table, replica_name"

echo "== Bootstrap complete =="
