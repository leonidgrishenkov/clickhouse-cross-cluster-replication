-- ============================================================
-- Cross-cluster replicated + distributed table setup and test
-- Run against: s1 topology (main_cluster: s1-ch-main-*, ext_cluster: s1-ch-ext-*)
-- ============================================================

-- --- Create databases on each cluster ---

CREATE DATABASE IF NOT EXISTS db ON CLUSTER main_cluster;

CREATE DATABASE IF NOT EXISTS db ON CLUSTER ext_cluster;

-- --- main_cluster: replicated local table + distributed table ---
-- run on any main_cluster node, e.g. s1-ch-main-s1r1

CREATE TABLE db.events_local ON CLUSTER main_cluster
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db/events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);

CREATE TABLE db.events ON CLUSTER main_cluster AS db.events_local
ENGINE = Distributed(main_cluster, db, events_local, rand());

-- --- ext_cluster: replicated local table ---
-- run on any ext_cluster node, e.g. s1-ch-ext-s1r1

CREATE TABLE db.events_local ON CLUSTER ext_cluster
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db/events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);

-- ext_cluster distributed table: ON CLUSTER hit MEMORY_LIMIT_EXCEEDED on these
-- small (700MB) containers, so it was created per-node instead:

-- run on s1-ch-ext-s1r1:
CREATE TABLE db.events AS db.events_local
ENGINE = Distributed(ext_cluster, db, events_local, rand());

-- run on s1-ch-ext-s2r1:
CREATE TABLE db.events AS db.events_local
ENGINE = Distributed(ext_cluster, db, events_local, rand());

-- --- Generate + insert 1000 test rows ---

-- main_cluster: run on s1-ch-main-s1r1
INSERT INTO db.events
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    ['click', 'view', 'purchase', 'signup', 'logout'][(rand() % 5) + 1] AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- ext_cluster: run on s1-ch-ext-s2r1 (s1-ch-ext-s1r1 was near its memory limit)
INSERT INTO db.events
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    ['click', 'view', 'purchase', 'signup', 'logout'][(rand() % 5) + 1] AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- --- Verification queries ---

SELECT count() FROM db.events; -- run on any main_cluster node
SELECT count() FROM db.events_local;      -- run per main_cluster node (s1r1, s1r2, s2r1, s2r2)

SELECT count() FROM db.events; -- run on any ext_cluster node
SELECT count() FROM db.events_local;      -- run per ext_cluster node (s1r1, s2r1)

-- ============================================================
-- Test scenario: insert data into the local replicated table on
-- a single ext_cluster node only (bypassing Distributed), to
-- observe shard/replica isolation.
-- ============================================================

-- run only on s1-ch-ext-s1r1:
INSERT INTO db.events_local
SELECT
    toDate('2026-08-20') AS event_date,
    now() AS event_time,
    999999 AS user_id,
    'test_local_insert' AS event_type,
    42.0 AS value
FROM numbers(5);

-- verification: run on s1-ch-ext-s1r1 and s1-ch-ext-s2r1
SELECT count() FROM db.events_local WHERE event_type = 'test_local_insert';
SELECT count() FROM db.events_local;
