-- ============================================================
-- PULL via Refreshable Materialized View
-- The external node reaches into the main cluster on a schedule under a
-- read-only user and tops itself up. No shared Keeper, no shared ZK paths,
-- no replication: the only coupling is one TCP connection, opened by ext.
--
-- Stand: s1, ClickHouse 26.3.17.110
--   main_cluster : ch-main-s1r1 + s1r2 (shard 1), ch-main-s2r1 + s2r2 (shard 2)
--   ext          : ch-ext-s1r1, a standalone node (plain MergeTree, no Keeper use)
-- The sandbox has no TLS configured, so remote()/port 9000 is used everywhere
-- remoteSecure()/9440 would be used in production. Nothing else changes.
-- ============================================================


-- ============================================================
-- PHASE 0 — environment
-- ============================================================

-- ch-ext-s1r1
SELECT version();                                   -- 26.3.17.110
SELECT name, value, default FROM system.settings WHERE name LIKE '%refreshable%';
-- allow_experimental_refreshable_materialized_view  1  1
-- => in 26.3 the feature is on by default; the SET ... = 1 from the 23.12-24.x
--    era is no longer needed.

-- the one hole in the firewall this pattern needs: ext -> main tcp/9000
-- from inside the ext container:
--   clickhouse-client --host ch-main-s1r1 --port 9000 -q "SELECT hostName(), version()"
--   ch-main-s1r1  26.3.17.110
-- (no iptables rules were active in DOCKER-USER during this run)


-- ============================================================
-- PHASE 1 — the source side (main cluster)
-- ============================================================

-- ch-main-s1r1
CREATE TABLE default.events_local ON CLUSTER main_cluster
(
    ts          DateTime,                   -- business time
    user_id     UInt64,
    event_type  LowCardinality(String),
    value       Float64,
    ingested_at DateTime DEFAULT now()      -- insert time: the monotone key
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default/events', '{replica}')
PARTITION BY toDate(ts)
ORDER BY (ts, user_id);

CREATE TABLE default.events_distributed ON CLUSTER main_cluster AS default.events_local
ENGINE = Distributed(main_cluster, default, events_local, rand());

-- a small reference table, replicated to all 4 main nodes (one shared path,
-- replica name '{shard}_{replica}' so the two shards do not collide)
CREATE TABLE default.dim_users ON CLUSTER main_cluster
(
    user_id    UInt64,
    country    LowCardinality(String),
    level      UInt32,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/dim_users/all', '{shard}_{replica}')
ORDER BY user_id;

-- the read-only export user
CREATE USER ro_export_user ON CLUSTER main_cluster
IDENTIFIED WITH sha256_password BY 'ro_export_pw_change_me'
SETTINGS readonly = 1;

GRANT SELECT ON default.events_distributed TO ro_export_user ON CLUSTER main_cluster;
GRANT SELECT ON default.events_local       TO ro_export_user ON CLUSTER main_cluster;
GRANT SELECT ON default.dim_users          TO ro_export_user ON CLUSTER main_cluster;
-- readonly = 1 turned out to be enough for everything below, including the
-- pull through the Distributed table.

-- seed: 5000 events, ingested 10..60 minutes ago; 100 dim rows
INSERT INTO default.events_distributed (ts, user_id, event_type, value, ingested_at)
SELECT
    now() - toIntervalSecond(rand() % 259200),
    (rand() % 1000) + 1,
    ['click','view','purchase','signup','logout'][(rand() % 5) + 1],
    round(randCanonical() * 100, 2),
    now() - toIntervalSecond(600 + (rand() % 3000))
FROM numbers(5000);

INSERT INTO default.dim_users (user_id, country, level, updated_at)
SELECT number + 1, ['RU','DE','US','FR'][(number % 4) + 1], (number % 10) + 1, now() - toIntervalSecond(3600)
FROM numbers(100);

SELECT count() FROM default.events_distributed;   -- 5000
SELECT count() FROM default.dim_users;            -- 100


-- ============================================================
-- PHASE 2 — how to address the source: comma vs pipe
--           The snippet's 'ch-main-1:9440,ch-main-2:9440' over a Distributed
--           table doubles every row.
-- ============================================================

-- ch-ext-s1r1
SELECT count() FROM remote('ch-main-s1r1:9000',
        default.events_distributed, 'ro_export_user', 'ro_export_pw_change_me');
-- 5000   correct

SELECT count() FROM remote('ch-main-s1r1:9000,ch-main-s2r1:9000',
        default.events_distributed, 'ro_export_user', 'ro_export_pw_change_me');
-- 10000  WRONG: a comma declares two SHARDS, so events_distributed (which already
--        fans out to every shard) is read twice, once per host.

SELECT count() FROM remote('ch-main-s1r1:9000|ch-main-s1r2:9000',
        default.events_distributed, 'ro_export_user', 'ro_export_pw_change_me');
-- 5000   correct: a pipe declares REPLICAS of one shard -> one read + failover

SELECT count() FROM remote('ch-main-s1r1:9000,ch-main-s2r1:9000',
        default.events_local, 'ro_export_user', 'ro_export_pw_change_me');
-- 5000   correct too, but only because events_local is the per-shard table:
--        one host per shard, each contributing its own data.


-- ============================================================
-- PHASE 3 — the incremental append view, twice:
--           once literally as in the snippet (>= watermark) and once fixed (>)
-- ============================================================

-- ch-ext-s1r1
CREATE DATABASE IF NOT EXISTS ext;

CREATE TABLE ext.events
(
    ts          DateTime,
    user_id     UInt64,
    event_type  LowCardinality(String),
    value       Float64,
    ingested_at DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (ts, user_id);

CREATE TABLE ext.events_fixed AS ext.events;

-- as in the snippet: >= watermark
CREATE MATERIALIZED VIEW ext.events_sync
REFRESH EVERY 10 SECOND APPEND
TO ext.events
AS
SELECT ts, user_id, event_type, value, ingested_at
FROM remote('ch-main-s1r1:9000|ch-main-s1r2:9000',
            default.events_distributed,
            'ro_export_user', 'ro_export_pw_change_me')
WHERE ingested_at >= (SELECT ifNull(max(ingested_at), toDateTime(0)) FROM ext.events)
  AND ingested_at <  now() - INTERVAL 30 SECOND;

-- corrected: > watermark
CREATE MATERIALIZED VIEW ext.events_sync_fixed
REFRESH EVERY 10 SECOND APPEND
TO ext.events_fixed
AS
SELECT ts, user_id, event_type, value, ingested_at
FROM remote('ch-main-s1r1:9000|ch-main-s1r2:9000',
            default.events_distributed,
            'ro_export_user', 'ro_export_pw_change_me')
WHERE ingested_at >  (SELECT ifNull(max(ingested_at), toDateTime(0)) FROM ext.events_fixed)
  AND ingested_at <  now() - INTERVAL 30 SECOND;
-- REFRESH EVERY 10 SECOND is accepted; refreshes are aligned to wall clock
-- (:00, :10, :20 ...), not to the creation time.

-- after ~3 refreshes
SELECT count(), uniqExact((ts,user_id,ingested_at,value)) FROM ext.events;        -- 5024 / 5000
SELECT count(), uniqExact((ts,user_id,ingested_at,value)) FROM ext.events_fixed;  -- 5000 / 5000
-- and it keeps growing, +N rows per refresh where N = number of rows sitting
-- exactly on the watermark second:
--   t+12s  events=5028  events_fixed=5000
--   t+24s  events=5036  events_fixed=5000
--   t+36s  events=5040  events_fixed=5000
-- Divergence after ~9 minutes:      ext.events = 30468, ext.events_fixed = 7000
-- and 2.5 minutes later:             ext.events = 37968, ext.events_fixed = 7000
-- (a 500-row batch shared one ingested_at second and was re-imported on every
--  refresh). >= is not a "safety margin", it is an unbounded duplicate source.
-- SYSTEM STOP VIEW ext.events_sync;   -- status becomes 'Disabled' in
--                                     -- system.view_refreshes; left stopped so
--                                     -- the table does not grow without bound.

SELECT view, status, last_success_time, last_success_duration_ms,
       next_refresh_time, retry, read_rows, written_rows, exception
FROM system.view_refreshes ORDER BY view;
-- events_sync        Scheduled  ...  106 ms  retry=0  read_rows=10024  written_rows=4
-- events_sync_fixed  Scheduled  ...   91 ms  retry=0  read_rows=10000  written_rows=0


-- ============================================================
-- PHASE 4 — lag guard, incremental pickup, late arrivals
-- ============================================================

-- ch-main-s1r1 — three batches at once
INSERT INTO default.events_distributed (ts, user_id, event_type, value, ingested_at)
SELECT now() - toIntervalSecond(rand() % 3600), (rand() % 1000) + 1, 'batch2_ok', 1.0, now() - toIntervalSecond(120)
FROM numbers(1000);          -- older than the lag guard, newer than the watermark

INSERT INTO default.events_distributed (ts, user_id, event_type, value, ingested_at)
SELECT now() - toIntervalSecond(rand() % 3600), (rand() % 1000) + 1, 'batch3_fresh', 2.0, now()
FROM numbers(500);           -- inside the 30s lag window

INSERT INTO default.events_distributed (ts, user_id, event_type, value, ingested_at)
SELECT now() - toIntervalSecond(rand() % 3600), (rand() % 1000) + 1, 'batch4_late', 3.0, now() - toIntervalSecond(3600)
FROM numbers(200);           -- a LATE arrival: written now, stamped an hour ago

-- ch-ext-s1r1, t+15s (one refresh later)
SELECT event_type, count() FROM ext.events_fixed GROUP BY event_type ORDER BY event_type;
-- batch2_ok 1000 | click 993 | logout 988 | purchase 1029 | signup 948 | view 1042
-- => batch2 is in, batch3 is held back by the lag guard, batch4 is missing

-- t+45s (lag window has passed)
-- batch2_ok 1000 | batch3_fresh 500 | click 993 | ... | view 1042
-- => batch3 arrived. batch4_late NEVER arrives: its ingested_at is below the
--    watermark, so the WHERE never selects it again. 200 rows lost silently.
SELECT max(ingested_at) FROM ext.events_fixed;   -- jumped to batch3's second


-- ============================================================
-- PHASE 5 — recovering the lost tail with REPLACE PARTITION
-- ============================================================

-- ch-ext-s1r1 — stop the view first: REPLACE PARTITION can move the watermark
-- backwards, and a refresh racing with it would re-import a whole window.
SYSTEM STOP VIEW ext.events_sync_fixed;

CREATE TABLE IF NOT EXISTS ext.events_tail AS ext.events_fixed;

INSERT INTO ext.events_tail
SELECT ts, user_id, event_type, value, ingested_at
FROM remote('ch-main-s1r1:9000|ch-main-s1r2:9000',
            default.events_distributed, 'ro_export_user', 'ro_export_pw_change_me')
WHERE toDate(ts) = toDate('2026-08-23');
-- 3243 rows

ALTER TABLE ext.events_fixed REPLACE PARTITION '2026-08-23' FROM ext.events_tail;

SELECT event_type, count() FROM ext.events_fixed GROUP BY event_type ORDER BY event_type;
-- batch2_ok 1000 | batch3_fresh 500 | batch4_late 200 | click 993 | ... | view 1042
SELECT count() FROM ext.events_fixed;               -- 6700
-- main at the same moment:                            6700  (exact match)
SELECT max(ingested_at) FROM ext.events_fixed;      -- unchanged, watermark preserved

SYSTEM START VIEW ext.events_sync_fixed;


-- ============================================================
-- PHASE 6 — full reload for a reference table (atomic swap)
-- ============================================================

-- ch-ext-s1r1
CREATE MATERIALIZED VIEW ext.dim_users_sync
REFRESH EVERY 10 SECOND
ENGINE = MergeTree ORDER BY user_id
AS SELECT user_id, country, level, updated_at
FROM remote('ch-main-s1r1:9000|ch-main-s1r2:9000',
            default.dim_users, 'ro_export_user', 'ro_export_pw_change_me');

SELECT count(), sum(level) FROM ext.dim_users_sync;     -- 100 / 550

SELECT name, engine, uuid FROM system.tables WHERE database = 'ext' ORDER BY name;
-- .inner_id.66b31243-...   MergeTree        9d5aede2-...
-- dim_users_sync           MaterializedView 66b31243-...
-- => the data lives in an inner table; each refresh builds a new one and swaps.
--    The inner uuid changes on every refresh: 9d5aede2 -> 635cf47e -> ae2ca1ad

-- ch-main-s1r1 — mutate the source
ALTER TABLE default.dim_users UPDATE level = 999, updated_at = now()
WHERE user_id = 1 SETTINGS mutations_sync = 2;
-- Code: 36. The source storage is replicated so ALTER UPDATE/ALTER DELETE
-- statements must use only deterministic functions. Function 'now' is
-- non-deterministic. (BAD_ARGUMENTS)

ALTER TABLE default.dim_users UPDATE level = 999, updated_at = toDateTime('2026-08-23 22:00:00')
WHERE user_id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE default.dim_users DELETE WHERE user_id = 2 SETTINGS mutations_sync = 2;
SELECT count(), sum(level) FROM default.dim_users;      -- 99 / 1546

-- ch-ext-s1r1, one refresh later
SELECT count(), sum(level) FROM ext.dim_users_sync;     -- 99 / 1546
SELECT user_id, level FROM ext.dim_users_sync WHERE user_id = 1;   -- 1 999
-- => full reload carries UPDATEs and DELETEs. That is the whole point of it.


-- ============================================================
-- PHASE 7 — what APPEND mode does NOT carry
-- ============================================================

-- ch-main-s1r1
ALTER TABLE default.events_local ON CLUSTER main_cluster
DELETE WHERE event_type = 'batch2_ok' SETTINGS mutations_sync = 2;

SELECT count() FROM default.events_distributed;                             -- 5700
SELECT count() FROM default.events_distributed WHERE event_type='batch2_ok';-- 0

-- ch-ext-s1r1, several refreshes later
SELECT count() FROM ext.events_fixed;                                       -- 6700
SELECT count() FROM ext.events_fixed WHERE event_type = 'batch2_ok';        -- 1000
-- => deletes and updates on the source are invisible to an APPEND view.
--    Remedies: ReplacingMergeTree + version on the ext side, or the
--    REPLACE PARTITION rebuild from PHASE 5.


-- ============================================================
-- PHASE 8 — how much of the source does every refresh read?
-- ============================================================

-- ch-main-s1r1 — what the source actually receives from the ext view
SYSTEM FLUSH LOGS;
SELECT event_time, user, read_rows, substring(query, 1, 300)
FROM system.query_log
WHERE type = 'QueryFinish' AND user = 'ro_export_user' AND is_initial_query
ORDER BY event_time DESC LIMIT 1;
-- SELECT `__table1`.`ts` ... FROM `default`.`events_distributed` AS `__table1`
-- WHERE (`__table1`.`ingested_at` > _CAST('2026-08-23 21:46:06', 'Nullable(DateTime)')) ...
-- => the scalar subquery over the LOCAL ext table is evaluated on ext first and
--    inlined as a literal, so the filter really is pushed to the source. Good.
--    Every refresh also issues a DESC TABLE.

-- but: ingested_at is not in the primary key, so the filter prunes nothing
SELECT read_rows, substring(query, position(query, 'WHERE'), 80) AS w
FROM system.query_log
WHERE type = 'QueryFinish' AND user = 'ro_export_user' AND position(query, 'WHERE') > 0
ORDER BY event_time DESC LIMIT 6;
-- 6000  WHERE `ingested_at` > '2026-08-23 22:00:51'        <- full scan, 300 rows returned
-- 1431  WHERE `ts`          > '2026-08-23 22:00:51'        <- PK pruning, 116 rows returned
-- 2543  WHERE `ingested_at` > '...' AND `ts` >= '...' - INTERVAL 1 HOUR  <- 300 rows returned

-- => a watermark on a non-indexed column means a full scan of the source table
--    on every refresh, forever, every 10 seconds. Pair the watermark with a
--    coarse PK/partition bound (the third form) or put the watermark column in
--    the sorting key.


-- ============================================================
-- PHASE 9 — failure modes
-- ============================================================

-- ch-ext-s1r1 — a wrong password is caught at CREATE time, not at refresh time
CREATE MATERIALIZED VIEW ext.broken_sync
REFRESH EVERY 10 SECOND
ENGINE = MergeTree ORDER BY user_id
AS SELECT user_id, country, level, updated_at
FROM remote('ch-main-s1r1:9000', default.dim_users, 'ro_export_user', 'wrong_password');
-- Code: 516. Authentication failed: password is incorrect, or there is no user
-- with such name. (AUTHENTICATION_FAILED)  -- the view is NOT created

-- runtime failure: pull the grant out from under a running view
-- ch-main-s1r1
REVOKE SELECT ON default.dim_users FROM ro_export_user ON CLUSTER main_cluster;

-- ch-ext-s1r1, ~20s later
SELECT view, status, retry, last_success_time, next_refresh_time, exception
FROM system.view_refreshes WHERE view = 'dim_users_sync';
-- status=Scheduled  retry=3  exception=Code: 497 ... Not enough privileges ...
SELECT count(), sum(level) FROM ext.dim_users_sync;     -- 99 / 1546
-- => readers keep seeing the last good snapshot; a failed refresh does not
--    empty or lock the table.

-- ch-main-s1r1
GRANT SELECT ON default.dim_users TO ro_export_user ON CLUSTER main_cluster;
-- ~25s later: retry=0, exception empty, last_success_time moving again.
-- No operator action needed.

-- replica failover inside remote(): stop the first host in the list
-- host: docker stop s1-ch-main-s1r1
-- ch-main-s2r1
INSERT INTO default.events_distributed (ts, user_id, event_type, value, ingested_at)
SELECT now() - toIntervalSecond(rand() % 600), (rand() % 1000) + 1, 'failover_batch', 4.0, now() - toIntervalSecond(60)
FROM numbers(300);
-- ch-ext-s1r1, 25s later
SELECT count() FROM ext.events_fixed WHERE event_type = 'failover_batch';   -- 300
SELECT view, status, retry, exception FROM system.view_refreshes WHERE view='events_sync_fixed';
-- Scheduled, retry=0, no exception -> 'a|b' failed over to ch-main-s1r2 silently
-- host: docker start s1-ch-main-s1r1

-- ext node restart: do the views come back?
-- host: docker restart s1-ch-ext-s1r1
SELECT view, status, last_success_time, retry FROM system.view_refreshes ORDER BY view;
-- all three Scheduled, retry=0, refreshing again on their own; no duplicates,
-- no gap. Server setting stop_refreshable_materialized_views_on_startup = 0
-- controls this.


-- ============================================================
-- PHASE 10 — credentials in the view definition
-- ============================================================

-- ch-ext-s1r1
SHOW CREATE TABLE ext.events_sync_fixed;
-- ... FROM remote('ch-main-s1r1:9000|ch-main-s1r2:9000', default.events_distributed,
--                 'ro_export_user', '[HIDDEN]')
SELECT count() FROM system.query_log WHERE query LIKE '%ro_export_pw_change_me%';   -- 0

-- but on disk:
-- $ grep -o "ro_export_user', '[^']*'" /var/lib/clickhouse/metadata/ext/events_sync_fixed.sql
-- ro_export_user', 'ro_export_pw_change_me'
-- => SHOW CREATE and query_log mask it, the metadata file does not.

-- the fix: a named collection
CREATE NAMED COLLECTION main_src AS
    host = 'ch-main-s1r1:9000',
    user = 'ro_export_user',
    password = 'ro_export_pw_change_me';

SELECT count() FROM remote(main_src, database = 'default', table = 'events_distributed');  -- 6000
SELECT collection FROM system.named_collections WHERE name = 'main_src';
-- {'host':'[HIDDEN]','password':'[HIDDEN]','user':'[HIDDEN]'}

CREATE MATERIALIZED VIEW ext.dim_users_sync_nc
REFRESH EVERY 1 MINUTE
ENGINE = MergeTree ORDER BY user_id
AS SELECT user_id, country, level, updated_at
FROM remote(main_src, database = 'default', table = 'dim_users');
-- $ grep -c 'ro_export_pw_change_me' /var/lib/clickhouse/metadata/ext/dim_users_sync_nc.sql
-- 0    <- no secret in the view's metadata any more


-- ============================================================
-- PHASE 11 — the push direction (main -> ext)
-- ============================================================

-- ch-ext-s1r1
CREATE TABLE ext.events_push AS ext.events_fixed;
CREATE USER ingest_user IDENTIFIED WITH sha256_password BY 'ingest_pw_change_me';
GRANT INSERT ON ext.events_push TO ingest_user;     -- INSERT only, no SELECT

-- ch-main-s1r1
INSERT INTO FUNCTION remote('ch-ext-s1r1:9000', ext.events_push, 'ingest_user', 'ingest_pw_change_me')
SELECT ts, user_id, event_type, value, ingested_at
FROM default.events_distributed
WHERE ingested_at >= now() - INTERVAL 10 MINUTE;

SELECT count() FROM ext.events_push;                                    -- 800 (ext)
SELECT count() FROM default.events_distributed
WHERE ingested_at >= now() - INTERVAL 10 MINUTE;                        -- 800 (main)

-- and the ingest user really cannot read:
SELECT count() FROM remote('ch-ext-s1r1:9000', ext.events_push, 'ingest_user', 'ingest_pw_change_me');
-- Code: 497. ingest_user: Not enough privileges. To execute this query, it's
-- necessary to have the grant SELECT for at least one column on ext.events_push.
-- => the window bookkeeping (which range was already sent, what to do after a
--    failure) now lives on the main side; nothing on ext tracks it.


-- ============================================================
-- SUMMARY
-- ============================================================
-- 1. The pattern works exactly as advertised on 26.3.17.110: a standalone ext
--    node, no Keeper in common, no replicated tables, one outbound connection,
--    a read-only user with readonly = 1 on the source. Refreshes every 10s cost
--    ~100 ms here.
-- 2. Two bugs in the snippet as written:
--      a) 'host1,host2' over a Distributed table doubles every row (comma =
--         shards). Use 'host1|host2' (replicas, with failover) against the
--         Distributed table, or comma against the per-shard local tables.
--      b) `ts >= (SELECT max(ts) ...)` re-imports every row sitting on the
--         watermark on EVERY refresh. Measured: 30468 rows instead of 7000.
--         Use `>` and deduplicate by another means if you need at-least-once.
-- 3. The lag guard works, and late arrivals below the watermark are lost
--    silently — exactly as the note predicted. The REPLACE PARTITION rebuild
--    recovers them and restores an exact match with the source; stop the view
--    while doing it.
-- 4. Full reload (no APPEND) carries UPDATEs and DELETEs, swaps a whole inner
--    table atomically, and keeps serving the previous snapshot while a refresh
--    fails. Good for reference tables of this size.
-- 5. The watermark filter IS pushed to the source as a literal, but a watermark
--    on a non-indexed column makes every refresh a full scan (6000 of 6000 rows
--    to fetch 300). Add a coarse PK/partition bound to the same WHERE.
-- 6. Failure handling is decent out of the box: bad credentials fail at CREATE,
--    runtime failures are retried with backoff and recorded in
--    system.view_refreshes, readers keep the last good data, replica failover
--    inside remote() is transparent, and views resume after a node restart.
-- 7. The source password is written in plaintext into the view's metadata file
--    even though SHOW CREATE and query_log mask it. Use a named collection.
-- 8. The push direction works with an INSERT-only user, but moves all the
--    "which window did I already send" bookkeeping to the main cluster.
