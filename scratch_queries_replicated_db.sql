-- ============================================================
-- Cross-cluster replication with ENGINE = Replicated databases
-- Topology: s1 (main_cluster: 2 shards x 2 replicas, ext_cluster: 2 shards x 1 replica)
--           shared Keeper ensemble, separate distributed_ddl queues
--           macros: {shard} = 1|2 ; {replica} = main_r1|main_r2|ext_r1
--
-- Question under test: what happens to cross-cluster replication when the
-- ZooKeeper path is NOT given explicitly to ReplicatedMergeTree, i.e. when it
-- falls back to the default `/clickhouse/tables/{uuid}/{shard}`.
--
-- All queries below are in the exact order they were executed.
-- Node each query ran on is noted above it; observed output is in comments.
-- ============================================================


-- ============================================================
-- PHASE 0 — inspect and drop the leftovers of the previous run
-- ============================================================

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1
SHOW DATABASES;
-- both: INFORMATION_SCHEMA, db, default, information_schema, system, testgame

-- s1-ch-main-s1r1
SELECT name, engine, uuid FROM system.tables WHERE database = 'db';
-- events        Distributed          48a3d66b-839a-407e-9d2f-8ae641c5f10b
-- events_local  ReplicatedMergeTree  5fafb8a0-a162-49ba-acc2-45239d0c962f

-- s1-ch-ext-s1r1
SELECT name, engine, uuid FROM system.tables WHERE database = 'db';
-- events        Distributed          84fe545b-df24-4c90-b1ad-b6a6368f5e5a
-- events_local  ReplicatedMergeTree  553d24a5-c468-4256-b4f6-dccabfb541a6
--
-- NOTE: the two clusters already disagree on the table UUID (5fafb8a0 vs
-- 553d24a5). Those tables replicated across clusters only because their ZK
-- path was written out explicitly and contained no {uuid}.

-- s1-ch-main-s1r1
DROP DATABASE IF EXISTS db ON CLUSTER main_cluster SYNC;

-- s1-ch-ext-s1r1
DROP DATABASE IF EXISTS db ON CLUSTER ext_cluster SYNC;

-- s1-ch-main-s1r1 — confirm ZK is clean
SELECT name FROM system.zookeeper WHERE path = '/clickhouse';
-- task_queue, sessions, tables
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables';
-- 1, 2
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/1';
-- db
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/2';
-- db
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/1/db';
-- (empty — DROP ... SYNC removed the replica subtree, only empty nodes remain)
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/2/db';
-- (empty)


-- ============================================================
-- PHASE 1 — one INDEPENDENT Replicated database per cluster
--           (different ZK paths) + ReplicatedMergeTree with NO arguments
--           Expectation: UUIDs diverge -> no cross-cluster replication
-- ============================================================

-- s1-ch-main-s1r1
CREATE DATABASE db_repl ON CLUSTER main_cluster
ENGINE = Replicated('/clickhouse/databases/db_repl_main', '{shard}', '{replica}');
-- OK on all 4 main nodes; no experimental flag needed (v26.3.17.110)

-- s1-ch-ext-s1r1
CREATE DATABASE db_repl ON CLUSTER ext_cluster
ENGINE = Replicated('/clickhouse/databases/db_repl_ext', '{shard}', '{replica}');
-- OK on both ext nodes

-- s1-ch-main-s1r1 — note: no ON CLUSTER, no engine arguments.
-- The Replicated database propagates the DDL through its own queue.
CREATE TABLE db_repl.events_local
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);
-- 1 main_r1 OK / 1 main_r2 OK / 2 main_r1 OK / 2 main_r2 OK

-- s1-ch-ext-s1r1
CREATE TABLE db_repl.events_local
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);
-- 1 ext_r1 OK / 2 ext_r1 OK

-- run on every node — which path did the default produce?
SELECT uuid, zookeeper_path, replica_name
FROM system.replicas
WHERE database = 'db_repl' AND table = 'events_local';
-- s1-ch-main-s1r1  cf3b8876-...  /clickhouse/tables/cf3b8876-f2eb-46dd-8c44-8d98e66b178e/1  main_r1
-- s1-ch-main-s1r2  cf3b8876-...  /clickhouse/tables/cf3b8876-f2eb-46dd-8c44-8d98e66b178e/1  main_r2
-- s1-ch-main-s2r1  cf3b8876-...  /clickhouse/tables/cf3b8876-f2eb-46dd-8c44-8d98e66b178e/2  main_r1
-- s1-ch-main-s2r2  cf3b8876-...  /clickhouse/tables/cf3b8876-f2eb-46dd-8c44-8d98e66b178e/2  main_r2
-- s1-ch-ext-s1r1   ad90359b-...  /clickhouse/tables/ad90359b-7d74-43f5-86db-0f17302ab893/1  ext_r1
-- s1-ch-ext-s2r1   ad90359b-...  /clickhouse/tables/ad90359b-7d74-43f5-86db-0f17302ab893/2  ext_r1
--
-- => default path is /clickhouse/tables/{uuid}/{shard} with replica = {replica}.
--    The UUID is shared inside ONE Replicated database, but the two databases
--    are independent, so the two clusters landed on different ZK subtrees.

-- s1-ch-main-s1r1
CREATE TABLE db_repl.events AS db_repl.events_local
ENGINE = Distributed(main_cluster, db_repl, events_local, rand());

-- s1-ch-ext-s1r1
CREATE TABLE db_repl.events AS db_repl.events_local
ENGINE = Distributed(ext_cluster, db_repl, events_local, rand());

-- s1-ch-main-s1r1 — insert into the MAIN cluster
INSERT INTO db_repl.events
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    ['click', 'view', 'purchase', 'signup', 'logout'][(rand() % 5) + 1] AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- s1-ch-ext-s1r1 — insert into the EXT cluster
INSERT INTO db_repl.events
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    ['click', 'view', 'purchase', 'signup', 'logout'][(rand() % 5) + 1] AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- run on every node
SELECT count() FROM db_repl.events_local;
-- s1-ch-main-s1r1  488   \ shard 1 of main: in-cluster replication OK
-- s1-ch-main-s1r2  488   /
-- s1-ch-main-s2r1  512   \ shard 2 of main
-- s1-ch-main-s2r2  512   /
-- s1-ch-ext-s1r1   534   <- ext's own data only
-- s1-ch-ext-s2r1   466   <- ext's own data only

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1
SELECT count() FROM db_repl.events;
-- main dist = 1000 , ext dist = 1000
-- => RESULT: each cluster sees only the 1000 rows it inserted itself.
--    NO cross-cluster replication.

-- s1-ch-main-s1r1 — the two replica sets are disjoint
SELECT path, name FROM system.zookeeper
WHERE path IN (
  '/clickhouse/tables/cf3b8876-f2eb-46dd-8c44-8d98e66b178e/1/replicas',
  '/clickhouse/tables/cf3b8876-f2eb-46dd-8c44-8d98e66b178e/2/replicas',
  '/clickhouse/tables/ad90359b-7d74-43f5-86db-0f17302ab893/1/replicas',
  '/clickhouse/tables/ad90359b-7d74-43f5-86db-0f17302ab893/2/replicas'
) ORDER BY path, name;
-- .../ad90359b-.../1/replicas  ext_r1
-- .../ad90359b-.../2/replicas  ext_r1
-- .../cf3b8876-.../1/replicas  main_r1
-- .../cf3b8876-.../1/replicas  main_r2
-- .../cf3b8876-.../2/replicas  main_r1
-- .../cf3b8876-.../2/replicas  main_r2


-- ============================================================
-- PHASE 2 — ONE Replicated database SHARED by both clusters
--           (identical ZK path on all 6 nodes), engine args still omitted
--           Expectation: one UUID for everybody -> replication works
-- ============================================================

-- s1-ch-main-s1r1
CREATE DATABASE db_xrepl ON CLUSTER main_cluster
ENGINE = Replicated('/clickhouse/databases/db_xrepl', '{shard}', '{replica}');

-- s1-ch-ext-s1r1 — SAME ZK path: the ext nodes join the existing database
CREATE DATABASE db_xrepl ON CLUSTER ext_cluster
ENGINE = Replicated('/clickhouse/databases/db_xrepl', '{shard}', '{replica}');

-- s1-ch-main-s1r1
SELECT name FROM system.zookeeper
WHERE path = '/clickhouse/databases/db_xrepl/replicas' ORDER BY name;
-- 1|ext_r1 , 1|main_r1 , 1|main_r2 , 2|ext_r1 , 2|main_r1 , 2|main_r2
-- => all 6 nodes of BOTH clusters are replicas of a single database.
--    Replica identity is the (shard, replica) macro pair, which is unique here.

-- s1-ch-main-s1r1 ONLY — one statement, no ON CLUSTER, no engine arguments
CREATE TABLE db_xrepl.events_local
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);
-- 1 main_r1 OK / 1 main_r2 OK / 1 ext_r1 OK / 2 ext_r1 OK / 2 main_r1 OK / 2 main_r2 OK
-- => a single DDL from a main node created the table on the ext cluster too.

-- run on every node
SELECT zookeeper_path, replica_name, total_replicas
FROM system.replicas WHERE database = 'db_xrepl';
-- s1-ch-main-s1r1  /clickhouse/tables/48ce21f2-105d-4bf7-a7e3-ad5a9e51e8f8/1  main_r1  3
-- s1-ch-main-s1r2  /clickhouse/tables/48ce21f2-105d-4bf7-a7e3-ad5a9e51e8f8/1  main_r2  3
-- s1-ch-main-s2r1  /clickhouse/tables/48ce21f2-105d-4bf7-a7e3-ad5a9e51e8f8/2  main_r1  3
-- s1-ch-main-s2r2  /clickhouse/tables/48ce21f2-105d-4bf7-a7e3-ad5a9e51e8f8/2  main_r2  3
-- s1-ch-ext-s1r1   /clickhouse/tables/48ce21f2-105d-4bf7-a7e3-ad5a9e51e8f8/1  ext_r1   3
-- s1-ch-ext-s2r1   /clickhouse/tables/48ce21f2-105d-4bf7-a7e3-ad5a9e51e8f8/2  ext_r1   3
-- => same {uuid} everywhere; each shard is now a 3-replica set spanning
--    both clusters (main_r1 + main_r2 + ext_r1).

-- Distributed tables are kept OUTSIDE the Replicated database on purpose:
-- anything created inside db_xrepl is propagated to the ext nodes, which do
-- not have main_cluster in their remote_servers (see the sub-test at the end).

-- s1-ch-main-s1r1
CREATE TABLE default.events_dist ON CLUSTER main_cluster AS db_xrepl.events_local
ENGINE = Distributed(main_cluster, db_xrepl, events_local, rand());

-- s1-ch-ext-s1r1
CREATE TABLE default.events_dist ON CLUSTER ext_cluster AS db_xrepl.events_local
ENGINE = Distributed(ext_cluster, db_xrepl, events_local, rand());

-- s1-ch-main-s1r1 — 1000 rows tagged 'from_main'
INSERT INTO default.events_dist
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    'from_main' AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- s1-ch-ext-s1r1 — 1000 rows tagged 'from_ext'
INSERT INTO default.events_dist
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    'from_ext' AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- run on every node
SYSTEM SYNC REPLICA db_xrepl.events_local;
SELECT count() AS total,
       countIf(event_type = 'from_main') AS from_main,
       countIf(event_type = 'from_ext')  AS from_ext
FROM db_xrepl.events_local;
--                    total  from_main  from_ext
-- s1-ch-main-s1r1     1011        502       509   \
-- s1-ch-main-s1r2     1011        502       509    > shard 1: identical on all
-- s1-ch-ext-s1r1      1011        502       509   /  three replicas, both clusters
-- s1-ch-main-s2r1      989        498       491   \
-- s1-ch-main-s2r2      989        498       491    > shard 2
-- s1-ch-ext-s2r1       989        498       491   /

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1
SELECT count() FROM default.events_dist;
-- main dist = 2000 , ext dist = 2000
-- => RESULT: bidirectional cross-cluster replication WITHOUT an explicit
--    zookeeper path.

-- --- targeted test: write to the local table on ONE ext node only ---

-- s1-ch-ext-s1r1
INSERT INTO db_xrepl.events_local
SELECT
    toDate('2026-08-20') AS event_date,
    now() AS event_time,
    999999 AS user_id,
    'test_local_insert_ext' AS event_type,
    42.0 AS value
FROM numbers(5);

-- run on every node
SELECT count() FROM db_xrepl.events_local WHERE event_type = 'test_local_insert_ext';
-- s1-ch-ext-s1r1   5   <- written here
-- s1-ch-main-s1r1  5   <- replicated to the other cluster, same shard
-- s1-ch-main-s1r2  5
-- s1-ch-main-s2r1  0   <- different shard, correctly untouched
-- s1-ch-ext-s2r1   0

-- --- reverse DDL propagation: ALTER issued from an ext node ---

-- s1-ch-ext-s2r1
ALTER TABLE db_xrepl.events_local ADD COLUMN source_note String DEFAULT 'added_from_ext_s2r1';
-- OK on all 6 nodes (1|main_r1, 1|main_r2, 1|ext_r1, 2|main_r1, 2|main_r2, 2|ext_r1)

-- run on several nodes
SELECT name FROM system.columns
WHERE database = 'db_xrepl' AND table = 'events_local' AND name = 'source_note';
-- s1-ch-main-s1r1  source_note
-- s1-ch-main-s2r2  source_note
-- s1-ch-ext-s1r1   source_note
-- => schema changes flow ext -> main as well. The two clusters are fully
--    coupled through the shared database DDL queue.

-- --- sub-test: a Distributed table INSIDE the shared Replicated database ---

-- s1-ch-main-s1r1
CREATE TABLE db_xrepl.events_dist_inside AS db_xrepl.events_local
ENGINE = Distributed(main_cluster, db_xrepl, events_local, rand());
-- OK on all 6 nodes — CREATE does NOT validate that the cluster name exists

-- s1-ch-main-s1r1
SELECT count() FROM db_xrepl.events_dist_inside;
-- 2005  (2000 + the 5 rows of the targeted insert, both on shard 1)

-- s1-ch-ext-s1r1
SELECT count() FROM db_xrepl.events_dist_inside;
-- Code: 701. DB::Exception: Requested cluster 'main_cluster' not found. (CLUSTER_DOESNT_EXIST)
-- => the table is created everywhere but is broken on the ext side.
--    Keep Distributed tables in a local (Atomic) database, or define the same
--    cluster names on both sides.

-- run on every node — the database DDL queue survived the broken table
SELECT replica_path, log_ptr, max_log_ptr, is_readonly
FROM system.database_replicas WHERE database = 'db_xrepl';
-- s1-ch-main-s1r1  /clickhouse/databases/db_xrepl/replicas/1|main_r1  9  9  0
-- s1-ch-main-s1r2  /clickhouse/databases/db_xrepl/replicas/1|main_r2  9  9  0
-- s1-ch-main-s2r1  /clickhouse/databases/db_xrepl/replicas/2|main_r1  9  9  0
-- s1-ch-main-s2r2  /clickhouse/databases/db_xrepl/replicas/2|main_r2  9  9  0
-- s1-ch-ext-s1r1   /clickhouse/databases/db_xrepl/replicas/1|ext_r1   9  9  0
-- s1-ch-ext-s2r1   /clickhouse/databases/db_xrepl/replicas/2|ext_r1   9  9  0
-- => all 6 database replicas in sync, none read-only.


-- ============================================================
-- PHASE 3 — independent Replicated databases (Phase 1 layout) but with an
--           EXPLICIT shared ZK path, to see whether that route is still open
-- ============================================================

-- s1-ch-main-s1r1
CREATE TABLE db_repl.events_shared
(
    event_date Date,
    user_id UInt32,
    event_type String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_repl/events_shared', '{replica}')
ORDER BY (event_date, user_id);
-- Code: 36. DB::Exception: It's not allowed to specify explicit zookeeper_path
-- and replica_name for ReplicatedMergeTree arguments in Replicated database.
-- If you really want to specify them explicitly, enable setting
-- database_replicated_allow_replicated_engine_arguments. (BAD_ARGUMENTS)

-- s1-ch-main-s1r1
SELECT name, value, description FROM system.settings
WHERE name = 'database_replicated_allow_replicated_engine_arguments';
-- value: 0
-- 0 - Don't allow to explicitly specify ZooKeeper path and replica name for
--     *MergeTree tables in Replicated databases. 1 - Allow. 2 - Allow, but
--     ignore the specified path and use default one instead. 3 - Allow and
--     don't log a warning.

-- s1-ch-main-s1r1 , with --database_replicated_allow_replicated_engine_arguments=1
CREATE TABLE db_repl.events_shared
(
    event_date Date,
    user_id UInt32,
    event_type String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_repl/events_shared', '{replica}')
ORDER BY (event_date, user_id);
-- 1 main_r1 OK / 1 main_r2 OK / 2 main_r1 OK / 2 main_r2 OK

-- s1-ch-ext-s1r1 , same setting, same explicit path
CREATE TABLE db_repl.events_shared
(
    event_date Date,
    user_id UInt32,
    event_type String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_repl/events_shared', '{replica}')
ORDER BY (event_date, user_id);
-- 1 ext_r1 OK / 2 ext_r1 OK

-- s1-ch-main-s1r1
INSERT INTO db_repl.events_shared VALUES ('2026-08-01', 1, 'from_main_local');

-- s1-ch-ext-s1r1
INSERT INTO db_repl.events_shared VALUES ('2026-08-02', 2, 'from_ext_local');

-- run on several nodes
SELECT groupArray(event_type) FROM db_repl.events_shared;
SELECT total_replicas FROM system.replicas
WHERE database = 'db_repl' AND table = 'events_shared';
-- s1-ch-main-s1r1  ['from_ext_local','from_main_local']  replicas=3
-- s1-ch-main-s1r2  ['from_ext_local','from_main_local']  replicas=3
-- s1-ch-ext-s1r1   ['from_ext_local','from_main_local']  replicas=3
-- s1-ch-main-s2r1  []                                    replicas=3
-- s1-ch-ext-s2r1   []                                    replicas=3
-- => an explicit shared path still works inside Replicated databases, but it
--    is off by default and has to be unlocked per query/profile.


-- ============================================================
-- PHASE 4 — where exactly do the macros in the default path come from?
--           Follow-up to phase 1: the fix is to give the DATABASE the same ZK
--           path on both clusters (that is what phase 2 did). These probes
--           pin down what else has to line up for that to work.
-- ============================================================

-- --- 4a: same database ZK path, but DIFFERENT shard arguments ---

-- s1-ch-main-s1r1 — shard identity 1, 2
CREATE DATABASE db_y ON CLUSTER main_cluster
ENGINE = Replicated('/clickhouse/databases/db_y', '{shard}', '{replica}');

-- s1-ch-ext-s1r1 — SAME database path, shard identity ext_1, ext_2
CREATE DATABASE db_y ON CLUSTER ext_cluster
ENGINE = Replicated('/clickhouse/databases/db_y', 'ext_{shard}', '{replica}');

-- s1-ch-main-s1r1
SELECT name FROM system.zookeeper
WHERE path = '/clickhouse/databases/db_y/replicas' ORDER BY name;
-- 1|main_r1 , 1|main_r2 , 2|main_r1 , 2|main_r2 , ext_1|ext_r1 , ext_2|ext_r1
-- => the database arguments name the DATABASE replica.

-- s1-ch-main-s1r1
CREATE TABLE db_y.t (event_date Date, user_id UInt32, event_type String)
ENGINE = ReplicatedMergeTree
ORDER BY (event_date, user_id);

-- run on every node
SELECT zookeeper_path, replica_name, total_replicas FROM system.replicas WHERE database = 'db_y';
-- s1-ch-main-s1r1  /clickhouse/tables/ab77a523-.../1  main_r1  3
-- s1-ch-main-s1r2  /clickhouse/tables/ab77a523-.../1  main_r2  3
-- s1-ch-main-s2r1  /clickhouse/tables/ab77a523-.../2  main_r1  3
-- s1-ch-ext-s1r1   /clickhouse/tables/ab77a523-.../1  ext_r1   3
-- s1-ch-ext-s2r1   /clickhouse/tables/ab77a523-.../2  ext_r1   3
-- => the ext nodes resolved to /1 and /2, NOT /ext_1 and /ext_2. The {shard}
--    in the table path came from the SERVER macro, not from the database
--    argument. Replica sets still line up across clusters.

-- s1-ch-main-s1r1
INSERT INTO db_y.t VALUES ('2026-08-01', 1, 'from_main');
-- s1-ch-ext-s1r1
INSERT INTO db_y.t VALUES ('2026-08-02', 2, 'from_ext');

-- run on several nodes
SELECT groupArray(event_type) FROM db_y.t;
-- s1-ch-main-s1r1  ['from_ext','from_main']
-- s1-ch-main-s1r2  ['from_ext','from_main']
-- s1-ch-ext-s1r1   ['from_ext','from_main']
-- s1-ch-main-s2r1  []
-- s1-ch-ext-s2r1   []
-- => cross-cluster replication works even with mismatched database shard
--    arguments, because the table path follows the server macros.

-- --- 4b: confirm the same for the replica name ---

-- s1-ch-main-s1r1
CREATE DATABASE db_z ON CLUSTER main_cluster
ENGINE = Replicated('/clickhouse/databases/db_z', '{shard}', '{replica}');

-- s1-ch-ext-s1r1 — literal replica name, deliberately != server macro (ext_r1)
CREATE DATABASE db_z ON CLUSTER ext_cluster
ENGINE = Replicated('/clickhouse/databases/db_z', '{shard}', 'zzz_r1');

-- s1-ch-main-s1r1
CREATE TABLE db_z.t (event_date Date, user_id UInt32, event_type String)
ENGINE = ReplicatedMergeTree ORDER BY (event_date, user_id);
-- DDL acknowledged as: 1 main_r1 / 1 main_r2 / 1 zzz_r1 / 2 zzz_r1 / 2 main_r1 / 2 main_r2

-- run on several nodes
SELECT zookeeper_path, replica_name, total_replicas FROM system.replicas WHERE database = 'db_z';
-- s1-ch-main-s1r1  /clickhouse/tables/5703fe79-.../1  main_r1  3
-- s1-ch-ext-s1r1   /clickhouse/tables/5703fe79-.../1  ext_r1   3
-- s1-ch-ext-s2r1   /clickhouse/tables/5703fe79-.../2  ext_r1   3
-- => database replica identity is 1|zzz_r1 (database argument) while the
--    table replica_name is ext_r1 (server macro). The two namespaces are
--    independent.

-- --- 4c: what breaks if the database replica identities collide ---

-- s1-ch-main-s1r1
CREATE DATABASE db_w ON CLUSTER main_cluster
ENGINE = Replicated('/clickhouse/databases/db_w', '{shard}', '{replica}');

-- s1-ch-ext-s1r1 — replica name collides with the main nodes
CREATE DATABASE db_w ON CLUSTER ext_cluster
ENGINE = Replicated('/clickhouse/databases/db_w', '{shard}', 'main_r1');
-- Code: 253. DB::Exception: Replica main_r1 of shard 1 of replicated database
-- at /clickhouse/databases/db_w already exists.
--   Replica host ID: 'ch%2Dmain%2Ds1r1:9000:c7df587c-...',
--   current host ID: 'ch%2Dext%2Ds1r1:9000:5945d687-...'. (REPLICA_ALREADY_EXISTS)
-- => sharing the database path requires every node's (shard, replica) pair to
--    be globally unique. In this topology it is (main_r1/main_r2 vs ext_r1).

-- --- cleanup of the phase 4 probes ---

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1, for db_y, db_z, db_w
DROP DATABASE IF EXISTS db_y ON CLUSTER main_cluster SYNC;
DROP DATABASE IF EXISTS db_y ON CLUSTER ext_cluster SYNC;
DROP DATABASE IF EXISTS db_z ON CLUSTER main_cluster SYNC;
DROP DATABASE IF EXISTS db_z ON CLUSTER ext_cluster SYNC;
DROP DATABASE IF EXISTS db_w ON CLUSTER main_cluster SYNC;
DROP DATABASE IF EXISTS db_w ON CLUSTER ext_cluster SYNC;

-- s1-ch-main-s1r1
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/databases' ORDER BY name;
-- db_repl_ext , db_repl_main , db_xrepl


-- ============================================================
-- PHASE 5 — is ENGINE = Replicated required on BOTH sides?
--           No. Replication is decided by the table's ZK path alone, so the
--           two clusters may use different database engines.
--           main = ordinary Atomic database, ext = Replicated database,
--           one and the same explicit table path.
--           Same events schema / same Distributed layout as scratch_queries.sql.
-- ============================================================

-- s1-ch-main-s1r1 — default engine (Atomic)
CREATE DATABASE db_mix ON CLUSTER main_cluster;

-- s1-ch-main-s1r1
CREATE TABLE db_mix.events_local ON CLUSTER main_cluster
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_mix/events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);

-- s1-ch-main-s1r1
CREATE TABLE db_mix.events ON CLUSTER main_cluster AS db_mix.events_local
ENGINE = Distributed(main_cluster, db_mix, events_local, rand());

-- s1-ch-ext-s1r1
CREATE DATABASE db_mix ON CLUSTER ext_cluster
ENGINE = Replicated('/clickhouse/databases/db_mix_ext', '{shard}', '{replica}');

-- s1-ch-ext-s1r1 , default settings -> rejected (same as Phase 3)
CREATE TABLE db_mix.events_local
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_mix/events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);
-- Code: 36. DB::Exception: It's not allowed to specify explicit zookeeper_path
-- and replica_name for ReplicatedMergeTree arguments in Replicated database.
-- If you really want to specify them explicitly, enable setting
-- database_replicated_allow_replicated_engine_arguments. (BAD_ARGUMENTS)

-- s1-ch-ext-s1r1 , with --database_replicated_allow_replicated_engine_arguments=1
-- no ON CLUSTER needed: the Replicated database propagates the DDL itself
CREATE TABLE db_mix.events_local
(
    event_date Date,
    event_time DateTime,
    user_id UInt32,
    event_type String,
    value Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_mix/events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);
-- 1 ext_r1 OK , 2 ext_r1 OK

-- s1-ch-ext-s1r1
CREATE TABLE db_mix.events AS db_mix.events_local
ENGINE = Distributed(ext_cluster, db_mix, events_local, rand());
-- 1 ext_r1 OK , 2 ext_r1 OK
-- (Distributed over ext_cluster is safe here: every replica of THIS database
--  is an ext node, so the cluster name resolves everywhere it is created —
--  unlike the shared database of Phase 2.)

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1
SELECT engine FROM system.databases WHERE name = 'db_mix';
-- main: Atomic , ext: Replicated

-- --- 1000 rows from each cluster, through its own Distributed table ---

-- s1-ch-main-s1r1
INSERT INTO db_mix.events
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    'from_main_atomic_db' AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- s1-ch-ext-s1r1
INSERT INTO db_mix.events
SELECT
    toDate('2026-08-01') + toIntervalDay(rand() % 20) AS event_date,
    now() - toIntervalSecond(rand() % 100000) AS event_time,
    (rand() % 5000) + 1 AS user_id,
    'from_ext_replicated_db' AS event_type,
    round(randCanonical() * 1000, 2) AS value
FROM numbers(1000);

-- run on every node
SELECT count(),
       countIf(event_type = 'from_main_atomic_db'),
       countIf(event_type = 'from_ext_replicated_db')
FROM db_mix.events_local;
--                     total  from_main  from_ext
-- s1-ch-main-s1r1     1038   518        520     shard 1
-- s1-ch-main-s1r2     1038   518        520     shard 1
-- s1-ch-ext-s1r1      1038   518        520     shard 1
-- s1-ch-main-s2r1      962   482        480     shard 2
-- s1-ch-main-s2r2      962   482        480     shard 2
-- s1-ch-ext-s2r1       962   482        480     shard 2
-- => every shard holds both clusters' rows; the two engines make no difference.

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1
SELECT count() FROM db_mix.events;
-- main: 2000 , ext: 2000

-- --- targeted local insert on one ext node (same test as scratch_queries.sql) ---

-- s1-ch-ext-s1r1 only
INSERT INTO db_mix.events_local
SELECT
    toDate('2026-08-20') AS event_date,
    now() AS event_time,
    999999 AS user_id,
    'test_local_insert' AS event_type,
    42.0 AS value
FROM numbers(5);

-- run on every node
SELECT count() FROM db_mix.events_local WHERE event_type = 'test_local_insert';
-- s1-ch-main-s1r1 5 , s1-ch-main-s1r2 5 , s1-ch-ext-s1r1 5
-- s1-ch-main-s2r1 0 , s1-ch-main-s2r2 0 , s1-ch-ext-s2r1 0
-- => the rows stayed inside shard 1 and crossed to the main cluster there.

-- s1-ch-main-s1r1
SELECT name FROM system.zookeeper
WHERE path = '/clickhouse/tables/1/db_mix/events/replicas' ORDER BY name;
-- ext_r1 , main_r1 , main_r2
SELECT name FROM system.zookeeper
WHERE path = '/clickhouse/tables/2/db_mix/events/replicas' ORDER BY name;
-- ext_r1 , main_r1 , main_r2

-- s1-ch-main-s1r1 / s1-ch-ext-s1r1
SELECT zookeeper_path, replica_name, total_replicas, active_replicas
FROM system.replicas WHERE database = 'db_mix';
-- main: /clickhouse/tables/1/db_mix/events  main_r1  3  3
-- ext:  /clickhouse/tables/1/db_mix/events  ext_r1   3  3
-- => one replica set per shard spanning both clusters, with different database
--    engines on either side.

-- --- how far does DDL travel in this mixed layout? ---

-- s1-ch-ext-s1r1 (Replicated database)
ALTER TABLE db_mix.events_local ADD COLUMN source_note String DEFAULT 'added_from_ext_s1r1';
-- 1 ext_r1 OK , 2 ext_r1 OK

-- run on every node
SELECT count() FROM system.columns
WHERE database = 'db_mix' AND table = 'events_local' AND name = 'source_note';
-- 1 on all 6 nodes
SELECT count() FROM system.columns
WHERE database = 'db_mix' AND table = 'events' AND name = 'source_note';
-- 0 on all 6 nodes
-- => the ALTER reached both ext nodes through the Replicated database queue and
--    then both main nodes of each shard through the ReplicatedMergeTree log —
--    metadata ALTERs of a replicated table replicate along the table's ZK path,
--    exactly like data. The Distributed wrapper is a separate, non-replicated
--    table and has to be altered per cluster by hand.

-- run on every node
SELECT count() FROM db_mix.events_local;
-- shard 1: 1043 , shard 2: 962
SELECT count() FROM db_mix.events;   -- main: 2005 , ext: 2005

-- cleanup — NOT executed this run, db_mix left in place for inspection
-- DROP DATABASE IF EXISTS db_mix ON CLUSTER main_cluster SYNC;   -- s1-ch-main-s1r1
-- DROP DATABASE IF EXISTS db_mix ON CLUSTER ext_cluster SYNC;    -- s1-ch-ext-s1r1


-- ============================================================
-- SUMMARY
-- ============================================================
-- 1. ENGINE = Replicated resolves ReplicatedMergeTree without arguments to
--    /clickhouse/tables/{uuid}/{shard}, replica name {replica}. The UUID is
--    assigned by the database, so it is identical for every replica OF THAT
--    DATABASE and different for any other database.
-- 2. Two independent Replicated databases (one per cluster) => two UUIDs =>
--    two disjoint replica sets => no cross-cluster replication, even though
--    both clusters share the same Keeper ensemble. (Phase 1)
-- 3. One Replicated database whose ZK path is shared by both clusters makes
--    the default {uuid} path work: each shard becomes a replica set spanning
--    both clusters, inserts flow both ways, and DDL propagates in both
--    directions from a single statement. (Phase 2)
-- 4. Cost of (3): the clusters are coupled. Any DDL on either side hits all 6
--    nodes, and objects that depend on node-local config (Distributed over a
--    cluster name defined on one side only) are created everywhere but fail
--    on the other side at query time.
-- 5. Explicit ZK paths are still available inside Replicated databases, but
--    are rejected by default and need
--    database_replicated_allow_replicated_engine_arguments = 1. (Phase 3)
-- 6. Two independent macro namespaces, easy to confuse (Phase 4):
--      - the Replicated database's (shard, replica) ARGUMENTS name the
--        database replica in /clickhouse/databases/<path>/replicas/<shard>|<replica>
--        and must be globally unique across every node joining that path;
--      - the table's default path /clickhouse/tables/{uuid}/{shard} and its
--        replica name expand from the SERVER macros in config.d, regardless of
--        what the database arguments say.
--    So the thing that actually has to match across clusters for data to
--    replicate is: the database ZK path (-> shared {uuid}) plus the server
--    {shard} macro (-> same replica set). Verified on 26.3.17.110.
-- 7. ENGINE = Replicated is NOT a requirement for cross-cluster replication,
--    on either side (Phase 5). What replicates data is the ReplicatedMergeTree
--    ZK path; the database engine only decides how that path is derived and
--    how DDL is propagated. Working combinations:
--      a) ordinary databases + explicit shared path, DDL run once per cluster
--         (this is what scratch_queries.sql does);
--      b) one Replicated database shared by both clusters, no explicit path,
--         DDL run once in total (Phase 2);
--      c) Replicated database per cluster + explicit shared path, needs
--         database_replicated_allow_replicated_engine_arguments = 1 (Phase 3);
--      d) mixed engines, e.g. Atomic on main and Replicated on ext, as long as
--         the explicit path matches (Phase 5).
--    The only combination that does NOT replicate is a Replicated database per
--    cluster with the default {uuid} path (Phase 1).
-- 8. Metadata ALTERs of a ReplicatedMergeTree table follow the table's ZK path,
--    not the database engine (Phase 5): an ALTER ... ADD COLUMN issued on the
--    ext side reached all 6 nodes — the ext Replicated database queue delivered
--    it to both ext nodes, and the replication log carried it from there to the
--    main replicas of both shards. So "ordinary databases" does not mean
--    "the clusters never affect each other": schema changes still cross. What
--    stays local is everything that is not a replicated table — Distributed
--    wrappers, views, dictionaries — which must be maintained per cluster.
