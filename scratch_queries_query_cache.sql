-- ============================================================
-- Scenario 6 — DBLink + query cache tuning on the external cluster
--
-- Hypothesis: the external cluster can absorb repeated reads in its query
-- cache and stop forwarding them to the main cluster.
--
-- Stand: s1, ClickHouse 26.3.17.110
--   main_cluster : ch-main-s1r1 + s1r2 (shard 1), ch-main-s2r1 + s2r2 (shard 2)
--                  default.events_local (ReplicatedMergeTree) + events_distributed
--                  read-only account ro_export_user (from the pull/RMV scenario)
--   ext          : ch-ext-s1r1, standalone, stores nothing of its own
-- All refreshable MVs from the previous scenario were stopped first so they do
-- not pollute the source-side query_log:
--   SYSTEM STOP VIEW ext.events_sync;  SYSTEM STOP VIEW ext.dim_users_sync;
-- ============================================================


-- ============================================================
-- PHASE 0 — what the defaults actually are (ch-ext-s1r1)
-- ============================================================

SELECT name, value, default FROM system.server_settings WHERE name LIKE '%query_cache%';
-- query_cache.max_size_in_bytes        1073741824   (1 GiB, whole server)
-- query_cache.max_entries              1024
-- query_cache.max_entry_size_in_bytes  1048576      (1 MiB per entry)
-- query_cache.max_entry_size_in_rows   30000000

SELECT name, value, default FROM system.settings WHERE name LIKE '%query_cache%' ORDER BY name;
-- use_query_cache                                 0
-- enable_reads_from_query_cache                   1
-- enable_writes_to_query_cache                    1
-- query_cache_ttl                                 60
-- query_cache_min_query_duration                  0
-- query_cache_min_query_runs                      0
-- query_cache_share_between_users                 0
-- query_cache_nondeterministic_function_handling  throw     <- NOT "silently off"
-- query_cache_system_table_handling               throw
-- query_cache_max_size_in_bytes                   0   (per-user quota, 0 = unlimited)
-- query_cache_max_entries                         0   (per-user quota)
-- query_cache_compress_entries                    1
-- query_cache_squash_partial_results               1
-- query_cache_tag                                 ''


-- ============================================================
-- PHASE 1 — two ways to build the DBLink, and they are not equivalent
-- ============================================================

-- ---- 1a. view over remote() + named collection ----
-- ch-ext-s1r1  (named collection main_src already exists:
--   CREATE NAMED COLLECTION main_src AS host='ch-main-s1r1:9000',
--       user='ro_export_user', password='***')

CREATE VIEW ext.link_events AS
SELECT ts, user_id, event_type, value, ingested_at
FROM remote(main_src, database = 'default', table = 'events_distributed');

CREATE SETTINGS PROFILE external_readers SETTINGS
    use_query_cache = 1,
    query_cache_ttl = 300,
    query_cache_min_query_duration = 1000,
    query_cache_nondeterministic_function_handling = 'save';

CREATE USER bi_user IDENTIFIED WITH sha256_password BY 'bi_pw' SETTINGS PROFILE external_readers;
GRANT SELECT ON ext.link_events TO bi_user;

-- as bi_user:
SELECT count() FROM ext.link_events;
-- Code: 497. bi_user: Not enough privileges. To execute this query, it's
-- necessary to have the grant NAMED COLLECTION ON main_src. (ACCESS_DENIED)
-- => a plain view runs with INVOKER rights: every reader would need the
--    named-collection grant, i.e. the ability to call remote() themselves.

CREATE VIEW ext.link_events_definer
DEFINER = default SQL SECURITY DEFINER
AS SELECT ts, user_id, event_type, value, ingested_at
FROM remote(main_src, database = 'default', table = 'events_distributed');

GRANT SELECT ON ext.link_events_definer TO bi_user;

-- as bi_user:
SELECT count() FROM ext.link_events_definer;                    -- 11700
SELECT count() FROM remote(main_src, database='default', table='events_distributed');
-- Code: 497 ACCESS_DENIED  -> the user can read through the link and only through it

-- ---- 1b. Distributed table over a cluster declared in the ext config ----
-- docker/s1/config.d/common-ext.xml, inside <remote_servers>:
--   <main_link>
--       <shard><internal_replication>true</internal_replication>
--           <replica><host>ch-main-s1r1</host><port>9000</port>
--               <user>ro_export_user</user><password>***</password></replica>
--           <replica><host>ch-main-s1r2</host>...</replica>
--       </shard>
--       <shard>... ch-main-s2r1, ch-main-s2r2 ...</shard>
--   </main_link>
-- Picked up live, no restart:
SELECT cluster, shard_num, replica_num, host_name, user FROM system.clusters WHERE cluster = 'main_link';
-- main_link 1 1 ch-main-s1r1 ro_export_user
-- main_link 1 2 ch-main-s1r2 ro_export_user
-- main_link 2 1 ch-main-s2r1 ro_export_user
-- main_link 2 2 ch-main-s2r2 ro_export_user

CREATE TABLE ext.link_events_dist
(
    ts DateTime, user_id UInt64, event_type LowCardinality(String),
    value Float64, ingested_at DateTime
)
ENGINE = Distributed(main_link, default, events_local, rand());

SELECT count() FROM ext.link_events_dist;                       -- 12000


-- ============================================================
-- PHASE 2 — does the proposed profile actually cache anything?
-- ============================================================

-- as bi_user, twice, with the profile exactly as proposed:
SELECT event_type, count() FROM ext.link_events_definer WHERE value > 11
GROUP BY event_type ORDER BY event_type;
-- ext query_log: hits=0 misses=1 (105 ms), hits=0 misses=1 (68 ms)
-- main query_log: 2 SELECTs
-- => query_cache_min_query_duration = 1000 disables the cache for every query
--    that this link produces: the link queries run in 35-105 ms.

-- same query with the gate lowered:
SELECT event_type, count() FROM ext.link_events_definer WHERE value > 12
GROUP BY event_type ORDER BY event_type SETTINGS query_cache_min_query_duration = 0;
-- ext query_log: hits=0 misses=1 (84 ms), then hits=1 misses=0 (8 ms)
-- main query_log: 1 SELECT
-- => the hypothesis holds: the repeat query does not reach the main cluster at all.

SELECT query, key_hash, expires_at, stale, shared, compressed FROM system.query_cache;
-- expires_at = now + 300 (the profile TTL), stale=0, shared=0, compressed=1

-- the better gate for "don't cache one-off queries": count runs, not milliseconds
SELECT count() FROM ext.link_events_definer WHERE value > 21
SETTINGS query_cache_min_query_duration = 0, query_cache_min_query_runs = 2;
-- run 1: miss, run 2: miss, run 3: miss (this one writes), run 4: hit
-- 4 runs -> 3 SELECTs on main

-- the cache key is the AST, not the text: reformatting still hits
SELECT count() FROM ext.link_events_definer WHERE value > 22 SETTINGS query_cache_min_query_duration = 0;
SELECT
       count()
   FROM ext.link_events_definer
   WHERE value > 22
   SETTINGS query_cache_min_query_duration = 0;
-- 2 runs -> 1 SELECT on main, second query hits (1 ms)


-- ============================================================
-- PHASE 3 — TTL and staleness
-- ============================================================

SELECT count() FROM ext.link_events_definer WHERE value > 31
SETTINGS query_cache_min_query_duration = 0, query_cache_ttl = 15;

SELECT expires_at, stale, now() FROM system.query_cache WHERE query LIKE '%value > 31%';
-- 2026-08-24 11:33:28  0  2026-08-24 11:33:13
-- ... 17 seconds later ...
-- 2026-08-24 11:33:28  1  2026-08-24 11:33:31     <- stale = 1, still listed
-- re-running it: 1 SELECT on main again

-- staleness is the whole trade-off:
SELECT count() FROM ext.link_events_definer SETTINGS query_cache_min_query_duration = 0, query_cache_tag = 'events_dash';
-- 11700
-- (meanwhile, on ch-main-s1r1)
INSERT INTO default.events_distributed (ts, user_id, event_type, value, ingested_at)
SELECT now(), 7, 'cache_probe', 9.0, now() FROM numbers(300);
-- main now: 12000
SELECT count() FROM ext.link_events_definer SETTINGS query_cache_min_query_duration = 0, query_cache_tag = 'events_dash';
-- 11700   <- readers keep seeing the old number for up to TTL seconds

-- targeted invalidation
SELECT extract(query,'FROM [a-z_.]+') AS q, tag FROM system.query_cache;
-- FROM ext.link_events_definer   events_dash
SYSTEM DROP QUERY CACHE TAG 'events_dash';
SELECT count() FROM ext.link_events_definer SETTINGS query_cache_min_query_duration = 0, query_cache_tag = 'events_dash';
-- 12000


-- ============================================================
-- PHASE 4 — the hit-rate killer: entries are per user
-- ============================================================

CREATE USER bi_user2 IDENTIFIED WITH sha256_password BY 'bi_pw2' SETTINGS PROFILE external_readers;
GRANT SELECT ON ext.link_events_definer TO bi_user2;

-- user A warms, user B repeats the identical query:
-- A: hits=0 misses=1 (61 ms)   B: hits=0 misses=1 (57 ms)  -> 2 SELECTs on main
SELECT count() FROM system.query_cache WHERE query LIKE '%value > 41%';   -- 1

-- and B never gets its own entry while A owns the key:
-- 3 more runs by B -> 3 more SELECTs on main, still 1 entry in the cache.
-- => it is not "every user warms its own copy". Exactly one user is served
--    from cache; everybody else always goes to the source.

-- sharing is decided by the WRITER, the reader needs nothing:
SELECT count() FROM ext.link_events_definer WHERE value > 51
SETTINGS query_cache_min_query_duration = 0, query_cache_share_between_users = 1;   -- as bi_user
SELECT query, shared FROM system.query_cache WHERE query LIKE '%value > 51%';        -- shared = 1
-- bi_user2 then hits it (2-4 ms) with or without the setting.


-- ============================================================
-- PHASE 5 — what sharing actually costs (measured, not quoted)
-- ============================================================

-- a) a user with NO grant at all on the table
CREATE USER bi_nogrant IDENTIFIED WITH sha256_password BY 'ng_pw' SETTINGS PROFILE external_readers;
-- (no GRANT SELECT for this user)
SELECT count() FROM ext.link_events_dist WHERE value > 5
SETTINGS query_cache_min_query_duration = 0, query_cache_share_between_users = 1;
-- bi_user (granted)  -> 9810
-- bi_nogrant         -> 9810      <- NOT "Access denied". The cache hit is served
--                                    before the access check.
SHOW GRANTS FOR bi_nogrant;         -- empty: the user has no grants at all
-- the same user, same table, one query that is NOT in the cache:
SELECT count() FROM ext.link_events_dist WHERE value > 5.5
SETTINGS query_cache_min_query_duration = 0, query_cache_share_between_users = 1;
-- Code: 497. bi_nogrant: Not enough privileges. To execute this query, it's
-- necessary to have the grant SELECT ON ext.link_events_dist. (ACCESS_DENIED)
-- => access control works normally; the cache hit is what skips it.

-- b) row policies are bypassed too
GRANT SELECT ON ext.events TO bi_user, bi_user2;
CREATE ROW POLICY rp_local ON ext.events USING event_type = 'click' TO bi_user2;
-- honest, uncached numbers first:
SELECT count() FROM ext.events WHERE value > 0.5 SETTINGS use_query_cache = 0;
--   bi_user2 (policy): 1836      bi_user (no policy): 10873
-- now with a shared entry warmed by bi_user:
SELECT count() FROM ext.events WHERE value > 0
SETTINGS query_cache_min_query_duration = 0, query_cache_share_between_users = 1;
--   bi_user  warms : 10915
--   bi_user2 reads : 10915      <- sees the full table
--   bi_user2 truth :  1843      (same query, use_query_cache = 0)
DROP ROW POLICY rp_local ON ext.events;
-- => query_cache_share_between_users = 1 bypasses grants AND row policies.
--    Not "not recommended" - it is an access-control hole on a multi-tenant node.


-- ============================================================
-- PHASE 6 — how much load actually goes away
--           3 users x 5 identical queries through ext.link_events_dist,
--           counting ro_export_user SELECTs on ALL FOUR main nodes
--           (the Distributed link sends one subquery per shard)
-- ============================================================

-- A) use_query_cache = 0                          -> main: 32   ext hits/misses: 0/0
-- B) cache on, default per-user isolation         -> main: 24   ext hits/misses: 4/11
-- C) cache on, query_cache_share_between_users=1  -> main:  2   ext hits/misses: 14/1
-- D) same 15 queries from ONE technical user      -> main:  2   ext hits/misses: 14/1
-- => per-user isolation buys ~25%. A single technical account (or the unsafe
--    sharing flag) buys ~94%. There is no middle ground.


-- ============================================================
-- PHASE 7 — the silent non-caching cases
-- ============================================================

-- a) result larger than max_entry_size_in_bytes (1 MiB)
SELECT number, cityHash64(number) FROM numbers(1000)    FORMAT Null SETTINGS use_query_cache=1, query_cache_min_query_duration=0;
SELECT count() FROM system.query_cache WHERE query LIKE '%numbers(1000)%';      -- 1
SELECT number, cityHash64(number) FROM numbers(2000000) FORMAT Null SETTINGS use_query_cache=1, query_cache_min_query_duration=0;
SELECT count() FROM system.query_cache WHERE query LIKE '%numbers(2000000)%';   -- 0
-- every run of the big query is a miss, no warning anywhere. And it is not free:
SELECT event, value FROM system.events WHERE event LIKE 'QueryCacheWritten%';
-- QueryCacheWrittenRows 4001066 / QueryCacheWrittenBytes 64017287
-- => the result IS serialized into a cache buffer on every run and then thrown away.

-- b) per-user quota
CREATE SETTINGS PROFILE quota_probe SETTINGS
    use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_max_entries = 1;
-- user with that profile, two distinct queries x2 runs:
--   value > 101: miss, hit      value > 102: miss, miss (never cached)
-- => the quota silently starves every query after the first.

-- c) non-deterministic functions - the DEFAULT is an ERROR, not "no caching"
SELECT now() AS t, count() FROM ext.link_events_definer
SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0;
-- Code: 704. The query result was not cached because the query contains a
-- non-deterministic function. Use setting
-- `query_cache_nondeterministic_function_handling = 'save'` or `= 'ignore'` ...
-- (QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS)

SELECT now() AS t FROM numbers(1)
SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0,
         query_cache_nondeterministic_function_handling = 'save';
-- 2026-08-24 11:35:09
-- 2026-08-24 11:35:09      (2 s later)
-- 2026-08-24 11:35:09      (4 s later)   <- frozen for the whole TTL

SELECT today() AS d, 'ignore_probe' FROM numbers(1)
SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0,
         query_cache_nondeterministic_function_handling = 'ignore';
SELECT count() FROM system.query_cache WHERE query LIKE '%ignore_probe%';       -- 0

-- d) system tables
SELECT count() FROM system.tables SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0;
-- Code: 719. QUERY_CACHE_USED_WITH_SYSTEM_TABLE
-- (query_cache_system_table_handling = 'save' | 'ignore')


-- ============================================================
-- PHASE 8 — readonly profiles and pinning the settings
-- ============================================================

CREATE SETTINGS PROFILE external_readers_ro SETTINGS
    readonly = 1, use_query_cache = 1, query_cache_ttl = 300,
    query_cache_min_query_duration = 0,
    query_cache_nondeterministic_function_handling = 'save';
CREATE USER bi_ro IDENTIFIED WITH sha256_password BY 'ro_pw' SETTINGS PROFILE external_readers_ro;
GRANT SELECT ON ext.link_events_definer, ext.events TO bi_ro;

SELECT count() FROM ext.events;                     -- 10615, local table is fine
SELECT count() FROM ext.link_events_definer;
-- Code: 164. default: Cannot execute query in readonly mode. (READONLY)
-- => readonly = 1 kills the remote() link. Note the user name in the message:
--    it is the DEFINER's execution that is refused.

GRANT SELECT ON ext.link_events_dist TO bi_ro;
SELECT count() FROM ext.link_events_dist;           -- 12000, twice: miss 36 ms, hit 1 ms
-- => the Distributed-table link works under readonly = 1. It is a table, not a
--    table function.

-- readonly = 2 lets the remote() link work, but then users can also opt out:
CREATE SETTINGS PROFILE external_readers_ro2 SETTINGS readonly = 2, use_query_cache = 1, ...;
SELECT count() FROM ext.link_events_definer SETTINGS use_query_cache = 0;   -- allowed

-- pinning individual settings is the actual answer:
CREATE SETTINGS PROFILE external_readers_const SETTINGS
    readonly = 2,
    use_query_cache = 1 CONST,
    query_cache_ttl = 300,
    query_cache_min_query_duration = 0,
    query_cache_nondeterministic_function_handling = 'save' CONST;
SELECT count() FROM ext.link_events_definer WHERE value > 91;                       -- 919
SELECT count() FROM ext.link_events_definer WHERE value > 91 SETTINGS use_query_cache = 0;
-- Code: 452. Setting use_query_cache should not be changed. (SETTING_CONSTRAINT_VIOLATION)
SELECT count() FROM ext.link_events_definer WHERE value > 91 SETTINGS query_cache_ttl = 3600;
-- 919   <- non-CONST settings stay overridable on purpose


-- ============================================================
-- PHASE 9 — operational properties
-- ============================================================

-- the cache keeps serving while the source cluster is unreachable
-- (warm one query, then: docker stop s1-ch-main-s1r1)
SELECT event_type, count() FROM ext.link_events_definer GROUP BY event_type ORDER BY event_type
SETTINGS query_cache_min_query_duration = 0, query_cache_ttl = 600;
-- full result, served from cache with the source down
SELECT count() FROM ext.link_events_definer WHERE value > 61 SETTINGS query_cache_min_query_duration = 0;
-- Code: 519. All attempts to get table structure failed.

-- the cache is in-process: it does not survive a restart
SELECT count() FROM system.query_cache;             -- 11 entries, QueryCacheBytes = 21776
-- docker restart s1-ch-ext-s1r1
SELECT count() FROM system.query_cache;             -- 0, and the next query hits the source

-- monitoring surface
SELECT name FROM system.columns WHERE table = 'query_cache' ORDER BY position;
-- query, query_id, result_size, tag, stale, shared, compressed, expires_at, key_hash
--   (no is_subquery column in 26.3)
SELECT metric, value FROM system.metrics WHERE metric LIKE 'QueryCache%';
-- QueryCacheBytes, QueryCacheEntries
SELECT event, value FROM system.events WHERE event LIKE 'QueryCache%';
-- QueryCacheHits, QueryCacheMisses, QueryCacheAgeSeconds,
-- QueryCacheReadRows, QueryCacheReadBytes, QueryCacheWrittenRows, QueryCacheWrittenBytes
-- per query: ProfileEvents['QueryCacheHits'] / ProfileEvents['QueryCacheMisses'] in query_log


-- ============================================================
-- SUMMARY
-- ============================================================
-- 1. The hypothesis holds, with a caveat about who is asking. A repeated
--    identical query is served entirely on the ext node: 36-105 ms -> 1-8 ms,
--    and zero queries reach the main cluster.
-- 2. The proposed profile as written caches NOTHING: link queries take tens of
--    milliseconds, and query_cache_min_query_duration = 1000 gates them all out.
--    Use query_cache_min_query_runs = 2..3 instead - it filters one-off queries
--    without filtering out fast ones.
-- 3. query_cache_nondeterministic_function_handling defaults to 'throw', not to
--    "no caching": turning use_query_cache on in a profile makes every dashboard
--    query containing now()/today() fail with Code 704 until 'save' or 'ignore'
--    is set. With 'save' the value is frozen for the whole TTL, so a rolling
--    "last hour" window silently stops rolling.
-- 4. Per-user isolation is worse than "each user warms its own copy": while one
--    user owns the key, other users' results are never cached at all. Measured
--    over 3 users x 5 identical queries: 32 -> 24 source subqueries (~25%).
--    A single technical account gives 32 -> 2 (~94%).
-- 5. query_cache_share_between_users = 1 is not merely "not recommended": it
--    serves cached results to users with no grant on the table and bypasses row
--    policies (10915 instead of 1843). On a multi-tenant external node it is an
--    access-control hole; the safe way to the same hit rate is one technical
--    user behind the BI proxy.
-- 6. Silent non-caching has three sources, none of them logged: results over
--    max_entry_size_in_bytes (1 MiB), per-user quotas (query_cache_max_entries /
--    max_size_in_bytes), and 'ignore' handling. Oversized results are still
--    serialized on every run and then discarded.
-- 7. Implementation choice matters: a view over remote() keeps credentials in a
--    named collection but needs SQL SECURITY DEFINER and is incompatible with
--    readonly = 1; a Distributed table over a cluster declared in the ext config
--    works under readonly = 1 but puts the source password into the config file.
-- 8. Pin the settings with CONST in the profile; readonly alone does not do it
--    (readonly = 2 still lets users change settings, readonly = 1 breaks the
--    remote() link entirely).
-- 9. Bonus property: while an entry is warm, the ext node answers even when the
--    main cluster is completely down. And the cache is in-process - a restart of
--    the ext node empties it.
