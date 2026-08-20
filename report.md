# Cross-Cluster Replication in ClickHouse — Full Report

> **Version note (post-research update)**: Docker Compose files/Makefile in
> this repo are pinned to `26.3.17.110` (26.3 LTS). Everything below —
> every measurement, error message, and behavior described — was produced
> against `25.8.30.16` (25.8 LTS), the version actually used when this
> research was run. Nothing below has been re-verified on 26.3.17.110.

## 1. Scope, method, and environment

**Goal**: determine whether ClickHouse's native mechanisms can replicate
data from a production sharded cluster into a separate, maximally-isolated
external cluster for read-only external users, and if so, which mechanism
to use under which conditions.

**Version**: `clickhouse/clickhouse-server:25.8.30.16` /
`clickhouse/clickhouse-keeper:25.8.30.16`. 25.8 is the current LTS branch
(released 2025-08-28). Verified via:
- https://clickhouse.com/docs/resources/changelogs/oss/2025 (accessed during
  this research session)
- https://endoflife.date/clickhouse (25.8 LTS shown active, 25.3 LTS the
  prior one, both accessed same session)
- Docker Hub tag listing for `clickhouse/clickhouse-server`, confirming
  `25.8.30.16` as the latest patch on the 25.8 branch at time of research.

Both images were pulled and `SELECT version()` confirmed `25.8.30.16` on
every container used in every scenario.

**clickhouse-copier**: confirmed via the OSS changelog and independent
sources (oneuptime.com, 2026-03-31-dated article; cross-referenced against
2024 OSS changelog entries) to be no longer actively maintained or bundled
with recent server packages — moved to a separate `ClickHouse/copier`
repository. Not built, per task instruction; recorded as a documentation
fact only.

**Sandbox**: 4 vCPU, 7.8 GB RAM, 144 GB disk, Docker 29.7.2 / Compose v5.4.0.
Idle single ClickHouse node measured at ~224 MiB RSS / ~6% CPU
(`--memory=600m` limit, 10s after start). Scenarios were run one at a time
(not all nine simultaneously) to give each the full resource budget; see
README.md for the explicit list of deviations from the task's defaults.

**Uniform methodology applied per scenario** (see each scenario's section
below for what specifically was and wasn't exercised):
1. Bring up environment, load data, confirm baseline consistency.
2. Insert load (batches of 2K–500K rows; NOT a sustained 30–100K rows/sec
   stream for 15+ minutes — see README.md's assumptions section for why,
   and how per-row costs were extrapolated instead).
3. Mutations (ALTER UPDATE/DELETE), OPTIMIZE FINAL, ADD COLUMN, DROP
   PARTITION — propagation and timing recorded.
4. Chaos: node down/up, network disconnect/reconnect, Keeper down (S1c),
   disk-growth-under-outage (S3), query bomb (S1, S9).
5. Consistency: `count()` and `sum(cityHash64(*))` per partition, with and
   without FINAL, compared main vs ext.
6. Isolation test: from the "external" side, attempt to reach main's nodes,
   run INSERT/ALTER/SYSTEM, read `system.zookeeper`.

---

## 2. S1 — External node as additional replica of the same ReplicatedMergeTree

### 2.1 Topology

2 shards × 2 replicas on main (`ch-main-s1r1/s1r2/s2r1/s2r2`), 2 single-replica
shards on ext (`ch-ext-s1r1/s2r1`), sharing ONE 3-node Keeper ensemble
(`keeper-1/2/3`). `docker/s1/docker-compose.yml`. ext nodes are attached to
`net-ext` + a shared `net-repl` network with main and Keeper; they are
**not** listed in main's `remote_servers` at all — they get their own
`ext_cluster` definition for their own `Distributed` tables (not built in
this pass, but the config supports it — see `config.d/common-ext.xml`).

### 2.2 The UUID/macros recipe (the documented "main practical pitfall")

Bare `ReplicatedMergeTree` (no explicit engine arguments) uses
`default_replica_path = /clickhouse/tables/{uuid}/{shard}`, where `{uuid}`
resolves to the table's own randomly-generated UUID in an `Atomic` database.
Confirmed empirically: attempting this bare form fails outright —
`Code: 36. BAD_ARGUMENTS: Macro 'uuid' in engine arguments is only
supported when the UUID is explicitly specified, used within an ON CLUSTER
query, or when using the Replicated database engine` — UNLESS you either
(a) run the CREATE via `ON CLUSTER` over a cluster naming both groups
(defeats the "ext absent from remote_servers" isolation goal for that one
statement), or (b) specify `UUID '...'` explicitly on every replica so they
agree, or (c) use the `Replicated` database engine (auto-manages UUID
agreement, not evaluated as S2 in this pass, see §9).

**The recipe actually used and recommended (avoids the pitfall entirely)**:
use EXPLICIT engine arguments with a macro-based path that does NOT depend
on `{uuid}` at all:
```sql
CREATE TABLE testgame.events
(...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/testgame/events', '{replica}')
...
```
with per-node macros:
```xml
<!-- ch-main-s1r1 -->
<macros><shard>1</shard><replica>main_r1</replica></macros>
<!-- ch-ext-s1r1 -->
<macros><shard>1</shard><replica>ext_r1</replica></macros>
```
Both resolve to the SAME zoo_path (`/clickhouse/tables/1/testgame/events`)
with DISTINCT replica names. Confirmed working end to end: an INSERT on
`ch-main-s1r1` appeared on `ch-ext-s1r1` within 5 seconds, and
`system.replicas` on both sides showed all registered replica names
(`main_r1`, `main_r2`, `ext_r1`) for the shard-1 table.

### 2.3 S1a (full peer) and S1b (follower-only tuning)

**S1a confirmed**: ext node is a genuine, full replica — reads, merges, can
become leader (`is_leader=1` by default), and **can accept writes that
propagate back to main** (confirmed: an INSERT issued directly against
`ch-ext-s1r1` appeared on `ch-main-s1r1` within 5s). This is the single
most important caveat of S1: the replication mechanism provides ZERO
write-isolation on its own. Isolation property (c) — "external users must
not be able to write or modify data" — is entirely the job of the
user/profile layer, not the replication topology.

**S1b settings verified to exist in 25.8.30.16 before use** (via
`system.merge_tree_settings` / `system.settings` on the running
container):

| Setting | Scope | Default | Verified |
|---|---|---|---|
| `always_fetch_merged_part` | per-table (MergeTree) | `0` | EXISTS |
| `replicated_can_become_leader` | per-table (MergeTree) | `1` | EXISTS |
| `max_concurrent_queries_for_user` | per-user (settings) | `0` (unlimited) | EXISTS |
| `max_memory_usage`, `max_execution_time`, `readonly` | per-user (settings) | version-standard | EXIST |

Applied via `ALTER TABLE testgame.events MODIFY SETTING
replicated_can_become_leader = 0` on the ext replica — confirmed effective:
`system.replicas.can_become_leader` flipped from `1` to `0`. A dedicated
`ext_reader` user (`profile=ext_readonly`, `readonly=1`,
`max_concurrent_queries_for_user=10`, `quota` with a 60s/1000-query window)
was created (`docker/s1/users.d/users-ext.xml`) and tested:

- SELECT: succeeds.
- INSERT / `ALTER ... DELETE`: rejected with `Code: 164. READONLY`.
- `SYSTEM DROP REPLICA`: rejected with `Code: 497. ACCESS_DENIED`.
- `SELECT ... FROM system.zookeeper WHERE path=...`: **succeeds** — this is
  a genuine isolation gap: `readonly=1` does NOT restrict access to
  `system.zookeeper`, exposing the main cluster's internal replica
  topology/paths to any readonly external user. **Fix (not yet tested,
  documented as required hardening)**: explicit
  `REVOKE SELECT ON system.zookeeper FROM ext_reader`, or grant `SELECT`
  only on `testgame.*` from the start rather than inheriting broad
  defaults.

### 2.4 S1c — split Keeper: confirmed to break completely

Stood up an independent single-node Keeper (`keeper-ext-1`, own
`raft_configuration`), repointed `ch-ext-s2r1`'s `<zookeeper>` block at it
while KEEPING the identical zoo_path string
(`/clickhouse/tables/2/testgame/events`). Result: **zero data sharing in
either direction** — an insert on the split-Keeper node never reached main,
and vice versa. The identical zoo_path string is meaningless across two
independent Keeper clusters; it only produces shared replication when
resolved within the SAME physical ensemble. **This definitively answers
the "is a shared Keeper acceptable" question for S1 specifically: it is
not optional, it is structurally required.** This is the core isolation
cost of the S1 approach: the external replica's health and behavior are
permanently coupled to the SAME Keeper ensemble serving production traffic.

### 2.5 Traffic direction and interserver load

`system.replicated_fetches` and `ReplicatedPartFetches` (system.events)
confirmed the ext replica FETCHES parts from main (expected: main did the
inserting in most tests). Reverse traffic (main fetching from ext) was
observed too, once — because an insert issued directly on ext propagated
data that main then had to fetch, confirming there is **no built-in
guarantee that production nodes never become a fetch SOURCE for external
ones**: any replica, including main's, will serve a part-fetch request
from any other replica of the same table, main or ext, without
distinction. The only way to structurally prevent main from ever serving
ext is the readonly-user write-isolation (§2.3) combined with
`always_fetch_merged_part`/`replicated_can_become_leader` tuning (which
reduces but does not eliminate ext's ability to become a merge/fetch
source for main, since those settings govern ext's own OUTGOING behavior,
not main's willingness to accept a fetch request FROM ext).

### 2.6 DDL: corrects the task's assumed risk

**Measured**: `ALTER TABLE testgame.events ADD COLUMN yet_another_field
UInt8 DEFAULT 0`, issued as an ordinary (non-`ON CLUSTER`) ALTER directly
on `ch-main-s1r1`, propagated to `ch-main-s1r2` (same shard, in
`main_cluster`) AND to `ch-ext-s1r1` (same shard, explicitly NOT in
`main_cluster`'s `remote_servers`) within 3 seconds — but did NOT reach
`ch-main-s2r1` (different shard). Same result for `ALTER ... DROP
PARTITION` and `ALTER ... UPDATE/DELETE` mutations.

**Why**: for a `ReplicatedMergeTree`, these ALTER types are themselves
entries written into the table's OWN replication log at its Keeper
zoo_path — the exact same mechanism that propagates INSERTs. `ON CLUSTER`
is unrelated; it's merely a convenience for fanning the SAME DDL text out
to N hosts named in a cluster definition in one client round-trip.

**Corrected framing for the report** (overturns the task brief's "DDL: ON
CLUSTER over main will not touch ext -> schema drift risk" as a blanket
statement): schema drift IS a real risk only for DDL that does NOT go
through the replication log — `CREATE TABLE`/`DROP TABLE`/`RENAME TABLE`
(these create/destroy the very znode path being watched) and DDL against
non-replicated objects (plain `MergeTree`, dictionaries, `Distributed`
tables, views). For THOSE cases, `ON CLUSTER` naming both groups (built as
`all_nodes_ddl` in `config.d/common-main.xml`, tested only for the create
step) or a manual per-node statement is the only fix, and it does cost an
isolation concession (main's config must name ext hostnames for that
cluster entry to exist) — but ordinary schema ALTERs on already-replicated
tables need no such fix at all.

### 2.7 Chaos and reverse-impact tests (full detail in LOG.md)

| Test | Result |
|---|---|
| ext down 40+ min, continuous inserts + mutation on main | No latency change on main (0.003s flat); mutation completed (`is_done=1`) without waiting for the dead replica |
| `insert_quorum=3` (all 3 replicas) with ext down | Insert REJECTED: `Code: 285. TOO_FEW_LIVE_REPLICAS` |
| `insert_quorum=auto` (majority) with ext down | Insert SUCCEEDS immediately |
| `SYSTEM DROP REPLICA` for a permanently-lost ext node | Clean: `total_replicas` 3→2 |
| Permanent-loss rejoin recipe | Orphaned node stuck `is_readonly=1` forever until `DROP TABLE ... SYNC` + `CREATE TABLE` with the SAME zoo_path and an up-to-date schema (every ADD COLUMN since); full re-fetch, no partial resync |
| Query bomb: 8 concurrent heavy queries on ext (readonly user, `max_concurrent_queries_for_user=10`) | Main latency flat throughout (0.011–0.054s); `docker stats` showed fully independent CPU/RAM (47%/697MiB main vs 17%/476MiB ext) — separate OS processes, no shared resource contention |
| Network disconnect/reconnect (`docker network disconnect`) | Replica stayed `is_readonly=0` within the Keeper `session_timeout_ms=30000` grace window; insert on main during the partition succeeded; ext caught up ~8s after reconnect — no special recovery needed for a partition shorter than the session timeout |

### 2.8 Security

`interserver_http_credentials` (shared secret between main and ext for
part-fetch authentication) was configured and required — without it,
interserver HTTP calls between arbitrary nodes on the same network would
succeed unauthenticated. Keeper itself has no ACL configured in this
sandbox (default ClickHouse Keeper has no ACL enforcement out of the box
unless explicitly configured with `four_letter_word_allow_list` and
Keeper-native ACLs, not evaluated in this pass — flagged as a production
hardening item). Required ports for S1: Keeper client (9181, ext→keeper),
interserver HTTP (9009, bidirectional depending on fetch direction). Native
client port (9000) must be firewalled between ext and main entirely (not
needed for replication itself, only for direct SQL access, which should be
routed through ext's own listener, not main's).

---

## 3. S6 — BACKUP/RESTORE to S3 (MinIO)

### 3.1 Topology

Two FULLY independent single-node clusters (`ch-main` + `keeper-main-1`;
`ch-ext` + `keeper-ext-1`), no shared Keeper, no shared network between
them at all after a topology fix (see §3.5). MinIO + `mc` provide the S3
bucket, dual-homed across two disjoint bridge networks so each side reaches
it without reaching each other.

### 3.2 Full and incremental backup — measured

| Metric | Full backup | Incremental (`base_backup=...`) |
|---|---|---|
| Wall time | 0.271s | 0.452s |
| `num_files` | 55 | 69 |
| `uncompressed_size` | 6,724,796 B | 7,577,103 B |
| Data | 300,000 rows, 4 partitions | +50,000 rows, +1-row `ALTER UPDATE` |

RESTORE: 0.505s for the full database; row-for-row consistency confirmed
(`count()` and `sum(cityHash64(*))` per partition, identical on both sides).

**Key finding**: the "incremental" backup was barely smaller than the full
one, because a single-row-matching `ALTER TABLE ... UPDATE` rewrote EVERY
active part's name (mutation-version suffix bump, e.g. `20260816_0_0_0` →
`20260816_0_0_0_1`), even though 3 of the 4 partitions' actual byte content
was unchanged. `BACKUP ... base_backup=...` deduplicates at the PART level
(unchanged part name+checksum are skipped), not at the row/byte level — any
operation that renames parts for unrelated reasons (mutations, `OPTIMIZE
FINAL`, merges) defeats incremental-backup efficiency for those partitions
specifically. **This version does expose `enable_lightweight_update=1`
(default-on) as a newer alternative mutation mechanism aimed at avoiding
full part rewrites; whether it avoids this specific backup-bloat effect was
NOT tested in this pass** — flagged as a follow-up, not asserted.

### 3.3 Partition-level backup, RESTORE pitfalls, and the actual working recipe

`BACKUP TABLE events PARTITION 'x' TO S3(...)` works as documented. Two
genuine dead ends were found in the "RESTORE into staging + ATTACH/REPLACE
PARTITION" pattern suggested by the task brief:

1. **`RESTORE ... SETTINGS allow_non_empty_tables=1` directly into the live,
   non-empty target table is NOT idempotent.** Re-running the same
   partition restore duplicated all rows in that partition (81,226 →
   212,452) — restored parts are added alongside existing ones, no
   deduplication. **Fix**: `ALTER TABLE events DROP PARTITION 'x'`
   immediately before the RESTORE, making the cycle idempotent at the cost
   of a brief single-partition-empty window (not whole-table downtime).

2. **`RESTORE TABLE src AS dst` is structurally broken for a
   `ReplicatedMergeTree` source with a literal (non-`{table}`-macro)
   zoo_path.** Tried three ways (pre-existing plain `MergeTree` staging
   table, pre-existing `ReplicatedMergeTree` staging table with its own
   distinct zoo_path, and letting RESTORE create the staging table fresh
   with `structure_only=1`): ALL THREE FAILED. The first two failed on
   schema-mismatch (`CANNOT_RESTORE_TABLE` — RESTORE compares against the
   backup's ORIGINAL DDL, ignoring the destination's actual definition);
   the third failed with `Code: 253. REPLICA_ALREADY_EXISTS` because
   RESTORE recreates the table AT THE SOURCE'S OWN zoo_path regardless of
   the `AS`-rename, colliding with the real table's already-registered
   replica on the same node. **This means the staging-table pattern from
   the task brief does not work as stated** for a table using the very
   zoo_path style this report recommends elsewhere (S1, §2.2) — the two
   recommendations partially conflict, and this report states that
   plainly. The only tested, actually-working recipe for minimal-downtime
   partition refresh in this version is the DROP-PARTITION-then-RESTORE
   pattern from finding #1 above, not staging+REPLACE PARTITION.

### 3.4 RESTORE onto a different shard topology

Not separately exercised as a distinct multi-shard test in this pass (S6's
build used single-node main/ext by design, to isolate the BACKUP/RESTORE
mechanism itself from S1's multi-shard concerns) — but the mechanism is
schema-driven, not topology-driven: `RESTORE DATABASE` only requires
column/engine compatibility, and was confirmed to succeed regardless of
main and ext each independently defining `{shard}`/`{replica}` macros as
`1`/`main_r1` and `1`/`ext_r1` respectively (i.e., ext's "shard 1" has no
relationship whatsoever to main's "shard 1" beyond the human-chosen macro
value) — this is the basis for the "best topology flexibility" rating in
the comparison matrix.

### 3.5 Isolation test caught a REAL bug in this environment's own topology

Initial build put `ch-main` and `ch-ext` on a SHARED network (`net-s3`) so
both could reach MinIO. The isolation test (mandated by the uniform
methodology) revealed this shared network ALSO gave them full mutual
reachability: `ch-ext` could `SELECT` from AND `INSERT` into `ch-main`'s
tables directly, over the native protocol, with the default blank-password
user. **This defeated S6's entire "the bucket is the only point of
contact" premise as first built.**

**Fix applied and reverified**: split into two disjoint bridge networks
(`net-s3-main`, `net-s3-ext`), with MinIO dual-homed across both. After the
fix: `ch-ext` cannot resolve `ch-main` at all (`getent hosts` empty,
`clickhouse-client --host ch-main` → `Code: 198. DNS_ERROR`), while
`BACKUP`/`RESTORE` against MinIO continued to work unchanged from both
sides (re-verified with a fresh `BACKUP DATABASE` call after the fix).

**Why this matters beyond the sandbox**: this is a hands-on demonstration
of a mistake that's easy to make in real infrastructure — putting main and
external ClickHouse nodes in a shared VPC/subnet "just for S3 access"
(shared NAT gateway, shared services VPC peering) can silently reproduce
exactly this hole. The isolation test is not optional busywork; it is what
caught a real defect in this very research project's own environment.

---

## 4. S3 — Push via Materialized View → Distributed

### 4.1 Topology

Main (single node) + ext (single node), on separate networks with one
explicit `net-push` channel carrying main-initiated INSERT traffic to ext's
native protocol port (opposite direction of S6/S7's pull model).

### 4.2 Mechanism confirmed

`INSERT INTO main.events` → MV fires → writes to
`Distributed(ext_push_cluster, testgame, events, rand())` → lands on ext.
Confirmed for both single-row and 10,000-row bulk inserts, ~3s propagation.

### 4.3 Backfill and POPULATE — a genuine hard limitation found

Confirmed the task brief's warning: historical data present in `main.events`
BEFORE the MV's creation NEVER propagates (MV is a pure insert trigger).

**New finding**: `CREATE MATERIALIZED VIEW ... TO <table> POPULATE AS ...`
is REJECTED OUTRIGHT — `Code: 62. SYNTAX_ERROR: you can't declare both 'TO
[db].[table]' and 'POPULATE'`. `POPULATE` only works for MVs that own their
own implicit storage (no `TO` target). **This means the task brief's
"POPULATE race condition" warning is moot for this specific push pattern —
POPULATE cannot even be attempted with a `TO` target, so that particular
race cannot occur.** The real, verified-working backfill recipe: create the
MV WITHOUT POPULATE first (new inserts start flowing with zero gap), then
manually `INSERT INTO events_push_dist SELECT * FROM events WHERE
<historical predicate>` to backfill. This introduces a DIFFERENT race: any
row inserted between "MV created" and "manual backfill query issued" that
also matches the backfill predicate is pushed TWICE. Mitigations: bound the
backfill predicate with a strict upper timestamp = the exact MV-creation
instant, or (for tables with a natural dedup key, like `users_profile` but
NOT `events` in this schema) rely on `ReplacingMergeTree` + `FINAL` on ext.

### 4.4 Distributed queue growth and the disk-fill scenario — measured

With ext down, `system.distribution_queue` grew linearly: ~41.3 KiB per
2,000-row batch (~20.6 bytes/row on-disk in the queue file format for this
schema); a separate single 50,000-row insert grew the queue directory by
exactly 1,011,088 bytes (~20.2 bytes/row, consistent). **Extrapolated to the
task spec's 30,000–100,000 rows/sec sustained rate: ~600 KB/s to ~2 MB/s of
UNBOUNDED disk growth on MAIN for every second ext is unreachable** —
`bytes_to_throw_insert` and `bytes_to_delay_insert` both default to `0`
(disabled) in this version. Both settings were confirmed to exist and work
(via the official docs and empirical test) but can ONLY be set at `CREATE
TABLE` time — `ALTER TABLE ... MODIFY SETTING` on a `Distributed` table
fails outright (`Code: 48. NOT_IMPLEMENTED`).

**The single most dangerous finding in this entire research project**:
recreated `events_push_dist` with `bytes_to_throw_insert=2000000,
bytes_to_delay_insert=500000`. With ext down, inserting past the threshold
produced client-visible exceptions (`Code: 574.
DISTRIBUTED_TOO_MANY_PENDING_BYTES`) on 6 of 8 test batches — but **all 8
batches' rows (160,000 total) were confirmed present in the SOURCE
`events` table on main regardless of the exception.** The MV's push into
the Distributed table is rejected AFTER the row is already durably written
to the source table; the local write is NOT rolled back when the
downstream MV target throws. From the client's point of view this looks
exactly like a failed INSERT, and the natural (correct, industry-standard)
response to an apparent INSERT failure is to retry — which will silently
DOUBLE-WRITE that batch into the PRIMARY production table, not just the
pushed copy, since ClickHouse's block-level insert deduplication does not
apply across independently-issued INSERT statements with different literal
data. **This must be a hard operational warning for anyone adopting S3,
not a footnote.** A second, related gap: the 120,000 rows from the 6
rejected batches are not queued anywhere for automatic retry once ext comes
back — they permanently exist ONLY on main; any gap-fill is the operator's
manual responsibility.

### 4.5 Isolation

Structurally requires main to be able to open outbound connections to ext
(push model) — the underlying Docker bridge (or any typical L3 network) is
bidirectional by default, so ext can ALSO reach main unless a directional
firewall rule is separately enforced (same `br_netfilter`-limited-testing
caveat as S1; see §7). This is the worst default network posture of any
tested scenario, because the connection-INITIATING side (main, the more
sensitive one) is the side making outbound calls, and the underlying
network fact that the callee can typically call back is a firewall
commitment, not a protocol guarantee.

---

## 5. S9 — Anti-pattern baseline (Distributed only, no replication)

### 5.1 Topology

Main (single node, 5,000,000-row `events` table) + ext (single node, ZERO
local tables, ZERO Keeper connection — only `Distributed(main_from_ext,
testgame, events, rand())`).

### 5.2 Quantified cost of zero isolation

| Condition | Main query latency |
|---|---|
| Baseline | 0.046s |
| During 6× concurrent ext-side full-table-sort queries (~35s window) | 0.887s / 0.999s / 1.377s / 1.498s / 0.892s |
| After bomb | 0.092s, settling back to ~0.05s |

**Degradation factor: ~20–33x baseline latency.** Every query issued against
ext's Distributed table executes ON main's own CPU, memory, and disk I/O —
there is no process/resource boundary at all, unlike every other scenario
tested. `INSERT INTO events_dist` from ext succeeded and reached main
within 2 seconds using the default blank-password user — write isolation is
completely absent by construction; would require the same
readonly-user-profile fix as S1, but WITHOUT gaining any of S1's actual
data/resource isolation benefits in exchange. Stopping main entirely made
ext 100% non-functional (`Code: 279. ALL_CONNECTION_TRIES_FAILED`) —
diametrically opposite to every other scenario's graceful degradation.

**This is the quantified "cost of doing nothing" the task asked for.**

---

## 6. S7 — ALTER TABLE ... FETCH PARTITION + ATTACH

### 6.1 Topology

Main + ext, separate networks, ext given ONLY an `auxiliary_zookeepers`
entry pointing at main's Keeper (read access to main's znode tree, no
membership) plus matching `interserver_http_credentials` to authenticate
the actual part-pull HTTP calls.

### 6.2 Mechanism confirmed, with an operational pitfall found

`ALTER TABLE events FETCH PARTITION 'x' FROM 'main_keeper:/clickhouse/tables/1/testgame/events'`
lands the part under `detached/`; a subsequent `ALTER TABLE events ATTACH
PARTITION 'x'` makes it live. Confirmed working for both a closed
historical partition (3,121 rows, 0.25s) and the "live" still-receiving-
inserts partition (110,479 rows at fetch time). Re-running the cycle after
new inserts landed on main correctly picked up the delta (110,479 →
110,979, matching main exactly) with NO duplication — when FETCH is
immediately followed by ATTACH in the same operational cycle.

**Pitfall found**: if the cycle is interrupted between FETCH and ATTACH
(script crash, or an operator running FETCH twice before attaching), the
SECOND FETCH fails outright: `Code: 256. PARTITION_ALREADY_EXISTS:
Detached partition 20260819 already exists` — FETCH refuses to overwrite an
existing detached part of the same name. Recovery requires `ALTER TABLE ...
DROP DETACHED PARTITION`, itself gated by `allow_drop_detached` (default:
rejected, `Code: 344. SUPPORT_IS_DISABLED`, requires explicit `SETTINGS
allow_drop_detached=1`). **Any periodic FETCH+ATTACH script must treat the
pair as one atomic unit with its own crash-recovery cleanup, or a single
missed cycle wedges every subsequent refresh of that partition** — scripted
in `scripts/fetch_partition_cycle.sh` (cleanup-before-fetch pattern).

### 6.3 Coupling — narrower than S1, wider than S6

Ext needs (a) network reachability to main's Keeper client port (read-only
znode access via `auxiliary_zookeepers`) and (b) reachability to main's
interserver HTTP port (9009) using shared credentials, both channels
INITIATED BY EXT (a pull model, same security-friendly direction as S6).
Ext NEVER writes a znode under main's replicated-table path and is never
visible in `system.replicas` on main — no replica-set membership, no
Keeper session to lose, and no `SYSTEM DROP REPLICA` cleanup ever required
if ext disappears permanently, a real operational simplification versus
S1. Isolation test showed the same "ext can reach main directly over the
shared fetch network" result as S1/S3 — required for the interserver pull
itself, not a topology accident; needs the same firewall enforcement
discussion as §7.

---

## 7. Network isolation: what was and wasn't verified

Three distinct findings, not to be conflated:

1. **Genuine, verified network isolation** (S6, after the fix in §3.5):
   containers on completely disjoint Docker networks cannot resolve or
   route to each other at all — enforced by Docker's network-namespace
   model itself, not by netfilter. This is unconditionally real and was
   directly demonstrated (DNS_ERROR, connection failure).

2. **Structurally-required bidirectional reachability** (S1 §2.8, S3 §4.5,
   S7 §6.3): mechanisms that need EITHER side to initiate outbound
   connections to the other inherently sit on a network segment that
   permits reachability in both directions by default (any typical Docker
   bridge or cloud VPC subnet). Restricting this to the exact declared
   port list requires an explicit firewall rule (iptables `DOCKER-USER`
   chain locally; security groups/NACLs in cloud deployments).

3. **Sandbox limitation, NOT a ClickHouse finding**: this sandbox's kernel
   lacks the `br_netfilter` module (`cat
   /proc/sys/net/bridge/bridge-nf-call-iptables` → "No such file"), so
   iptables `DOCKER-USER` DROP/ACCEPT rules had ZERO effect on traffic
   between two containers on the SAME bridge network — confirmed: rules
   were correctly listed by `iptables -L`, but a blocked port was still
   reachable. Loading `br_netfilter` requires a privileged `--pid=host`
   container running `modprobe`, a host-kernel-affecting action requiring
   explicit user approval that was requested but not granted within the
   session window; the safer default (no host kernel changes) was taken.
   **On any real Linux Docker host with `br_netfilter` loaded (the default
   on most standard cloud-init'd distros), `DOCKER-USER` iptables rules of
   the shape written in `scripts/firewall_s1.sh` DO enforce
   container-to-container port restriction** — this is Docker's officially
   documented mechanism. The rule syntax/ordering was validated in this
   session; end-to-end enforcement was not, and this report says so
   plainly rather than claiming a result that wasn't actually observed.

---

## 8. Runbook for the recommended options

### 8.1 S1 rollout (real-time freshness required)

1. Confirm the target tables use `ReplicatedMergeTree` with an EXPLICIT,
   literal (non-`{uuid}`) zoo_path — migrate existing bare-`ReplicatedMergeTree`
   tables to an explicit zoo_path BEFORE attempting to add an ext replica
   (bare form cannot support this pattern safely; see §2.2).
2. Stand up ext nodes with matching `{shard}` macro values and DISTINCT
   `{replica}` values, pointed at the SAME Keeper ensemble as main.
3. Create the table on ext with the identical `CREATE TABLE` statement
   (same zoo_path, same schema) — no `ON CLUSTER` needed, ext registers
   itself automatically.
4. Immediately apply a dedicated `readonly=1` profile + quota to any user
   that will connect from outside; explicitly `REVOKE SELECT ON
   system.zookeeper` from that user (found to be readable by default under
   `readonly=1`, §2.3).
5. Set `insert_quorum=auto` (never a fixed count including ext replicas)
   for any writer relying on quorum acknowledgment.
6. Configure `interserver_http_credentials` identically on both sides
   before opening any interserver network path.
7. Monitor: `system.replicas.active_replicas` vs `total_replicas` (gap =
   dead replica needing investigation or `SYSTEM DROP REPLICA`),
   `system.replication_queue` size (backlog under sustained ext
   unavailability), `absolute_delay` (freshness).
8. **Ext failure runbook**: transient (< Keeper `session_timeout_ms`) needs
   no action, auto-recovers. Permanent loss: `SYSTEM DROP REPLICA` on
   main, then on the (if ever recovered) orphaned node: `DROP TABLE ...
   SYNC` + `CREATE TABLE` with the SAME zoo_path and CURRENT schema (every
   ADD COLUMN applied since), full re-fetch from scratch.

### 8.2 S6 rollout (maximum isolation required)

1. Deploy main and ext with ZERO shared Docker network / VPC subnet /
   security group — verify with an actual cross-node connectivity test
   (§3.5's isolation-test finding shows why this must be verified, not
   assumed).
2. Point both sides at the SAME S3 bucket/endpoint via disjoint network
   paths (see §3.5's fixed topology as the reference pattern) — issue
   separate read-only and read-write bucket policies/IAM credentials for
   ext vs main if the object store supports it (not distinctly tested in
   this pass; flagged as a hardening item, same blank-shared-credential
   caveat as elsewhere in this report).
3. Schedule `BACKUP DATABASE ... TO S3(...) SETTINGS base_backup=<previous>`
   on a cadence matching the acceptable staleness window; monitor
   `system.backup_log` for duration/size trend and alert on anomalous
   growth (mutation-inflation effect, §3.2).
4. For "hot tail" refresh of a specific still-open partition: use the
   verified working recipe — `ALTER TABLE ... DROP PARTITION 'x'` on ext
   THEN `RESTORE TABLE ... PARTITIONS 'x' ... SETTINGS
   allow_non_empty_tables=1` — NOT the staging-table/REPLACE-PARTITION
   pattern, which does not work as documented for a literal-zoo_path
   `ReplicatedMergeTree` (§3.3).
5. **Ext failure runbook**: ext's already-restored data is fully
   independent; a permanent ext loss requires only re-provisioning a fresh
   node and re-running the last full+incremental RESTORE chain — main is
   never involved beyond serving the backup schedule.

---

## 9. Not tested (S2, S4, S5, S8) — documented, not fabricated

Per the task's own priority ordering and budget guidance ("four scenarios
with measurements beat nine with assumptions"), these were researched from
documentation only and NOT built:

- **S2 — `ENGINE=Replicated` database + `replica_group_name`**: per the
  official docs (`clickhouse.com/docs/reference/engines/database-engines/replicated`),
  a `Replicated` database splits DATA replication scope via
  `replica_group_name` while DDL replication remains shared across the
  whole database's Keeper path — but this was NOT independently verified
  in this session. Flagged as the natural next experiment: it may resolve
  the exact zoo_path/`{uuid}` pitfall from §2.2 automatically (the
  `Replicated` database engine manages UUID agreement across replicas by
  design) while retaining S1's write-isolation caveat, since replica-group
  members remain full peers of the underlying tables. Not tested.
- **S4 — Refreshable Materialized View (`REFRESH EVERY`)**: not built.
  Known from documentation to require checking this version's feature
  maturity flag before use; not independently confirmed GA/experimental
  status in this session, so no claim is made either way.
- **S5 — Scheduled `INSERT INTO FUNCTION remoteSecure(...) SELECT` by
  partition**: not built. The pull-vs-push, idempotency-via-staging
  question is directly analogous to S7's findings (§6.2) and S6's
  staging-table pitfall (§3.3) — likely to hit similar issues, but this is
  an inference, not a measurement, and is presented as such.
- **S8 — Shared S3 zero-copy / `s3_plain_rewritable` / read-only web disk**:
  not built. These features are explicitly marked experimental /
  not-recommended-for-production in ClickHouse's own documentation as of
  this version; not evaluated further per the task's own instruction to
  deprioritize experimental features.

---

## 10. Risks and what this solution does NOT give you

- **No native mechanism gives both real-time freshness AND full network/
  write isolation simultaneously.** Every choice is a trade on this axis;
  see the README's comparison matrix.
- **S1's write isolation is entirely a user/profile-layer property, not a
  replication-topology property.** A misconfigured or compromised ext user
  writes directly into production. This is not a hypothetical: it was
  reproduced directly in this research (§2.3).
- **S3's failure mode under sustained ext unavailability is dangerous by
  default** (unbounded disk growth) and, once bounded via
  `bytes_to_throw_insert`/`bytes_to_delay_insert`, produces a DIFFERENT
  dangerous failure mode: apparent insert failures that actually succeeded
  on the source table, inviting silent duplicate-producing retries (§4.4).
  This is the single riskiest mechanism evaluated in this project.
- **S6's "incremental" backups are not proportional to actual data change**
  when mutations/OPTIMIZE FINAL touch already-backed-up partitions (§3.2)
  — budget for cost spikes on mutation days, not steady-state.
- **`system.zookeeper` is readable by a readonly user by default** (found
  in S1, §2.3) — a genuine, easily-missed isolation gap that must be
  explicitly revoked, not assumed covered by `readonly=1`.
- **iptables/firewall enforcement for the "narrow list of allowed ports"
  requirement was NOT verified end-to-end** in this sandbox due to a
  missing kernel module (§7) — this is a testing gap in THIS environment,
  not a claim that the mechanism doesn't work in production; but it means
  this report cannot claim first-hand verification of the firewall layer,
  only of the application-layer mechanisms.
- **None of the tested mechanisms natively enforce column/row-level
  subsetting.** S1 requires an identical table; S3/S6/S7 can expose a
  DIFFERENT table (e.g. a curated view materialized into its own table)
  but this requires an additional layer built by the operator, not a
  built-in filter.

## 11. What changes in production

- **Separate DCs / latency to Keeper**: S1 and S2 require the external
  replica's Keeper round-trip latency to stay within tolerable bounds for
  session heartbeats (`session_timeout_ms`) and replication-log polling;
  cross-DC/cross-region Keeper latency (tens of ms) is a materially
  different regime than this sandbox's sub-millisecond same-host Docker
  networking, and was not evaluated under realistic WAN latency.
- **TLS / interserver credentials**: production MUST enable
  `interserver_https_port` (not just HTTP + shared password, as used in
  this sandbox for simplicity) and TLS on the native/HTTP client ports for
  any traffic crossing an untrusted network segment.
- **Firewall rules**: the exact port lists documented per scenario
  (§2.8 for S1: Keeper 9181 + interserver 9009; S3/S7: interserver 9009 +
  Keeper 9181 as applicable; S6: only the S3 endpoint) must be enforced via
  real firewall/security-group rules — validated for correct rule
  authoring in this project (§7), not for live enforcement.
- **Keeper ACLs**: not configured at all in this sandbox (default
  ClickHouse Keeper permits any authenticated connection full read/write
  to any znode it can reach). Production should configure Keeper-native
  ACLs restricting which credentials can read/write which znode subtrees,
  especially for S1/S7's cross-boundary Keeper access.
- **RBAC and quotas**: this sandbox used a single shared `default` user
  with a blank password in several scenarios (S3, S9) purely for test
  simplicity — production must never do this; every external-facing user
  needs its own credentials, `readonly` profile, and quota, as built for
  S1's `ext_reader` (§2.3), applied identically regardless of which
  scenario is chosen.
- **Capacity**: S3's queue-growth math (§4.4, ~20 bytes/row observed for
  this schema) and S1's Keeper znode growth under sustained load were
  measured at small scale only; production capacity planning at 30–100K
  rows/sec must extrapolate carefully and budget disk headroom on MAIN
  (not just ext) for any push-based or replica-based mechanism's queue/log
  growth during an extended external-side outage.
