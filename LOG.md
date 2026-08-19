# LOG.md — Hypothesis → Action → Result

Format per entry: **Hypothesis** → **What I did** → **What actually happened**.
Timestamps are wall-clock during the research session (single continuous run).

---

## Environment

- ClickHouse version pinned: `25.8.30.16` (25.8 LTS branch, released 2025-08-28,
  verified as latest LTS via https://clickhouse.com/docs/resources/changelogs/oss/2025,
  https://endoflife.date/clickhouse and Docker Hub tag listing on the date of this
  research session). Docker Hub image: `clickhouse/clickhouse-server:25.8.30.16` and
  `clickhouse/clickhouse-keeper:25.8.30.16`. Both pulled and confirmed running via
  `SELECT version()` -> `25.8.30.16`.
- Sandbox: 4 vCPU / 7.8GB RAM / 144GB disk (Docker 29.7.2, Compose v5.4.0).
  Idle ClickHouse server container footprint measured: ~224MB RSS, ~6% CPU
  (docker stats, single node, `--memory=600m` limit, 10s after start).
- clickhouse-copier status (checked before building anything): per the ClickHouse
  2024/2025 OSS changelogs and third-party sources (oneuptime.com blog, altinity),
  clickhouse-copier is no longer actively maintained/bundled with recent server
  packages, moved to a separate `ClickHouse/copier` repo. NOT built per task
  instructions; recorded as-is. Source: oneuptime.com/blog (2026-03-31 dated
  article) cross-checked against 2024 OSS changelog mentions of clickhouse-copier
  deprecation-adjacent entries.

## Pitfall discovered: ClickHouse Keeper Docker config directory

**Hypothesis**: ClickHouse Keeper, like clickhouse-server, merges extra config
from a `config.d/` subdirectory next to its main config file.

**What I did**: Mounted keeper XML config at
`/etc/clickhouse-keeper/config.d/keeper.xml` (by analogy with clickhouse-server's
`/etc/clickhouse-server/config.d/`), matching the 3-node raft_configuration.

**What actually happened**: Config was silently NOT merged. `keeper_config-preprocessed.xml`
still showed the image's default single-node config (`raft_configuration` with only
`localhost:9234`, `server_id=1` for every node). All 3 keeper containers ended up
running as three independent single-node ensembles, `tcp_port 9181` bound only to
`127.0.0.1`/`::1` (default `listen_host`), so cross-container connections were refused
(`Connection refused` from ClickHouse server pods trying `keeper-2:9181` /
`keeper-3:9181`). Root cause: ClickHouse Keeper's merge directory is
`/etc/clickhouse-keeper/keeper_config.d/` (note the `keeper_` prefix), NOT
`config.d/`. Confirmed via official docs
(https://clickhouse.com/docs/operations/configuration-files, accessed during this
session): "additional configuration files for Keeper need to be placed in
`/etc/clickhouse-keeper/keeper_config.d/`". Fixed by changing the compose mount
path; `docker compose down` + recreate; keeper logs then showed genuine 3-node
raft election, `Connection refused` errors disappeared, `SELECT * FROM
system.replicas` returned real cross-node state.

**Also encountered**: XML comments containing a literal `--` (e.g. "time -- NOT")
inside `<!-- ... -->` blocks caused a SAXParseException ("Invalid token") on
clickhouse-server startup — XML spec forbids `--` inside comment bodies, not just
as delimiters. All comments across configs were rewritten to avoid embedded `--`.

## S1a: External node as full-peer replica — CONFIRMED, with an important caveat

**Hypothesis** (from task spec): replication lives at the table/Keeper-path level;
`<remote_servers>` is only routing config for Distributed/ON CLUSTER. Therefore an
external node with matching macros (shard number matches main's shard, replica name
distinct) and the SAME explicit `zoo_path` in `ReplicatedMergeTree('/clickhouse/tables/{shard}/db/table','{replica}')`
becomes a genuine additional replica of that shard, entirely without appearing in
main cluster's `remote_servers`.

**What I did**:
1. Created `testgame.events` / `testgame.users_profile` as `ReplicatedMergeTree`
   with explicit zoo_path (avoiding the `{uuid}` default-path pitfall entirely —
   see below) via `ON CLUSTER main_cluster` on the 4 main nodes (2 shards x 2
   replicas).
2. On `ch-ext-s1r1` (shard=1, NOT in main_cluster's remote_servers, only
   connected via the same 3-node Keeper ensemble and matching macros
   `shard=1, replica=ext_r1`), ran the identical `CREATE TABLE` with the same
   zoo_path, no `ON CLUSTER`.
3. Inserted a row on `ch-main-s1r1`. Waited 5s.

**What actually happened**: `SELECT count()` on `ch-ext-s1r1` immediately showed
the row (`1`), full column values intact, confirmed via `system.replicas` showing
both `main_r1` and `ext_r1` as registered replicas of `testgame.events` /
`testgame.users_profile`, both `is_leader=1` (both can become leader — expected
default, `replicated_can_become_leader=1`), `is_readonly=0`. **CONFIRMED**: the
hypothesis holds exactly as stated — replica-set membership is purely a function
of the Keeper znode path + distinct replica_name, independent of `remote_servers`.

**Critical caveat found (not yet in task spec's framing)**: Because it is a true
peer replica of the SAME table, an `INSERT` issued directly against
`ch-ext-s1r1` (simulating what would happen if an external, nominally read-only
user's connection were somehow used to write, or if the read-only enforcement at
the user-profile layer failed) propagates back to the main cluster nodes
(confirmed: row with `event_type='ext_test_insert'` appeared on `ch-main-s1r1`
within 5s). **This means S1a's isolation property (d): "external users must not
be able to write or modify data" is NOT provided by the replication mechanism
itself — it is provided ENTIRELY by the readonly user/profile/quota layer on the
ext node.** If that layer is misconfigured, a write on the "external" cluster is
indistinguishable from a write on the main cluster from ClickHouse's point of
view: it is the same table. This is the single most important finding of S1 and
will be tested further under S1b (readonly enforcement) and the chaos tests
(insert_quorum, node-down).

## S1b: readonly-user enforcement — CONFIRMED at the user/profile layer

**What I did**: Created a dedicated `ext_reader` user with `profile=ext_readonly`
(`readonly=1`, `max_memory_usage=1.5e9`, `max_execution_time=30`,
`max_concurrent_queries_for_user=10`) and a `quota=ext_quota` (60s window, 1000
queries, 500M read_rows). Verified each setting name exists in this version's
`system.settings` / `system.merge_tree_settings` / `system.server_settings`
before using it (see below). Then, as `ext_reader` on `ch-ext-s1r1`, attempted
SELECT, INSERT, ALTER TABLE ... DELETE, and SYSTEM DROP REPLICA.

**What actually happened**:
- SELECT: succeeded (count() returned 2 rows, matching main).
- INSERT: rejected — `Code: 164. DB::Exception: ext_reader: Cannot execute
  query in readonly mode. (READONLY)`.
- ALTER TABLE ... DELETE: rejected with the same READONLY error.
- SYSTEM DROP REPLICA: rejected — `Code: 497. ACCESS_DENIED: Not enough
  permissions to drop these databases`.
**CONFIRMED**: `readonly=1` at the profile level fully blocks writes/mutations/
SYSTEM admin commands for that user, regardless of the fact that the underlying
table is a full peer replica capable of accepting writes from other users. This
is the correct place to enforce isolation property (c) ("no write/modify"), NOT
the replication topology itself — reinforcing the S1a finding above.

**Setting existence verified before use** (via `system.merge_tree_settings` /
`system.settings` on the running 25.8.30.16 container):
  - `always_fetch_merged_part` — EXISTS (merge_tree_settings, default `0`).
  - `replicated_can_become_leader` — EXISTS (merge_tree_settings, default `1`).
  - `max_concurrent_queries_for_user` — EXISTS (settings, default `0` = unlimited).
  - `max_memory_usage`, `max_execution_time`, `readonly` — EXIST (settings).
  Both `always_fetch_merged_part` and `replicated_can_become_leader` are
  **per-table MergeTree settings**, not user/profile settings — applied via
  `ALTER TABLE ... MODIFY SETTING` on the ext replica's table, confirmed to take
  effect: after `ALTER TABLE testgame.events MODIFY SETTING
  replicated_can_become_leader = 0`, `system.replicas` showed
  `can_become_leader=0` for that replica (was `1` before).

## Isolation gap found: system.zookeeper is readable by a readonly external user

**Hypothesis to check** (from task's uniform methodology, "Isolation test"):
does a readonly external user have any indirect visibility into main-cluster
internal state via `system.zookeeper`?

**What I did**: As `ext_reader` (readonly profile, no explicit
`system.zookeeper` grant), ran `SELECT count() FROM system.zookeeper WHERE
path=...` and `SELECT path FROM system.zookeeper WHERE
path='/clickhouse/tables/1/testgame/events/replicas'`.

**What actually happened**: Both queries succeeded and returned real data
(replica list under that znode path). **This is a genuine isolation hole**:
`system.zookeeper` access is not blocked by `readonly=1`; it requires an
explicit `GRANT`/`REVOKE` on the `system.zookeeper` virtual table (not tested
in this session but documented as the fix — `REVOKE SELECT ON
system.zookeeper FROM ext_reader` or, more robustly, only grant `SELECT` on
`testgame.*` explicitly rather than relying on the default `default` user's
broad grants that `ext_reader`'s profile inherited). Recorded in report.md
under the S1 isolation-test section as a required hardening step, not
optional.

## Sandbox limitation: iptables FORWARD rules did not restrict bridge-to-bridge traffic

**Hypothesis**: applying iptables rules on the `DOCKER-USER` chain (Docker's
documented hook for custom firewall policy that survives its own iptables
management) would let us enforce the declared port allow-list (ext -> main:9009,
ext -> keeper:9181, everything else DROPped) between containers on the shared
`net-repl` bridge network, and this could be verified end-to-end (block direct
client access on 9000, keep replication working through 9009).

**What I did**: Identified the actual container IPs on `s1-net-repl`, wrote
`scripts/firewall_s1.sh` to inject `iptables -A DOCKER-USER ...` rules via a
privileged `--net=host` helper container (no root/sudo available directly on the
sandbox host), applied a rule set that DROPs all ext<->main and ext<->keeper
traffic except tcp/9009 and tcp/9181 respectively.

**What actually happened**: The rules were accepted and listed correctly by
`iptables -L DOCKER-USER -n --line-numbers`, but had **no effect** on traffic
between two containers on the same Linux bridge (`br-<net-repl-id>`). A
`clickhouse-client --host ch-main-s1r1` from `ch-ext-s1r1` on port 9000 still
succeeded after the DROP rules were in place. Root cause: bridged traffic between
two ports of the *same* bridge is, by default, forwarded by the kernel's bridge
code before it ever reaches netfilter's `FORWARD` hook, unless the
`br_netfilter` kernel module is loaded and
`net.bridge.bridge-nf-call-iptables=1` is set. `cat
/proc/sys/net/bridge/bridge-nf-call-iptables` returned "No such file", meaning
the module was not loaded in this sandbox's kernel, and loading it requires a
privileged, `--pid=host` container running `modprobe` — a host-kernel-affecting
action I did not have standing user approval to perform (the user was asked and
did not respond within the session; I chose not to proceed rather than assume
consent).

**Consequence for this report**: this is a **sandbox artifact, not a ClickHouse
finding**. In any real Linux host/VM where `br_netfilter` is loaded (the default
on most distros used as Docker hosts, including standard `cloud-init`d Ubuntu
images) `DOCKER-USER` iptables rules of exactly this shape DO enforce
container-to-container port restrictions on a shared bridge — this is
Docker's officially documented mechanism
(https://docs.docker.com/engine/network/packet-filtering-firewalls/). I did
not fabricate a "it worked" result here; instead:
  1. The rule *syntax* and *placement* were validated (accepted by iptables,
     correct chain, correct order).
  2. Genuine network-level isolation for the rest of this research was instead
     demonstrated using Docker's native mechanism that IS fully enforced
     regardless of br_netfilter: containers not attached to a given
     docker-compose network cannot resolve or route to it at all. `ch-ext-*`
     containers are simply never attached to `net-main` (the network holding
     only main-cluster-internal traffic), which Docker enforces at the
     network-namespace level, not via netfilter — confirmed: `ch-ext-s1r1`
     cannot resolve `keeper-1` by any name that isn't advertised on a shared
     network, and has literally no interface routing to `net-main`'s subnet.
  3. The port-level allow-list (9009, 9181 only) is recorded as the *design
     target* to enforce with `DOCKER-USER` iptables rules (or an equivalent
     security-group/NACL in cloud deployments) in the "What changes in
     production" section of report.md, with an explicit flag that it was
     validated for correct rule syntax/ordering here but NOT validated
     end-to-end for enforcement due to this sandbox's kernel configuration.

## S1: DDL propagates via the per-shard replication log, NOT via ON CLUSTER

**Hypothesis to check**: the task spec assumes DDL propagation to the ext
replica requires an explicit fix (a shared "all" cluster covering both groups,
or a shared `/clickhouse/task_queue/ddl`), i.e. that ON CLUSTER main_cluster
(main-only) DDL will NOT reach the ext replica.

**What I did**: Ran `ALTER TABLE testgame.events ADD COLUMN yet_another_field
UInt8 DEFAULT 0` directly on `ch-main-s1r1`, with NO `ON CLUSTER` clause at
all (single-node ALTER, the normal case for a replicated table where the
statement itself gets written into the table's own replication log, then
`ON CLUSTER` is only a convenience wrapper that runs the same single-node
ALTER against every host listed in a cluster definition). Then checked the
column's presence on: `ch-main-s1r2` (same shard, in main_cluster),
`ch-ext-s1r1` (same shard, explicitly NOT in main_cluster's remote_servers),
and `ch-main-s2r1` (different shard, in main_cluster).

**What actually happened**: The column appeared on `ch-main-s1r2` (expected)
AND on `ch-ext-s1r1` (initially assumed this would require the fix) within 3
seconds — but did NOT appear on `ch-main-s2r1` (different shard). **This
overturns the task's framing**: for a `ReplicatedMergeTree`/`ReplicatedReplacingMergeTree`
table, `ALTER TABLE ... ADD COLUMN` (and, separately confirmed, `ALTER ...
DROP PARTITION`, `ALTER ... UPDATE/DELETE` mutations) are themselves written
as entries into the table's own replication log at its Keeper zoo_path — the
exact same mechanism that propagates INSERTs. They have NOTHING to do with
`ON CLUSTER` or `remote_servers`. `ON CLUSTER main_cluster` in the task's
"CREATE TABLE" step was only ever a convenience for fanning the *same DDL
text* out to N hosts in one client round-trip; once the table exists as a
replica of a given Keeper path, every subsequent schema-affecting ALTER on
ANY replica of that path (main or ext) is replicated to all other replicas
of that SAME path automatically, via the replication log, regardless of
`ON CLUSTER`, `remote_servers`, or which "cluster" concept the DDL was issued
under.

**Practical consequence — corrects the task's assumed risk**: "DDL: ON
CLUSTER over the main cluster will not touch the external nodes -> schema
drift risk" is **not accurate** for the specific case of ADD COLUMN / DROP
PARTITION / mutations issued as ordinary (non-ON-CLUSTER) ALTERs on
replicated tables, which is the common production pattern (an ops engineer
runs `ALTER TABLE x ADD COLUMN y` once against any single node of the shard,
relying on ReplicatedMergeTree to fan it out — NOT `ALTER ... ON CLUSTER`).
Schema drift IS a real risk only for DDL that does NOT go through the
replication log: `CREATE TABLE` / `DROP TABLE` / `RENAME TABLE` (metadata
operations that create/destroy the very znode path being watched), and any
DDL against non-replicated objects (plain `MergeTree`, dictionaries, views,
`Distributed` tables) — for those, `ON CLUSTER` (or a manual per-node
statement) genuinely is the only propagation path, and an "all_nodes_ddl"
cluster naming both groups (as built in config.d/common-main.xml) — or
running the statement by hand against both groups — is the only fix. This
distinction must be in the report's DDL section, not the blanket "DDL
requires a fix" framing from the task brief.

## Chaos: ext replica down 40+ minutes, mutation, and permanent-loss recipe

**What I did**: Stopped `ch-ext-s1r1` (`docker stop`), then over the outage
window: (a) ran 5 batches of 200-row inserts on main, (b) ran an `ALTER TABLE
... DELETE` mutation, (c) checked `system.replicas.active_replicas`,
`system.mutations.is_done`, keeper znode count under `.../events/log`,
and main-cluster baseline query latency throughout.

**What actually happened**:
- `active_replicas` correctly dropped from 3 to 2 the instant the container
  stopped (`replica_is_active` map showed `ext_r1:0`); `total_replicas`
  stayed 3 (registration is not removed automatically).
- Inserts on main succeeded with **no observable latency change**
  (`SELECT count()` on main: 0.003s before and during the outage — a dead
  *asynchronous* replica does not block writes or reads on the surviving
  replicas at all, confirming `insert_quorum=0` by default in this test —
  see the quorum test below for the alternative).
- The `ALTER ... DELETE` mutation completed (`is_done=1`) on main WITHOUT
  waiting for the dead ext replica — confirms mutations are asynchronous
  per-replica by default (`mutations_sync=0` default), each replica applies
  the log entry independently whenever it catches up; a dead replica just
  accumulates a backlog, it does not block others.
- Keeper znode count under the table's `/log` path grew from 16 to 17
  entries during the additional mutation (small, since only a few operations
  were issued in this short test window; at 30-100k rows/sec sustained for
  30+ minutes in the full task spec's regime, this log would grow much
  larger and is capped/truncated by ClickHouse's own log-entry retention —
  NOT tested at that scale in this sandbox; flagged as a documented gap).
- On restart, `ch-ext-s1r1` caught up to full parity (`count()` matched
  main) within ~13 seconds for the ~2000-row test dataset — this is NOT a
  meaningful throughput measurement at task-spec scale (30-100k rows/sec);
  it only confirms the *mechanism* (catch-up via replication log replay)
  works, not that it is *fast enough* at production volumes. Flagged as a
  gap: full-scale catch-up-time-vs-outage-duration curve was not measured.
- `SYSTEM DROP REPLICA 'ext_r1' FROM TABLE testgame.events` cleanly removed
  the dead replica from `system.replicas` (`total_replicas` 3 -> 2).
- **Recovery-after-permanent-loss recipe, confirmed empirically**: once
  dropped, the orphaned physical node (`ch-ext-s1r1`, if it comes back
  online with its old local table intact) is permanently stuck
  `is_readonly=1` for that table — it cannot rejoin by simply restarting.
  The only way to rejoin observed: `DROP TABLE ... SYNC` locally on the
  orphaned node, then `CREATE TABLE` again with the SAME zoo_path and an
  up-to-date schema (must include every `ADD COLUMN` applied since the
  node's last real membership, or the CREATE will fail/diverge) and a
  (potentially reused) replica_name. The node then re-fetches 100% of its
  partition data from the surviving replicas from scratch — there is no
  partial resync. This is the runbook procedure documented in report.md's
  "runbook for external cluster failure / permanent loss" section.

## Chaos: insert_quorum, query bomb, and network partition

**insert_quorum test**: with `ch-ext-s1r1` stopped, `insert_quorum=3` (all
3 replicas) correctly REJECTED an insert: `Code: 285. TOO_FEW_LIVE_REPLICAS:
Number of alive replicas (2) is less than requested quorum (3/3)`. With
`insert_quorum=auto` (majority = 2 of 3), the same insert SUCCEEDED
immediately with the ext replica still down. **Operational recommendation**:
if an external replica is added to a production table's replica set, NEVER
use a fixed `insert_quorum` that counts it (e.g. `insert_quorum=3` with 3
total replicas) — a single flaky/slow external node then has veto power over
every write to the main cluster. Use `insert_quorum=auto` (majority) or,
better, do not count external replicas in the quorum calculation at all by
keeping them structurally separate from the quorum-eligible main replicas
(ClickHouse does not currently support per-replica quorum exclusion directly;
the practical mitigation is: never set insert_quorum higher than the number
of MAIN replicas, and treat external replicas purely as extra fetch targets).

**Query bomb test**: launched 8 concurrent heavy GROUP BY / uniqExact queries
over ~500K rows as `ext_reader` (readonly, `max_concurrent_queries_for_user
=10`) against `ch-ext-s1r1`, while sampling `SELECT count(), avg(revenue_usd)
... WHERE country='US'` on `ch-main-s1r1` once per second. Main-side latency
stayed flat (0.011-0.054s per query throughout, no correlation with the ext
load) and `docker stats` showed `ch-main-s1r1` and `ch-ext-s1r1` CPU/RAM
completely independent (47% / 697MiB vs 17% / 476MiB respectively at the
same instant) — expected and confirmed: main and ext are separate OS
processes/containers, so a query bomb on the ext side cannot starve main's
CPU or RAM. The ONLY shared resource under a heavy ext read load is the
interserver fetch bandwidth/Keeper session traffic, which is small relative
to query execution cost and was not observed to create any measurable main
degradation in this test. This is the key resource-isolation upside of S1
relative to S9 (Distributed-only, tested later) — the tradeoff is the write/
security caveats documented above, not resource contention.

**Network disconnect/reconnect test**: `docker network disconnect
s1-net-repl s1-ch-ext-s1r1` cut ext_s1r1's only path to Keeper and main.
Immediately after, `system.replicas` on ext_s1r1 (queried through the docker
exec, which still works since docker exec does not need the disconnected
network) showed `is_readonly=0, is_session_expired=0` still — the Keeper
session had not timed out yet within the few seconds tested
(`session_timeout_ms=30000` in this environment's keeper config, so a
strictly-partitioned network gets ~30s of grace before the replica is
marked expired/readonly; not tested to the full 30s+ boundary in this
session due to time constraints — flagged as a gap). An insert issued on
main during the partition succeeded immediately (`replica_is_active` still
showed `ext_r1:1` at that instant, within the grace window). After
`docker network connect s1-net-repl s1-ch-ext-s1r1` (reconnect), ext_s1r1
caught up and picked up the row inserted during the partition within ~8s.
No special recovery action was needed for a transient network partition
shorter than the Keeper session timeout — this is a materially easier
recovery path than the permanent-loss case above (which requires the
DROP+CREATE recipe) precisely because the replica's Keeper session was
never invalidated.

## S1c: Split Keeper — CONFIRMED, "same zoo_path" requires the same physical Keeper ensemble

**Hypothesis to check** (task spec): "confirm or refute that one and the same
replicated table requires one and the same Keeper ensemble. Show exactly
what breaks."

**What I did**: Started an independent single-node Keeper ensemble
(`keeper-ext-1`, its OWN `raft_configuration` naming only itself, no
connection to `keeper-1/2/3`). Repointed `ch-ext-s2r1`'s `<zookeeper>` block
at `keeper-ext-1:9181` instead of the shared 3-node ensemble (kept the SAME
zoo_path string, `/clickhouse/tables/2/testgame/events`, and the same
replica_name `ext_r1`). Restarted the container. Because the old data
volume still referenced the shared-Keeper table state, the pre-existing
table went `is_readonly=1` and returned 0 rows (it could not find its own
registration on the new, unrelated Keeper). Then `DROP TABLE ... SYNC` and
recreated a FRESH `ReplicatedMergeTree` with the identical zoo_path/replica
string, now backed only by `keeper-ext-1`. Inserted a row locally on this
split node, and separately inserted a row on `ch-main-s2r1` (backed by the
shared 3-node ensemble).

**What actually happened**: Neither insert crossed over. The row inserted
locally on the split-Keeper node never appeared on main; the row inserted
on main never appeared on the split-Keeper node. **CONFIRMED**: the
identical zoo_path string is meaningless across two different physical
Keeper clusters — `/clickhouse/tables/2/testgame/events` on `keeper-ext-1`
and the path of the same name on `keeper-1/2/3` are two entirely unrelated
znode trees. "Same zoo_path" only produces shared replication when it
resolves within the SAME Keeper ensemble (verified by `<zookeeper>` node
list). There is no way to make two independent Keeper clusters serve as one
logical replication domain for a `ReplicatedMergeTree` table — Keeper/ZK
itself would have to be the shared component, which is exactly what S1a/S1b
rely on and what this experiment shows cannot be avoided. **This is the
definitive answer to "is a shared Keeper acceptable" for the S1 approach
specifically: it is not optional, it is structurally required.** The
isolation cost of S1 is therefore: the external side's replicas are
coupled to the main cluster's Keeper ensemble for as long as they are
members of that replica set — a Keeper outage/attack originating from the
main side can affect the external replica's ability to operate (it will go
`is_readonly=1`, unable to accept writes-through-INSERT-triggered-registration
or track merges, though existing local data remains queryable), and
conversely a badly-behaved external replica consumes Keeper session/watch
resources on the shared ensemble alongside production traffic. This
Keeper coupling is the fundamental, unavoidable cost of the S1 approach and
is the primary argument in report.md for preferring S6 (BACKUP/RESTORE via
S3) when true Keeper-level isolation is required.

**Cleanup**: split-Keeper experiment container and `keeper-ext-1` removed
after this test; not part of the default `make s1-up` topology (only started
via `make s1-up-s1c`, `--profile s1c`).




## S6: BACKUP/RESTORE to MinIO S3 — full + incremental, measured

**Environment**: two fully independent single-node ClickHouse clusters
(`ch-main`, own single-node Keeper `keeper-main-1`; `ch-ext`, own single-node
Keeper `keeper-ext-1`), on SEPARATE docker networks (`s6-net-main`,
`s6-net-ext`) with NO direct connectivity between them at all -- confirmed by
construction, not just by omission: `ch-main` and `ch-ext` do not share a
network attachment and cannot resolve each other's hostnames. Both attach to
a third network (`s6-net-s3`) that also holds `minio` + `mc`. This is
structurally the most isolated topology among all scenarios: the only
possible contact point is the S3 bucket.

**What I did**:
1. Loaded `testgame.events` (single-node `ReplicatedMergeTree`, matches the
   task's schema) with 300,000 rows across 4 daily partitions on `ch-main`.
2. `BACKUP DATABASE testgame TO S3('http://minio:9000/ch-backups/full_backup_1', ...)`.
3. `RESTORE DATABASE testgame FROM S3(same path)` on `ch-ext`.
4. Verified full row-for-row consistency: `count()` and
   `sum(cityHash64(*))` per partition matched exactly between main and ext
   (all 4 partitions, both counts and hashes bit-identical).
5. Inserted 50,000 more rows + ran `ALTER TABLE events UPDATE country='CA'
   WHERE user_id=1` (a single-row-matching mutation) on `ch-main`.
6. Ran an INCREMENTAL backup: `BACKUP DATABASE testgame TO S3(.../incr_backup_1)
   SETTINGS base_backup = S3(.../full_backup_1)`.

**What actually happened (measured)**:

| Metric | Full backup | Incremental backup |
|---|---|---|
| Wall time (BACKUP query) | 0.271s | 0.452s |
| num_files (system.backup_log) | 55 | 69 |
| uncompressed_size | 6,724,796 B (6.4 MiB) | 7,577,103 B (7.2 MiB) |
| MinIO object count (mc du) | 42 objects, 6.4 MiB | 25 objects, 7.2 MiB |
| RESTORE wall time | 0.505s | not separately re-measured (same mechanism) |

**Key finding -- incremental backup was NOT small relative to the change,
because ALTER UPDATE rewrites whole parts**: only 1 row out of 350,000
matched the mutation's WHERE clause, yet `system.parts` after the mutation
showed EVERY active part got a new mutation-version suffix
(`20260816_0_0_0` -> `20260816_0_0_0_1`, ditto for the 17th/18th/19th
partitions, plus a brand new part `20260819_1_1_0_2` for the freshly
inserted 50K rows). ClickHouse's classic (non-lightweight) `ALTER ...
UPDATE/DELETE` mutation mechanism rewrites the ENTIRE part containing any
matching row, not just the matching rows or a delta -- so the incremental
backup had to re-upload full rewritten copies of the 3 previously-unchanged
daily partitions (20260816-20260818) purely because their part name
changed, even though their content was byte-identical to before. This is
why the "incremental" backup (7.2 MiB) was barely different in size from
the full backup (6.4 MiB) despite the actual data change being ~50K new
rows + 1 updated cell: BACKUP with base_backup deduplicates at the part
level (unchanged part names/checksums are skipped), not at the row/byte
level, so any operation that produces new part names for old data
(mutations, OPTIMIZE FINAL, merges) defeats incremental-backup efficiency
for those partitions. This version does expose `enable_lightweight_update=1`
(verified via system.settings) as a default-on setting name, which is
ClickHouse's newer mechanism aimed at updating rows without a full part
rewrite -- evaluating whether it avoids this specific backup-bloat problem
was out of scope for this pass and is flagged as a follow-up, not tested
here.

**Practical consequence for the recommendation**: incremental
BACKUP/RESTORE is genuinely cheap ONLY for workloads dominated by
append-only INSERT into fresh partitions (the common telemetry/events
case, where old daily partitions are immutable after their day closes).
Any scenario with regular mutations, OPTIMIZE FINAL, or DROP PARTITION
touching already-backed-up partitions will inflate the "incremental"
backup back toward full-partition-copy cost for those partitions
specifically -- still cheaper than a full BACKUP of the whole database, but
proportional to the size of MUTATED partitions, not to the size of the
actual row-level change. This must be stated explicitly in the
recommendation and the "what changes in production" section: a Backup/
Restore pipeline sized for "small incremental deltas" will see cost spikes
on mutation days.

## S6: partition-level RESTORE pitfalls — two genuine dead ends found, one working recipe

**Hypothesis to check** (task spec): "RESTORE into staging plus ATTACH/
REPLACE PARTITION for minimal downtime."

**Pitfall 1 — `RESTORE TABLE ... PARTITIONS 'x' ... SETTINGS
allow_non_empty_tables=1` directly into the live, non-empty target table is
NOT idempotent.** Re-running the same partition-level RESTORE against
`testgame.events` (which already had 81,226 rows in partition 20260819)
resulted in 212,452 rows in that partition afterward — the restored parts
were added as NEW parts alongside the existing ones, with no deduplication
against already-present data. Repeating a partition backup/restore cycle
(e.g. a nightly job that re-backs-up "today's" still-open partition several
times before it closes) will silently duplicate every row each time it
runs, unless the target partition is dropped first. **Corrected recipe**:
`ALTER TABLE events DROP PARTITION 'x'` on the destination immediately
before `RESTORE TABLE events PARTITIONS 'x' ... SETTINGS
allow_non_empty_tables=1` — this makes the operation idempotent (old parts
of that partition are gone, so nothing to duplicate), at the cost of a
brief window where that one partition is empty on the destination (not
"minimal downtime" in the truest sense, but bounded to a single partition,
not the whole table).

**Pitfall 2 — `RESTORE TABLE src AS dst` does NOT let you actually stage
into a differently-named table when the source is a `ReplicatedMergeTree`
whose Keeper zoo_path is a literal string (not built from `{database}`/
`{table}` macros).** Attempted `RESTORE TABLE testgame.events AS
testgame.events_staging FROM S3(...)` three times (against a pre-existing
plain-`MergeTree` staging table: `CANNOT_RESTORE_TABLE`/schema mismatch,
since the comparison uses the backup's ORIGINAL `ReplicatedMergeTree(...)`
definition, not one implicitly rewritten for the new name; against a
pre-existing `ReplicatedMergeTree` staging table using its own distinct
zoo_path: same schema-mismatch error, RESTORE still compares to the
source's original zoo_path string, not the staging table's; and against NO
pre-existing table at all, letting RESTORE create it fresh): all three
failed. The no-pre-existing-table case failed with `Code: 253.
REPLICA_ALREADY_EXISTS: Replica /clickhouse/tables/1/testgame/events/
replicas/ext_r1 already exists` — because RESTORE recreates the table
using the LITERAL zoo_path string embedded in the backup's DDL
(`/clickhouse/tables/{shard}/testgame/events`, with `{shard}`/`{replica}`
resolved by the CURRENT node's own macros), completely ignoring the `AS
events_staging` rename for purposes of the Keeper path. Since the real
`testgame.events` table already occupies that exact zoo_path on this node,
any `AS`-renamed restore of the same source table collides with it every
time. **This means the documented "RESTORE into staging" pattern in the
task brief does not work as stated for a `ReplicatedMergeTree` table whose
zoo_path is a fixed literal string** — which is precisely the style of
zoo_path this report recommends elsewhere (S1) for avoiding the `{uuid}`
pitfall. The two recommendations partially conflict and the report must
say so plainly rather than assume they compose.

**Working alternative that WAS verified**: partition-scoped
DROP-then-RESTORE-with-allow_non_empty_tables directly into the real
target table (Pitfall 1's fix) is the recipe that actually achieves
idempotent, minimal-blast-radius partition refresh with this version's
RESTORE semantics — NOT the staging-table/REPLACE-PARTITION pattern the
task brief suggested, which does not work as stated for
ReplicatedMergeTree with a literal zoo_path. If true blue/green
zero-downtime partition swap is required, the practical fix is to restore
into a table using `ENGINE=MergeTree` (non-replicated, no zoo_path, so no
collision is possible) as the staging table, THEN `ALTER TABLE events
REPLACE PARTITION 'x' FROM events_staging` — not tested end-to-end in this
session due to time budget; flagged as the next experiment rather than
asserted as working.

**Follow-up test (closes the gap above)**: tried the "MergeTree staging +
REPLACE PARTITION" idea directly. `RESTORE TABLE testgame.events AS
testgame.events_stage_mt ... SETTINGS structure_only=1` (letting RESTORE
create the staging table itself, matching engine, rather than pre-creating
it) STILL failed with the same `REPLICA_ALREADY_EXISTS` /
`/clickhouse/tables/1/testgame/events/replicas/ext_r1` collision. **This
confirms the limitation is structural, not a staging-table-choice
workaround**: for a `ReplicatedMergeTree` source table whose zoo_path is a
literal (non-`{table}`-macro) string, `RESTORE ... AS <any-other-name>`
always tries to recreate a table AT THE SOURCE'S OWN zoo_path (ignoring the
destination name entirely for Keeper-path purposes), which collides with
the original table's own already-registered replica the moment both
tables are meant to coexist on the same node. The only way to restore this
specific source table under a different local name on the SAME node is if
the original table's zoo_path itself contains `{table}` (so the path
differs per table name) — a schema design choice that must be made BEFORE
backup, not fixed at restore time. **Practical, verified-working recipe for
minimal-downtime partition refresh, given this constraint**: partition-
scoped DROP-then-RESTORE-with-`allow_non_empty_tables=1` directly into the
live target table (Pitfall 1's fix, above) is the only pattern that
actually works in this version without a schema redesign. The
staging-table pattern from the task brief requires either restoring onto
a DIFFERENT node (no collision possible cross-node) or designing the
zoo_path with a `{table}` macro from day one; this must be stated plainly
as a limitation, not glossed over.

## S6: isolation test caught a real bug in this environment's own topology

**Isolation test performed per the uniform methodology** ("from an external
node, try to reach the main cluster's nodes on all ports, run INSERT, ALTER,
SYSTEM commands").

**What I did**: From `ch-ext`, ran `clickhouse-client --host ch-main --query
"SELECT count() FROM testgame.events"`, then, since that succeeded, escalated
to `INSERT INTO testgame.events (...) VALUES (...)` against `ch-main` FROM
the ext container.

**What actually happened — a genuine hole, not a hypothetical one**: BOTH
succeeded. `SELECT` returned real data (350,000 rows) and the `INSERT`
against `ch-main`, issued from `ch-ext`, succeeded and the row was
confirmed present on `ch-main` afterward (`count()
WHERE event_type='isolation_leak_test'` = 1). **Root cause: `ch-main` and
`ch-ext` are BOTH attached to `s6-net-s3` (needed so each side can reach
`minio` for `BACKUP`/`RESTORE`), and that shared network attachment gives
them full IP connectivity and DNS resolution to EACH OTHER too** — Docker
does not scope a bridge network attachment to "only reach one other
specific container on it"; being on the same network means full mutual
reachability on all ports, including ClickHouse's native protocol on 9000
with the `default` user's blank password. This defeats the entire premise
of S6 as "the bucket is the only point of contact" as originally built —
it is currently NOT true in this repository's `docker/s6/docker-compose.yml`
as first written.

**Fix required (documented, to be applied)**: `ch-main` and `ch-ext` cannot
share ANY docker network directly, including one that also holds MinIO.
The correct topology needs the S3 access split onto genuinely separate
paths — either (a) run two separate MinIO-reachable networks
(`net-main-s3`, `net-ext-s3`) both routed to the SAME external MinIO
endpoint via a reverse proxy/gateway container that itself has no
ClickHouse-native-protocol listener, so `ch-main` and `ch-ext` each only
ever see "a MinIO endpoint," never each other; or (b), simpler for this
sandbox, keep MinIO on its own network and connect `ch-main` and `ch-ext`
to it via SEPARATE bridge networks each containing only [that CH node,
minio] — Docker allows a container (`minio`) to be attached to multiple
networks simultaneously while the CH nodes on each network still cannot
see each other, because Docker's bridge isolation is per-network-pair, not
per-container-allowlist. This second fix was the one applied in this
research (see remediated topology note below) and re-verified.

**This finding matters beyond the sandbox**: it is a direct, hands-on
demonstration of a mistake that is easy to make in real infrastructure
too — "the bucket is the only point of contact" is a claim that must be
verified against the ACTUAL network topology, not assumed from the backup/
restore workflow alone. A team that puts main and external ClickHouse nodes
on a shared VPC/subnet "just for S3 access" (e.g. a shared NAT gateway
subnet, or peering both VPCs to a shared services VPC without further
segmentation) can end up with exactly this hole: full mutual reachability
via the client port, discovered only by an explicit isolation test — which
is exactly why the task's uniform methodology mandates one for every
scenario. This is now the single most important operational warning in
the S6 section of report.md.

**Remediation applied and reverified**: separated MinIO's network
attachment so `ch-main` and `ch-ext` each reach it via disjoint bridge
networks (`ch-main` + `minio` on one bridge, `ch-ext` + `minio` on a
second, separate bridge; `minio` itself dual-homed). After the fix,
`docker exec s6-ch-ext getent hosts ch-main` returns nothing (name
resolution failure) and `clickhouse-client --host ch-main` from `ch-ext`
times out / connection refused, while `BACKUP`/`RESTORE` against MinIO
continue to work unchanged from both sides.

**Verification of the fix (re-run, not just asserted)**: applied the
corrected `docker-compose.yml` (two disjoint `net-s3-main`/`net-s3-ext`
bridges, `minio` dual-homed across both, `ch-main`/`ch-ext` each on only
one). After `docker compose up -d` recreated the affected containers:
`docker exec s6-ch-ext getent hosts ch-main` -> empty/failure;
`clickhouse-client --host ch-main` from `ch-ext` -> `Code: 198.
DB::NetException: Not found address of host: ch-main. (DNS_ERROR)`.
`BACKUP DATABASE testgame TO S3(...)` on `ch-main` immediately afterward
still succeeded (`BACKUP_CREATED`), and the pre-existing MinIO bucket/
objects from earlier in this session were confirmed intact (`mc ls`
showed `full_backup_1/`, `incr_backup_1/`, `partition_backup_1/` still
present). The corrected topology is the one committed to
`docker/s6/docker-compose.yml` in this repository.





## S3: MV -> Distributed push — mechanism confirmed, backfill/POPULATE gaps found

**Environment**: `ch-main` and `ch-ext` on separate networks (`s3-net-main`,
`s3-net-ext`) with one explicit shared network (`s3-net-push`) carrying only
main-initiated INSERT traffic to ext's native protocol port. `testgame.events`
(ReplicatedMergeTree) on both sides + `testgame.events_push_dist`
(`Distributed(ext_push_cluster, testgame, events, rand())`) + a Materialized
View `events_push_mv` with `TO testgame.events_push_dist` reading `SELECT *
FROM testgame.events` on `ch-main` only.

**MV push mechanism — CONFIRMED working**: `INSERT INTO testgame.events` on
`ch-main` (both single-row and 10,000-row bulk inserts) triggered the MV,
which inserted into the Distributed table, which forwarded to `ch-ext`;
`count()` matched on both sides within ~3 seconds each time.

**Backfill gap — CONFIRMED, exactly as the task brief warned**: dropped the
MV, inserted 5,000 rows of "historical" data directly into
`testgame.events` on main (simulating data that existed before the MV was
ever created), then recreated the MV. The historical rows NEVER appeared
on ext (`count() WHERE event_type='pre_mv_data'` = 0 on ext, while main had
them). **An MV is purely an insert trigger scoped to inserts that occur
strictly after its own creation; it has zero visibility into rows already
present in the source table.**

**POPULATE + `TO <table>` incompatibility — a genuine hard limitation, not
previously documented in the task brief**: attempted `CREATE MATERIALIZED
VIEW ... TO testgame.events_push_dist POPULATE AS SELECT * FROM
testgame.events` to backfill in one atomic step. This FAILED outright:
`Code: 62. SYNTAX_ERROR: When creating a materialized view you can't
declare both 'TO [db].[table]' and 'POPULATE'`. **`POPULATE` is only valid
for MVs that own their own implicit storage (no explicit `TO` target); it
cannot be combined with the `TO <table>` form needed to push into an
existing Distributed table.** This means the "POPULATE race condition"
warning in the task brief is moot for THIS specific push pattern — you
cannot even attempt POPULATE with `TO`, so the race condition it warns
about literally cannot occur via POPULATE. The real-world workaround
(verified working): create the MV WITHOUT POPULATE first (so all NEW
inserts start flowing immediately, with no gap), then separately run a
manual `INSERT INTO events_push_dist SELECT * FROM events WHERE <historical
predicate>` to backfill. This introduces a DIFFERENT race: any row inserted
into `events` between "MV created" and "manual backfill query started" that
also matches the backfill predicate will be pushed TWICE (once by the MV,
once by the manual backfill) -- a genuine duplicate-on-retry/race scenario,
distinct from the one described in the task brief, and it must be handled
by either (a) using a backfill predicate with a strict upper timestamp
bound set to the exact MV-creation instant, or (b) accepting duplicates and
deduplicating on the ext side with ReplacingMergeTree + FINAL (only viable
if the table has a natural version/dedup key, which `events` here does
not — `users_profile` does).

## S3: distribution_queue growth, bytes_to_throw_insert, and a severe retry-duplication risk

**What I did**: Stopped ch-ext. Inserted 5 batches of 2,000 rows each
directly into testgame.events on main (each batch triggers the MV ->
Distributed push), sampling system.distribution_queue (count, data_files,
compressed bytes) after each batch. Then measured raw on-disk growth of the
Distributed table's queue directory for a single 50,000-row insert. Then
recreated events_push_dist WITH bytes_to_throw_insert=2000000,
bytes_to_delay_insert=500000, max_delay_to_insert=5 (verified these are
real, version-current Distributed engine table-level settings via the
official docs; NOTE: confirmed empirically that they can ONLY be set at
CREATE TABLE time -- ALTER TABLE ... MODIFY SETTING on a Distributed table
fails outright: Code: 48. NOT_IMPLEMENTED: Alter of type MODIFY_SETTING is
not supported by storage Distributed). With ext still down, inserted 8
more batches of 20,000 rows to force the queue past the configured
thresholds.

**What actually happened (measured)**:
- distribution_queue grew linearly and cleanly with each batch of 2,000
  rows: 41.33 KiB -> 82.67 KiB -> 124.01 KiB -> 165.25 KiB -> 206.57 KiB
  (~41.3 KiB per 2,000 rows = ~20.6 bytes/row on-disk in the queue file
  format for this schema). Separately, a single 50,000-row insert grew the
  queue directory by exactly 1,011,088 bytes on disk (~20.2 bytes/row,
  consistent). Extrapolated to the task spec's 30,000-100,000 rows/sec
  sustained rate: ~600 KB/s to ~2 MB/s of UNBOUNDED disk growth on the MAIN
  node for every second the external side is unreachable, with NO default
  cap (bytes_to_throw_insert and bytes_to_delay_insert both default to 0 =
  disabled/unbounded, confirmed for this version). This is a real
  operational risk: a multi-hour external outage at production insert
  rates could fill main's disk with pending Distributed-queue files alone.
- With the explicit settings applied, INSERT against the source table
  (which fires the MV -> Distributed push) started failing with Code:
  574. DISTRIBUTED_TOO_MANY_PENDING_BYTES: Too many bytes pending for
  async INSERT: 792.34 KiB (bytes_to_delay_insert=488.28 KiB): while
  pushing to view testgame.events_push_mv from batch 3 onward -- confirming
  the cap IS enforced and the queue growth stops (sum(data_compressed_bytes)
  stayed flat at 811,357 bytes through all subsequent failed batches,
  never reaching the table's own bytes_to_throw_insert=2000000 ceiling
  because bytes_to_delay_insert=500000 triggered the delay/throw path
  first at a lower threshold, as documented).
- Severe, non-obvious finding: despite the client receiving an EXCEPTION
  on every one of batches 3 through 8, SELECT count() FROM testgame.events
  WHERE event_type='throw_test' on MAIN showed all 160,000 rows present
  (8 batches x 20,000), including the 6 "failed" batches. The exception is
  raised by the Materialized View's push into the Distributed table, AFTER
  the row has already been durably written to the source events table --
  the local write is NOT rolled back when the downstream MV target
  rejects the insert. From the inserting client's point of view, this
  looks exactly like a failed INSERT (it received Code: 574), and the
  standard, correct client behavior on an apparent INSERT failure is to
  retry. Retrying an "apparently failed" batch insert in this
  configuration will silently double-write that batch into the source
  table (ClickHouse's insert deduplication only applies within a single
  Distributed/replicated insert block under specific conditions, not
  across two independently-issued INSERT statements with different
  literal data unless block-level deduplication hashes happen to match,
  which they will NOT for now()/generateUUIDv4()-based test data or any
  real telemetry event). This is arguably the single most dangerous
  operational property of the MV-push design found in this entire
  research project: a naive retry-on-error policy at the ingestion layer,
  combined with bytes_to_throw_insert/bytes_to_delay_insert protecting
  the ext-push queue, produces silent duplicate data in the PRIMARY
  production table, not just the pushed copy. Must be documented as a
  hard warning, not a footnote.
- Confirmed the queue drains automatically once ext comes back online:
  after docker start s3-ch-ext + ~25s, system.distribution_queue bytes
  returned to 0 and ext received the 40,000 rows from the two batches that
  DID make it into the queue file before the delay/throw threshold
  engaged (count() WHERE event_type='throw_test' = 40,000 on ext, matching
  batches 1-2 only -- batches 3-8's rows exist ONLY on main, permanently,
  since they never reached the Distributed queue at all). This is a
  second, separate data-completeness gap: the 120,000 rows from the
  rejected batches are NOT queued anywhere for later retry by ClickHouse
  itself -- they simply never left the source table. Any retry/backfill
  of that gap is the operator's responsibility, not automatic.

## S3: isolation test reveals a structural property of the PUSH model, not a fixable bug

**What I did**: Same isolation test as S6/S1 -- from ch-ext, tried to reach
ch-main directly (getent hosts, clickhouse-client --host ch-main).

**What actually happened -- and why this one is NOT the same class of
finding as the S6 network bug**: `ch-ext` CAN resolve and reach `ch-main`
over `s3-net-push` (`getent hosts ch-main` succeeds, `clickhouse-client
--host ch-main` succeeds). Unlike the S6 finding, this is NOT an
accidental topology mistake fixable by splitting the network -- it is a
STRUCTURAL property of the push model: main must be able to open an
OUTBOUND connection to ext (to push via Distributed), and a plain Docker
bridge network (or any L3-routable network without additional firewalling)
is inherently bidirectional -- there is no "outbound only" primitive at
the Docker network level. The SAME finding this session already made for
S1 applies here identically: `DOCKER-USER` iptables rules (or a cloud
security-group with directional/stateful rules) are REQUIRED to restrict
this to "main can reach ext:9000, ext cannot reach main:anything" -- and
this sandbox cannot validate iptables enforcement end-to-end due to the
missing br_netfilter kernel module (documented earlier in this LOG for
S1). This is recorded as a DESIGN REQUIREMENT for S3 in production
(direction-restricted firewall rule, main-initiates-only), not a bug to
fix in the compose file -- there is no compose-level fix available for a
model that inherently requires main to reach ext.

**This is an important comparative point for the recommendation**: S3
(push) has a STRICTLY WORSE default network posture than S6/S7 (pull)
specifically because the connection-initiating side is the more
security-sensitive one (main). In a pull model (S6, S7), the EXTERNAL side
initiates every connection, so from main's perspective there is no
listener/session for an attacker on the external side to abuse beyond
what's already exposed for the pull mechanism (S3(...) URL credentials
for S6; interserver HTTP fetch for S7). In S3, main's ClickHouse server
process is the one making outbound connections to ext's native protocol
port using the `default` user with a BLANK PASSWORD in this sandbox's
config (`users.d/users.xml`) -- meaning if `ch-ext` (or anything spoofing
its identity within the reachable network segment) were compromised, it
could not directly attack main over this specific channel (it's outbound
FROM main), but the underlying network fact that ch-ext can ALSO reach
main directly (since bridge/most L3 networks are bidirectional) means the
"only main can initiate" property is a firewall-rule commitment, not a
protocol-level guarantee, and must be independently enforced and tested in
real infrastructure -- exactly the same conclusion as S1's port-restriction
discussion.

## S9 (anti-pattern baseline): quantified cost of zero isolation

**Environment**: `ch-main` (single node, ReplicatedMergeTree `testgame.events`,
5,000,000 rows loaded) + `ch-ext` (single node, ZERO local tables, ZERO
Keeper connection at all -- only a `Distributed(main_from_ext, testgame,
events, rand())` table pointing at `ch-main`). This is the simplest possible
topology and the one most teams reach for first, precisely because it
requires no replication machinery at all.

**What I did**: measured baseline query latency on `ch-main`
(`SELECT count(), avg(revenue_usd) FROM testgame.events WHERE country='US'
AND event_time > now() - INTERVAL 7 DAY`, ~427K matching rows) before,
during, and after launching 6 concurrent CPU/memory-heavy queries from
`ch-ext` against `testgame.events_dist` (a full-table `ORDER BY
cityHash64(...)` forcing a real sort over 5M rows, each query independently
taking ~3.3s in isolation). Also tested: INSERT from ext against the
Distributed table (write-isolation test), and main-down / ext-still-running
(reverse chaos, the mirror image of every other scenario's "ext down, does
main survive" test).

**What actually happened (measured, this is the quantitative reference
point the task explicitly asked for)**:

| Condition | Main query latency |
|---|---|
| Baseline (before bomb) | 0.046s |
| During 6x concurrent ext-side sort-heavy queries | 0.887s, 0.999s, 1.377s, 1.498s, 0.892s (5 samples across the ~35s bomb window) |
| After bomb completed | 0.092s -> settled back to ~0.05s |

**Degradation factor: roughly 20x-33x baseline latency while the ext-side
query bomb ran**, on a query that touches a COMPLETELY DIFFERENT part of
the same table (a simple filtered count/avg vs. the bomb's full-table
sort) -- because in this architecture there is no process/resource
boundary at all between "external" queries and main's own query engine:
every query issued against `ch-ext`'s Distributed table is literally
EXECUTED on `ch-main`'s CPU, memory, and I/O, competing directly with
main's own production queries for the exact same resources. This is
categorically different from S1's query-bomb result (flat latency,
0.011-0.054s, no correlation) precisely because S1's ext replica is a
SEPARATE physical process — S9's ext node is not a separate execution
context at all, merely a routing shim.

**Write isolation: confirmed absent.** `INSERT INTO testgame.events_dist
(...)` issued from `ch-ext` succeeded and the row appeared on `ch-main`
within 2 seconds. Property (c) from the task's isolation definition ("must
NOT be able to write or modify data") fails completely and immediately in
this architecture with the default `default` user (blank password) -- there
is no readonly enforcement layer built into this scenario at all (unlike
S1's `ext_reader` profile), and even if one were added on the ext node's
OWN Distributed-table user, that only protects against a misbehaving
QUERY on ext; the ext node itself, and anyone with any access to it, is
one blank-password `clickhouse-client --user default` call away from
writing directly into production. This scenario has NO structural
write-protection at all -- protection would have to be bolted on entirely
via user/profile/quota config on the ext node, identical to the fix
needed in S1, but WITHOUT any of S1's actual data/resource isolation
benefits to offset it.

**Reverse chaos: main down -> ext is 100% non-functional.** Stopped
`ch-main`; `SELECT count() FROM testgame.events_dist` on ext immediately
failed with `Code: 279. ALL_CONNECTION_TRIES_FAILED` (nested `SOCKET_TIMEOUT`
/ `DNS_ERROR` once the container was fully gone). Ext holds precisely
ZERO bytes of usable data on its own -- it is not a cluster in any
meaningful sense, just a client-side query router with a persistent TCP
dependency on main being reachable and healthy at all times. This is the
diametric opposite of every other scenario tested (S1: ext degrades
gracefully and catches up later; S6/S7: ext's already-restored data
remains fully queryable even if main vanishes permanently; S3: ext's
already-pushed data remains queryable, only new data stops arriving).
**S9 provides ZERO resilience to main-cluster changes of any kind and ZERO
resilience to its own node's relationship with main -- it is strictly
worse than every other scenario on every isolation axis the task defined,
which is exactly its intended role as the anti-pattern baseline.**

## S7: ALTER TABLE ... FETCH PARTITION + ATTACH — confirmed, with an operational pitfall

**Environment**: main + ext, structurally identical to S6's separated
topology, but instead of a shared Keeper OR a shared S3 bucket, ext gets a
narrow `auxiliary_zookeepers` entry pointing ONLY at main's Keeper (read
access to main's znode tree), plus `interserver_http_credentials` matching
main's, to authenticate the actual part-fetch HTTP calls. ext's OWN table
is a completely independent ReplicatedMergeTree registered against its OWN
local Keeper (`keeper-ext-1`) -- it is NOT a replica of main's table at
all, unlike S1.

**What I did**: `ALTER TABLE testgame.events FETCH PARTITION '20260817'
FROM 'main_keeper:/clickhouse/tables/1/testgame/events'` on ext, then
`ALTER TABLE testgame.events ATTACH PARTITION '20260817'`. Repeated for
partition 20260819 (the "live", still-receiving-inserts partition), then
inserted MORE rows into that same partition on main, and re-ran the
fetch+attach cycle to test periodic-refresh idempotency.

**What actually happened**:
- First-time FETCH+ATTACH of a closed historical partition (20260817,
  3,121 rows): worked immediately, `count()` matched main exactly, 0.25s
  wall time for the ATTACH step.
- FETCH+ATTACH of the LIVE partition (20260819, 110,479 rows at fetch
  time): also worked, matched main's count at that instant.
- Refresh cycle after new inserts landed on main (500 more rows in the
  live partition): re-running FETCH+ATTACH picked up the delta correctly
  (110,479 -> 110,979, exact match with main) -- **when FETCH is
  immediately followed by its ATTACH in the same operational cycle**, this
  is a clean, idempotent "pull the latest snapshot of a partition" refresh.
- **Pitfall found**: if a FETCH's resulting detached part is NOT
  immediately consumed by ATTACH (e.g. a script crashes between the two
  steps, or an operator runs FETCH twice before attaching), the second
  FETCH fails outright: `Code: 256. PARTITION_ALREADY_EXISTS: Detached
  partition 20260819 already exists`, because FETCH lands the part under
  `detached/` using the partition ID as its identifying name and refuses
  to overwrite an existing detached part of the same name. Recovery
  requires `ALTER TABLE ... DROP DETACHED PARTITION` -- which is itself
  gated by `allow_drop_detached` (confirmed: default rejects it with
  `Code: 344. SUPPORT_IS_DISABLED`, must pass `SETTINGS
  allow_drop_detached=1` explicitly). **Any periodic FETCH+ATTACH cron
  script MUST treat the pair as a single atomic step (or add its own
  crash-recovery cleanup calling DROP DETACHED PARTITION with
  allow_drop_detached=1) or a single missed/interrupted cycle wedges every
  subsequent refresh of that partition until manually unblocked.**

**Coupling summary for S7 (narrower than S1, wider than S6)**: ext needs
(a) network reachability to main's Keeper client port for the
`auxiliary_zookeepers` read, and (b) network reachability to main's
interserver HTTP port (9009) to actually pull part data, using shared
`interserver_http_credentials`. ext NEVER writes a znode under main's
`/clickhouse/tables/...` path and is never visible in
`system.replicas` on main at all -- there is no replica-set membership,
no Keeper session for ext to lose, and no `SYSTEM DROP REPLICA` cleanup
ever required if ext goes away permanently (a strict operational
simplification over S1). The isolation cost is exactly two specific,
narrow, well-defined channels (Keeper client port read-only access +
interserver HTTP pull), both INITIATED BY EXT (a pull model, same
security-friendly initiator-direction property as S6) -- this makes S7 a
credible middle ground between S1 (full replica, richest but most tightly
coupled) and S6 (maximum isolation, but backup/restore cadence-limited
freshness) when partition-level granularity and ext-initiated pulls both
matter.

## S7: isolation test — same structural network caveat as S1/S3

`ch-ext` can resolve and reach `ch-main` directly over `s7-net-fetch`
(same Docker-bridge-is-bidirectional-by-default property already
documented for S1 and S3). This is the required channel for FETCH
PARTITION's interserver pull, so unlike S6's accidental leak this is not a
topology mistake -- it is the same "firewall enforcement is a commitment,
not a protocol guarantee" situation as S1 and S3, and needs the same
`DOCKER-USER`/security-group direction+port restriction in production
(interserver HTTP 9009 + Keeper client 9181, ext-initiated only). Recorded
once here rather than repeating the full iptables/br_netfilter discussion.
