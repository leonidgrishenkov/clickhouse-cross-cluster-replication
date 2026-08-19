# Cross-Cluster Replication in ClickHouse — Executive Summary

**Question**: can a production ClickHouse cluster replicate data into a separate,
maximally-isolated external cluster, using native mechanisms only?

**Answer: yes — but "maximally isolated" and "native replica" are mutually
exclusive.** ClickHouse offers a spectrum of native mechanisms, not one
solution. Every mechanism that gives you real-time (seconds) freshness
requires SOME durable coupling to the main cluster (a shared Keeper ensemble,
or main's willingness to make outbound connections to a node it doesn't
otherwise trust). Every mechanism that gives you TRUE isolation (main cannot
even resolve the external side's hostname) trades away real-time freshness
for a backup/restore or scheduled-pull cadence measured in minutes-to-hours.
There is no native option that gives you both simultaneously.

Five scenarios were built as working Docker environments, run through the
uniform test methodology (continuous load, mutations/DDL, chaos, isolation
tests, consistency checks), and measured. Four more (S2, S4, S5, S8) were
researched but not built, for budget reasons stated explicitly below — mark
them "not tested," not "confirmed."

ClickHouse version used throughout: **25.8.30.16** (25.8 LTS branch, released
2025-08-28, verified as the current LTS via the official 2025 changelog,
endoflife.date, and Docker Hub tag listing on the date of this research).

## The comparison matrix

| Scenario | Freshness (measured) | Isolation (write/network) | Impact on main under ext load | Resilience to ext failure | Ops complexity | Topology flexibility | Maturity |
|---|---|---|---|---|---|---|---|
| **S1** — ext node as additional replica | Seconds (real-time, same mechanism as main-to-main) | **Weak**: same physical table — a write via a misconfigured/compromised ext user reaches main directly (measured). Readonly profile fully mitigates this but is the ONLY protection. | **None measured**: 8 concurrent heavy queries on ext, main latency flat 0.011–0.054s | **Good**: main unaffected by ext down (30+ min tested); requires `SYSTEM DROP REPLICA` + rejoin recipe on permanent loss | **High**: shared Keeper is mandatory (S1c proved this), macros/UUID pitfalls, DDL-vs-ON-CLUSTER nuance, quorum tuning required | **Poor**: must be the exact same table (schema, ORDER BY); shard count differs OK, but no column/row subsetting without a view layer on top | GA, core mechanism, years old |
| **S6** — BACKUP/RESTORE via S3 | Minutes–hours (backup cadence); full restore 0.5s @ 300K rows, incremental backup NOT proportional to row delta when mutations occur (measured: 7.2MiB incr. vs 6.4MiB full after a 1-row mutation touching all partitions) | **Strongest of all tested**: zero direct network path once topology bug was found+fixed; only shared component is the bucket | **Minimal**: BACKUP runs on main but is a bounded, schedulable operation, not continuous load | **Best**: ext's already-restored data is 100% independent of main afterward — permanent main loss doesn't touch it at all | **Medium**: `RESTORE ... AS` rename is broken for literal-zoo_path ReplicatedMergeTree (found + worked around); partition-level restore is NOT idempotent without a DROP PARTITION first (found + fixed) | **Best**: any shard count, ORDER BY, or table subset — RESTORE only needs schema compatibility, not cluster topology match | GA |
| **S3** — MV push → Distributed | Seconds for new data; **zero** for pre-existing data (MV is insert-trigger only, confirmed) | **Structurally weak**: push model requires main to reach ext; direction is inherently bidirectional on typical L3 networks unless firewalled | **Severe under sustained ext outage**: unbounded disk growth on MAIN (~20.6 bytes/row measured; ~600KB–2MB/s at task-spec rates) unless `bytes_to_throw_insert`/`bytes_to_delay_insert` set explicitly (both default to 0/unbounded) | **Dangerous**: when the queue cap rejects an insert, the SOURCE table write still succeeds (confirmed) — client sees an error and may retry, silently duplicating data in the PRIMARY table, not just the pushed copy | **High**: POPULATE is incompatible with `TO <table>` syntax (hard syntax error, found); backfill requires a separate manual step with its own race window | **Good**: independent schema/ORDER BY on ext is natural since it's a plain INSERT target | GA, but the failure semantics found here are not well documented |
| **S9** — Distributed-only (anti-pattern) | Instant (no replication — always reads live from main) | **None**: confirmed INSERT from ext reaches main directly with default blank-password user | **Severe, measured**: main baseline latency degraded 20–33x (0.046s → 0.887–1.498s) during 6 concurrent ext-side heavy queries — ext queries execute ON main's own CPU/RAM | **None**: main down = ext 100% non-functional (`ALL_CONNECTION_TRIES_FAILED`, confirmed) | **Lowest** — no replication to configure at all | **N/A** — no independent data at all | GA (this is just `Distributed` + no local table) |
| **S7** — FETCH PARTITION + ATTACH | Minutes–hours (pull cadence, operator/cron-scheduled) | **Strong**: ext never registers as a replica (`system.replicas` on main never sees it); only reads a Keeper znode subtree + pulls over interserver HTTP, both ext-initiated | **Minimal**: bounded, scheduled pull operations, not continuous coupling | **Best of the "live" mechanisms**: no replica-set membership to clean up on permanent ext loss | **Medium**: FETCH+ATTACH must be run as one atomic unit — an interrupted cycle leaves a stuck `detached` part that blocks the next FETCH (`PARTITION_ALREADY_EXISTS`) until `DROP DETACHED PARTITION` with `allow_drop_detached=1` (found + scripted fix) | **Best**: partition-level granularity, any ext schema as long as the fetched partition's columns are compatible | GA, an older/simpler mechanism than most people realize |
| S2, S4, S5, S8 | **Not tested** | — | — | — | — | — | — |

## Conditional recommendation

- **Staleness must be seconds, topology matches (same schema)** → **S1**
  (external replica). Accept the Keeper coupling and the readonly-user
  write-isolation dependency; it is the only tested mechanism with real-time
  freshness. Use `insert_quorum=auto`, never a fixed count that includes ext
  replicas; use a dedicated `readonly=1` profile PLUS an explicit
  `REVOKE SELECT ON system.zookeeper` (found to be readable by a readonly
  user by default — a genuine gap).

- **Maximum isolation required, hours of staleness acceptable** → **S6**
  (BACKUP/RESTORE to S3). This is the only tested mechanism where main and
  ext can be on completely disjoint networks with zero direct reachability.
  Budget for the mutation-inflates-incremental-backup effect if the source
  tables see regular `ALTER UPDATE/DELETE`/`OPTIMIZE FINAL`.

- **Only a subset of columns/rows should be exposed** → **S6 or S7**, backing
  up/fetching a curated view-backed table rather than the raw source table
  (neither S1 nor S3 supports column/row subsetting without an extra
  materialized layer; S6/S7 only need schema compatibility with WHATEVER you
  choose to expose).

- **Seconds-freshness for a "hot tail" + strict isolation for the bulk of
  history (hybrid)**: incremental **S6** backups as the historical foundation
  (run every few minutes to hours, covering closed/immutable partitions only,
  where the mutation-inflation problem does not apply) plus **S3**'s MV push
  restricted ONLY to the still-open, hot partition, with
  `bytes_to_throw_insert` set conservatively and a documented alerting rule
  on `system.distribution_queue` growth. This bounds S3's dangerous failure
  mode (unbounded queue growth, silent retry-duplication) to the smallest
  possible surface — the current day's partition — while S6 keeps the
  isolation guarantee for everything older.

- **Never** default to **S9** for anything with a real production main
  cluster behind it. It is included only as the quantified reference point
  for "the cost of doing nothing": in this sandbox, six concurrent heavy
  queries on the "external" side degraded the main cluster's own baseline
  query latency by 20-33x, and a main outage takes the entire external side
  down with it. It has the lowest operational complexity of any scenario,
  which is exactly why teams reach for it first and exactly why it fails the
  isolation requirement completely, on every axis.

## Definition-of-Done checklist

- [x] Direct answer to "is this possible natively" — yes, five distinct
      native mechanisms confirmed; none gives full isolation AND real-time
      freshness simultaneously; see report.md for the boundary of "real"
      isolation for each.
- [x] At least 4 scenarios built in Docker and run through the uniform
      methodology — 5 built (S1, S6, S3, S9, S7).
- [x] Comparison matrix filled with numbers from results/*.csv.
- [x] Step-by-step rollout recipe, monitoring metrics, and failure runbook
      for the recommended option(s) — see report.md §Recommendation Rollout.
- [x] "Risks and what this solution does NOT give you" — see report.md.
- [x] "What changes in production" — see report.md.

## Assumptions and deviations from the task's defaults (flagged explicitly)

- Sandbox constrained to 4 vCPU / 7.8 GB RAM (after an initial 2 vCPU/1.9GB
  environment proved too small even for a single scenario; user re-provisioned
  mid-session). Scenarios were run SEQUENTIALLY (one scenario's containers up
  at a time), not all nine simultaneously, to give each the full resource
  budget. This is a deviation from "bring up all of S1..S9 at once" but
  preserves per-scenario measurement validity.
- Insert rate actually driven in tests: 2,000–500,000 row batches, largely
  synchronous single-shot INSERTs rather than a sustained 30–100K rows/sec
  streaming load for 15+ minutes as specified. Per-row disk/CPU costs were
  measured precisely (e.g. S3's ~20.6 bytes/row queue growth) and
  extrapolated to the target rate range; this is stated as an extrapolation,
  not a directly-measured sustained-load result, everywhere it's used.
- Main cluster topology for S1 was built at the full spec (2 shards × 2
  replicas); S6/S3/S9/S7 used single-node main + single-node ext (sufficient
  to exercise their respective mechanisms — BACKUP/RESTORE, MV push,
  Distributed routing, and FETCH PARTITION do not depend on shard count).
- iptables/`DOCKER-USER` port-restriction rules were written and confirmed
  syntactically correct, but this sandbox's kernel lacks the `br_netfilter`
  module, so end-to-end enforcement of "only port 9009/9181 allowed" could
  NOT be verified live. Genuine network segmentation (no shared docker
  network at all) WAS verified for S6 after an initial topology bug was
  found and fixed. This is documented, not glossed over, in LOG.md and
  report.md.
- S2 (Replicated database + replica_group_name), S4 (Refreshable MV), S5
  (scheduled remoteSecure), and S8 (zero-copy/shared S3) were **not built**,
  for time/budget reasons, per the task's own instruction to prioritize depth
  over breadth. They are marked "not tested" in report.md with a short
  paragraph each on what's known from documentation only — explicitly NOT
  presented as measured findings.
- clickhouse-copier: confirmed (not built, per task instruction) to be
  effectively deprecated/unmaintained for recent ClickHouse versions —
  moved to a separate `ClickHouse/copier` repository and no longer bundled
  with the server package. Not evaluated further.

See **report.md** for full per-scenario detail (configs, exact commands, raw
output), **LOG.md** for the hypothesis → action → result narrative as it was
generated in real time, and **results/*.csv** for the raw measurements
backing every number in the table above.
