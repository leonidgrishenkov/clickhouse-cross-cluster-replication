#!/bin/bash
# Per-partition consistency check: count() and sum(cityHash64(*)) with and
# without FINAL, comparing two nodes. This is the core DoD "consistency"
# check from the task's uniform methodology (section 6.6).
#
# Usage: verify_consistency.sh <node_a_container> <node_b_container> <db.table>
set -euo pipefail

NODE_A="${1:?Usage: verify_consistency.sh <node_a> <node_b> <db.table>}"
NODE_B="${2:?}"
TABLE="${3:?}"

echo "== Consistency check: $NODE_A vs $NODE_B on $TABLE =="

echo "--- without FINAL ---"
docker exec "$NODE_A" clickhouse-client --query "
SELECT toYYYYMMDD(event_time) AS p, count(), sum(cityHash64(*))
FROM $TABLE GROUP BY p ORDER BY p FORMAT TSV" > /tmp/consistency_a_nofinal.tsv
docker exec "$NODE_B" clickhouse-client --query "
SELECT toYYYYMMDD(event_time) AS p, count(), sum(cityHash64(*))
FROM $TABLE GROUP BY p ORDER BY p FORMAT TSV" > /tmp/consistency_b_nofinal.tsv

if diff -q /tmp/consistency_a_nofinal.tsv /tmp/consistency_b_nofinal.tsv > /dev/null; then
  echo "PASS (no FINAL): identical per-partition count + cityHash64"
else
  echo "DIFF (no FINAL):"
  diff /tmp/consistency_a_nofinal.tsv /tmp/consistency_b_nofinal.tsv || true
fi

echo "--- with FINAL ---"
docker exec "$NODE_A" clickhouse-client --query "
SELECT toYYYYMMDD(event_time) AS p, count(), sum(cityHash64(*))
FROM $TABLE FINAL GROUP BY p ORDER BY p FORMAT TSV" > /tmp/consistency_a_final.tsv 2>/dev/null || echo "(FINAL not applicable / errored on $NODE_A)"
docker exec "$NODE_B" clickhouse-client --query "
SELECT toYYYYMMDD(event_time) AS p, count(), sum(cityHash64(*))
FROM $TABLE FINAL GROUP BY p ORDER BY p FORMAT TSV" > /tmp/consistency_b_final.tsv 2>/dev/null || echo "(FINAL not applicable / errored on $NODE_B)"

if [ -s /tmp/consistency_a_final.tsv ] && [ -s /tmp/consistency_b_final.tsv ]; then
  if diff -q /tmp/consistency_a_final.tsv /tmp/consistency_b_final.tsv > /dev/null; then
    echo "PASS (FINAL): identical per-partition count + cityHash64"
  else
    echo "DIFF (FINAL):"
    diff /tmp/consistency_a_final.tsv /tmp/consistency_b_final.tsv || true
  fi
fi
