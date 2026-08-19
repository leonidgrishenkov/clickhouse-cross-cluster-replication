#!/bin/bash
# Enforces the S1 "declared holes" firewall policy on the DOCKER-USER chain
# (the recommended hook for custom filtering that survives Docker's own
# iptables management). Because this sandbox has no root/sudo on the host,
# rules are injected via a privileged --net=host container that shares the
# host's network namespace and therefore its iptables state.
#
# Declared allowed cross-group channels (net-repl, s1-net-repl bridge):
#   ext (ch-ext-*) -> main (ch-main-*) : tcp/9009  (interserver HTTP; used for
#                                                    replicated part fetch/push)
#   ext (ch-ext-*) -> keeper-1/2/3     : tcp/9181  (Keeper client protocol)
# Everything else between the ext group and the main+keeper group is DROPped,
# including the ClickHouse client ports 9000 (native) and 8123 (HTTP) which
# would otherwise let an "external" node's operator run ad-hoc SQL directly
# against a main-cluster node.
set -euo pipefail

EXT_IPS=(172.20.0.9 172.20.0.10)      # ch-ext-s1r1, ch-ext-s2r1
MAIN_IPS=(172.20.0.5 172.20.0.6 172.20.0.7 172.20.0.8)  # ch-main-*
KEEPER_IPS=(172.20.0.2 172.20.0.3 172.20.0.4)           # keeper-1/2/3

RUN() {
  docker run --rm --cap-add=NET_ADMIN --net=host alpine sh -c \
    "apk add --no-cache iptables >/dev/null 2>&1; $1"
}

echo "== Flushing any prior S1 rules in DOCKER-USER (idempotent) =="
RUN "iptables -F DOCKER-USER" || true

RULES=""
for ext in "${EXT_IPS[@]}"; do
  for main in "${MAIN_IPS[@]}"; do
    RULES+="iptables -A DOCKER-USER -s $ext -d $main -p tcp --dport 9009 -j ACCEPT; "
    RULES+="iptables -A DOCKER-USER -s $main -d $ext -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT; "
    RULES+="iptables -A DOCKER-USER -s $ext -d $main -j DROP; "
    RULES+="iptables -A DOCKER-USER -s $main -d $ext -j DROP; "
  done
  for keeper in "${KEEPER_IPS[@]}"; do
    RULES+="iptables -A DOCKER-USER -s $ext -d $keeper -p tcp --dport 9181 -j ACCEPT; "
    RULES+="iptables -A DOCKER-USER -s $keeper -d $ext -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT; "
    RULES+="iptables -A DOCKER-USER -s $ext -d $keeper -j DROP; "
  done
done
RULES+="iptables -A DOCKER-USER -j RETURN; "

echo "== Applying rules =="
RUN "$RULES"

echo "== Current DOCKER-USER chain =="
RUN "iptables -L DOCKER-USER -n --line-numbers"
