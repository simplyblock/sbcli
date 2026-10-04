#!/usr/bin/env bash
# Disable swap on a stress-test host, persistently.
#
# Piped to the target over ssh by the docker stress workflows:
#     ssh ... "$SSH_USER@$ip" 'bash -s' < e2e/scripts/disable_swap.sh
#
# Why this exists
# ---------------
# The docker stress hosts were already out of memory headroom before the
# workload started. First memory sample of run
# n_plus_k_failover_multi_client_ha_all_nodes-20260905-232656, taken on
# 192.168.10.201 two minutes into the run:
#
#     Mem:   31Gi total, 30Gi used, 374Mi free, 339Mi available
#     Swap:  3.0Gi total, 171Mi used
#
# No kernel OOM kill fired on any of the four hosts, so this never showed up as
# an obvious failure. But SPDK pins hugepages and is latency critical: once the
# box starts swapping, poller threads stall and qpairs go delayed, which in the
# logs is indistinguishable from a storage fault. Keeping swap off makes memory
# exhaustion fail loudly (OOM) instead of quietly degrading into what looks
# like a product bug.
#
# Idempotent, and safe to run on a host that has no swap at all.
# /etc/fstab is backed up once to /etc/fstab.bak-stress before first edit.
set -eux

swapoff -a || true

# Persist across reboots. The original line is preserved after the marker so it
# can be restored by hand if a host ever needs swap back.
if [ -f /etc/fstab ]; then
    cp -n /etc/fstab /etc/fstab.bak-stress || true
    awk '$0 !~ /^[[:space:]]*#/ && $3 == "swap" { print "# disabled-for-stress " $0; next } { print }' \
        /etc/fstab > /etc/fstab.stress
    mv /etc/fstab.stress /etc/fstab
fi

# zram / zswap style setups bring swap up through systemd units rather than
# fstab, so those need masking too or they come back on the next boot.
for unit in $(systemctl list-units --type=swap --no-legend --plain 2>/dev/null | awk '{print $1}'); do
    systemctl stop "$unit" || true
    systemctl mask "$unit" || true
done

# Belt and braces for anything that re-enables swap behind our back.
sysctl -w vm.swappiness=0 || true

echo "--- swap state after disable ---"
free -h
swapon --show || echo "(no swap devices active)"
if grep -qE '^[^#]*[[:space:]]swap[[:space:]]' /etc/fstab 2>/dev/null; then
    echo "WARN: /etc/fstab still contains an active swap entry"
else
    echo "fstab: no active swap entries"
fi
