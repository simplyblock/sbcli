#!/usr/bin/env bash
# enable_psi.sh -- turn on kernel Pressure Stall Information on the lab hosts.
#
# PSI answers "was this node starved of CPU, memory or IO?" directly, which is
# the question left open every time nodes stall together and nothing in the
# application logs accounts for it. The harness already samples /proc/pressure
# at each diagnostics checkpoint (CommonUtils.collect_pressure_stall), but on
# these hosts the file does not exist:
#
#   $ cat /boot/config-$(uname -r) | grep PSI
#   CONFIG_PSI=y
#   CONFIG_PSI_DEFAULT_DISABLED=y
#   $ cat /proc/cmdline
#   BOOT_IMAGE=... root=... ro crashkernel=... resume=...      <- no psi=1
#
# The feature is compiled in but off by default, so it needs psi=1 on the
# kernel command line and a reboot. That is the whole change; PSI's overhead is
# a few per-CPU counters and it is what the kernel's own cgroup pressure
# accounting uses in production.
#
# Usage:
#   ./enable_psi.sh                   # report current state on every host, change nothing
#   ./enable_psi.sh --apply           # add psi=1 to the boot args (NO reboot)
#   ./enable_psi.sh --apply --reboot  # add it and reboot each host, one at a time
#   ./enable_psi.sh --apply --reboot --parallel   # reboot them all at once
#
# Reboots are serialised by default and each host is waited back before the next
# is taken, so a storage cluster is never missing two nodes at once. Even so,
# only run this on an idle lab: on a storage node the spdk_4420 container has
# restart policy "no", so it does NOT come back by itself and the node stays
# offline in the cluster until the control plane restarts it. --parallel drops
# the serialisation and is for when the cluster is being recreated afterwards
# anyway.
#
# NFS: /mnt/nfs_share is mounted by hand on these hosts -- there is no fstab
# entry and no autofs -- so a reboot loses it and the next run has nowhere to
# write its logs. Every reboot here is followed by a remount.
set -uo pipefail

NFS_SRC="${NFS_SRC:-10.10.10.140:/srv/nfs_share}"
NFS_MNT="${NFS_MNT:-/mnt/nfs_share}"

SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519_sim}"
SSH_USER="${SSH_USER:-root}"
HOSTS="${HOSTS:-192.168.10.210 192.168.10.201 192.168.10.202 192.168.10.203 192.168.10.204 192.168.10.165 192.168.10.166}"
SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=15)

APPLY=0
REBOOT=0
PARALLEL=0
for a in "$@"; do
  case "$a" in
    --apply)    APPLY=1 ;;
    --reboot)   REBOOT=1 ;;
    --parallel) PARALLEL=1 ;;
    *) echo "unknown argument: $a" >&2; exit 2 ;;
  esac
done

run() { ssh "${SSH_OPTS[@]}" "${SSH_USER}@$1" "$2" 2>&1; }

wait_back() {
  local h=$1 deadline=$((SECONDS + 600))
  printf '    waiting for %s ' "$h"
  while [ $SECONDS -lt $deadline ]; do
    if run "$h" true >/dev/null 2>&1; then echo " up"; return 0; fi
    printf '.'; sleep 10
  done
  echo " TIMED OUT after 600s"
  return 1
}

# Nothing mounts /mnt/nfs_share at boot on these hosts, so put it back.
remount_nfs() {
  local h=$1
  run "$h" "
    mountpoint -q '$NFS_MNT' && { echo '    nfs already mounted'; exit 0; }
    mkdir -p '$NFS_MNT' 2>/dev/null
    mount -t nfs4 '$NFS_SRC' '$NFS_MNT' 2>&1 \
      || mount -t nfs '$NFS_SRC' '$NFS_MNT' 2>&1
    if mountpoint -q '$NFS_MNT'; then
      echo \"    nfs remounted: \$(findmnt -no SOURCE '$NFS_MNT')\"
    else
      echo '    NFS REMOUNT FAILED'; exit 1
    fi
  "
}

post_boot() {
  local h=$1
  remount_nfs "$h"
  run "$h" '
    if [ -d /proc/pressure ]; then
      printf "    PSI ON  -- cpu: %s\n" "$(head -1 /proc/pressure/cpu)"
    else
      echo "    PSI STILL OFF (/proc/pressure absent)"; exit 1
    fi'
}

rc=0
PENDING=""
for h in $HOSTS; do
  echo "===== $h"
  state=$(run "$h" '
    printf "  /proc/pressure : "
    if [ -d /proc/pressure ]; then echo "PRESENT"; else echo "absent"; fi
    printf "  cmdline psi=1  : "
    if grep -qw "psi=1" /proc/cmdline; then echo "yes"; else echo "no"; fi
    printf "  CONFIG_PSI     : "
    grep -hE "^CONFIG_PSI(_DEFAULT_DISABLED)?=" /boot/config-$(uname -r) 2>/dev/null | paste -sd" "
  ')
  echo "$state"

  if ! grep -q "CONFIG_PSI=y" <<<"$state"; then
    echo "  SKIP: kernel was not built with CONFIG_PSI=y; psi=1 would do nothing."
    continue
  fi
  if grep -q "PRESENT" <<<"$state"; then
    echo "  already enabled, nothing to do."
    continue
  fi
  if [ "$APPLY" -ne 1 ]; then
    echo "  would run: grubby --update-kernel=ALL --args=\"psi=1\"   (re-run with --apply)"
    continue
  fi

  echo "  applying psi=1 to all installed kernels..."
  out=$(run "$h" '
    command -v grubby >/dev/null || { echo "NO_GRUBBY"; exit 1; }
    grubby --update-kernel=ALL --args="psi=1" >/dev/null 2>&1 || { echo "GRUBBY_FAILED"; exit 1; }
    echo "  now set to: $(grubby --info=DEFAULT | grep -m1 ^args=)"
  ')
  echo "$out"
  case "$out" in
    *NO_GRUBBY*)     echo "  FAILED: grubby not installed; edit GRUB_CMDLINE_LINUX by hand."; rc=1; continue ;;
    *GRUBBY_FAILED*) echo "  FAILED: grubby could not update the kernels."; rc=1; continue ;;
  esac

  if [ "$REBOOT" -ne 1 ]; then
    echo "  NOT rebooting. psi=1 takes effect on this host's next boot."
    continue
  fi

  echo "  rebooting..."
  run "$h" 'nohup sh -c "sleep 2; systemctl reboot" >/dev/null 2>&1 &' >/dev/null
  PENDING="$PENDING $h"

  if [ "$PARALLEL" -eq 1 ]; then
    continue                     # collect them all, wait once at the end
  fi
  sleep 15
  if ! wait_back "$h"; then rc=1; continue; fi
  post_boot "$h" || rc=1
done

# --parallel: every host was told to reboot without waiting; wait for them now.
if [ "$REBOOT" -eq 1 ] && [ "$PARALLEL" -eq 1 ] && [ -n "${PENDING// /}" ]; then
  echo
  echo "===== waiting for:$PENDING"
  sleep 15
  for h in $PENDING; do
    if ! wait_back "$h"; then rc=1; continue; fi
    post_boot "$h" || rc=1
  done
fi

if [ "$REBOOT" -eq 1 ] && [ -n "${PENDING// /}" ]; then
  echo
  echo "===== final state"
  for h in $PENDING; do
    printf '  %-16s psi=%-7s nfs=%s\n' "$h" \
      "$(run "$h" '[ -d /proc/pressure ] && echo ON || echo OFF')" \
      "$(run "$h" "mountpoint -q '$NFS_MNT' && echo mounted || echo MISSING")"
  done
  echo
  echo "Storage nodes: spdk_4420 has restart policy 'no', so SPDK is down on"
  echo "every rebooted storage node and the cluster needs restarting/recreating."
fi
exit $rc
