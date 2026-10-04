#!/usr/bin/env bash
# probe_bin.sh -- prove WHICH build is running, from inside the spdk container.
#
# An mtime is too weak an identity. ultra:main-latest's manifest list pointed
# amd64 at a five-day-old image and three soak runs were blind before anyone
# noticed. The ultra Dockerfile bakes `git log` of both repos into the image
# (docker/Dockerfile_spdk_ultra lines 11 and 13) and copies the whole spdk tree
# into the runtime stage (line 59), so the running image can be pinned down to
# the commit and the source can be read directly.
set -u

echo "  --- spdk commit baked into the image ---"
if [ -f /root/spdk/git_log.txt ]; then
    head -4 /root/spdk/git_log.txt | sed 's/^/    /'
else
    echo "    FAIL: /root/spdk/git_log.txt missing"
fi

echo "  --- ultra commit baked into the image ---"
if [ -f /root/spdk/ultra/git_log.txt ]; then
    head -4 /root/spdk/ultra/git_log.txt | sed 's/^/    /'
else
    echo "    FAIL: /root/spdk/ultra/git_log.txt missing"
fi

# spdk/spdk#3686: nvme_bdev_io.retry_count and submit_tsc were never
# initialised at submit while driver_ctx is never zeroed, so a recycled bdev_io
# could carry a stale retry count and fail a retryable abort straight up as
# EIO. Upstream d528e1a67 zeroes both at the submission entry point; an earlier
# local attempt zeroed them at completion instead, which misses a bdev_io whose
# previous occupant was another bdev module. These two greps tell the builds
# apart with no ambiguity.
echo "  --- retry-state init (upstream d528e1a67, fixes spdk/spdk#3686) ---"
SRC=/root/spdk/module/bdev/nvme/bdev_nvme.c
if [ -f "$SRC" ]; then
    printf '    [%s] bdev_nvme_submit_request_initial defined   (want 1)\n' \
        "$(grep -c '^bdev_nvme_submit_request_initial' "$SRC")"
    printf '    [%s] fn_table .submit_request wired to it       (want 1)\n' \
        "$(grep -c '\.submit_request.*bdev_nvme_submit_request_initial' "$SRC")"
    printf '    [%s] nbdev_io->retry_count zeroed at submit     (want 1)\n' \
        "$(grep -c 'nbdev_io->retry_count = 0;' "$SRC")"
    printf '    [%s] bio->retry_count zeroed at completion      (want 0, superseded)\n' \
        "$(grep -c 'bio->retry_count = 0;' "$SRC")"
else
    echo "    FAIL: $SRC missing from image"
fi

echo "  --- placement instrumentation in the ultra binary ---"
B=/root/spdk/ultra/build_bdts/bdts
[ -f "$B" ] || { echo "    FAIL: $B missing"; exit 0; }
stat -c '    size=%s  mtime=%y' "$B"
for p in "fault tolerance degraded" "anti-affinity dropped" "Failed to find available location"; do
    printf '    [%s] %s\n' "$(strings -a "$B" 2>/dev/null | grep -c -F -- "$p")" "$p"
done
