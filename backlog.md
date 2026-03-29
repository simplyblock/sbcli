# FT=2 Testing Backlog

## Open Issues

### 1. Soak test script: shutdown nodes not restarted on fio failure
**Severity:** Script bug
**Found:** 2026-03-20 soak test iteration 7
**Description:** `test_dual_shutdown_30s_gap()` returns early when `check_all_fio` fails after the second shutdown, without restarting the shutdown nodes. This leaves nodes permanently offline and cascades into subsequent iterations.
**Fix:** Always restart shutdown nodes in a `trap` or finally block, regardless of fio check result.

### 2. Nodes stuck offline after dual shutdown — won't auto-recover
**Severity:** High — needs investigation
**Found:** 2026-03-20 soak test iteration 7
**Description:** After shutting down .112 (c5aa6c57) and .114 (08dc25cb) with 30s gap, both nodes stayed offline for 4+ minutes and never auto-recovered despite health check service running. `sbctl sn restart` was never called (script bug above), but the health check should have restarted them.
**Nodes affected:** .112 and .114 (still offline)
**Cluster:** 0fe6dbc2-1745-4f2a-8b42-09574c454cdb
**Questions:**
- Why didn't the health check auto-restart these nodes?
- Is SPDK still running on .112 and .114?
- What do the SNodeAPI/SPDK logs show on these nodes?

### 3. fio processes die during dual shutdown of non-adjacent nodes
**Severity:** Medium — needs investigation
**Found:** 2026-03-20 soak test iterations 7, and between iterations 2-3
**Description:** When two nodes are shut down (even with 30s gap), all fio processes report as dead (count=0). This happened when .112+.114 were shut down — these are primaries of vol2 and vol4. The remaining nodes (.113, .115) should still serve vol1 (primary .115) and vol3 (primary .113), so at least 2 fio instances should survive.
**Questions:**
- Did fio actually die or did `pgrep` fail?
- Were the NVMe paths on .113/.115 affected by the shutdown of .112/.114?
- Check dmesg on client (.111) for cascade errors

### 4. 14-minute gap between iterations 2 and 3
**Severity:** Low — may be related to issue #2
**Found:** 2026-03-20 soak test
**Description:** Iteration 2 ended at 12:14:08, iteration 3 started at 12:28:52 — a 14min gap instead of the expected 2min cooldown. During this gap, fio died and had to be restarted. The long gap suggests `wait_all_online` was polling for nodes that were slow to recover after the iteration 2 dual shutdown (.113 + .115).

### 5. `sn shutdown --force` may cascade to uninvolved nodes
**Severity:** High — needs investigation
**Found:** 2026-03-19 manual test step 5
**Description:** During manual testing, shutting down primary (.112) then sec2 (.114) caused ALL 4 nodes to go offline, including .113 and .115 which were not explicitly shut down. This cascade was not reproduced in the soak test but was observed once during manual testing.
**Questions:**
- Is this a shutdown side-effect (e.g., cluster map update taking down distribs)?
- Does the shutdown of a node's secondary trigger something on the primary?

## Passed Tests

- Single node shutdown/restart: primary, sec1, sec2 — all PASS
- Dual shutdown (30s gap): various combinations — mostly PASS (6/8 iterations)
- fio survives single-node outages reliably
- ANA failover works correctly for single-node failures
