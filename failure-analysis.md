# FT=2 Soak Test Failure Analysis — 2026-03-20

## Context
- Cluster: `0fe6dbc2-1745-4f2a-8b42-09574c454cdb`
- Soak test iteration 7: dual shutdown of .112 (c5aa6c57) + .114 (08dc25cb) with 30s gap
- After shutdown, fio died on all 4 volumes (reported 0 processes)
- Nodes .112 and .114 stuck offline, never auto-recovered
- Nodes .113 and .115 remained online

## Current State
- .112 (c5aa6c57): OFFLINE — SNodeAPI running, SPDK status unknown
- .113 (d4339392): ONLINE
- .114 (08dc25cb): OFFLINE — SNodeAPI running, SPDK status unknown
- .115 (4e03ae27): ONLINE

## Volume-to-NVMe Device Mapping

From `nvme list-subsys` (current state, some paths already removed):

| Volume | NQN (tail) | Primary | Sec1 | Sec2 | NVMe devs (originally) |
|--------|-----------|---------|------|------|----------------------|
| vol1 (0e428c8f) | lvol:0e428c8f | .115 (4e03ae27) | .112 (c5aa6c57) | .113 (d4339392) | nvme0(.115), nvme1(.112), nvme2(.113) |
| vol2 (45848555) | lvol:45848555 | .112 (c5aa6c57) | .113 (d4339392) | .114 (08dc25cb) | nvme3(.112), nvme4(.113), nvme5(.114) |
| vol3 (31a87393) | lvol:31a87393 | .113 (d4339392) | .114 (08dc25cb) | .115 (4e03ae27) | nvme6(.113), nvme7(.114), nvme8(.115) |
| vol4 (87e0453d) | lvol:87e0453d | .114 (08dc25cb) | .115 (4e03ae27) | .112 (c5aa6c57) | nvme9(.114), nvme10(.115), nvme11(.112) |

**NOTE:** To correlate NVMe devices to volumes in general, search dmesg for `new ctrl.*NQN` entries — these show the NQN (which contains the lvol UUID) and the nvme device name at connect time. The NQN identifies the volume, and the traddr identifies the storage node.

## Investigation Steps

### Step 1: Check client dmesg for volume path failures

**Problem:** dmesg ring buffer on .111 has wrapped. Only contains 10:12–10:47 timeframe. The failure at 12:46 is gone. journalctl also empty for that window.

**What we found in the remaining dmesg:**
- At 10:47:03: nvme1(.112→vol1), nvme3(.112→vol2), nvme11(.112→vol4) all **removed** (ctrl-loss-tmo reached)
- At 10:47:51: nvme5(.114→vol2), nvme7(.114→vol3), nvme9(.114→vol4) all **removed**
- These are ALL paths to .112 and .114 — confirming both nodes' SPDK has been down since before 10:12
- `Property Set error: 880, offset 0x14` on multiple devices after removal

**Remaining live paths (from nvme list-subsys):**
- vol1: nvme0(.115), nvme2(.113) — 2 paths, missing nvme1(.112) ← removed
- vol2: nvme4(.113) — 1 path only, missing nvme3(.112) + nvme5(.114) ← both removed
- vol3: nvme6(.113), nvme8(.115) — 2 paths, missing nvme7(.114) ← removed
- vol4: nvme10(.115) — 1 path only, missing nvme9(.114) + nvme11(.112) ← both removed

**Surviving path analysis (online nodes are .113 and .115 only):**

| Volume | Primary | Surviving paths | Role of surviving path |
|--------|---------|----------------|----------------------|
| vol1 (0e428c8f) | .115 (ONLINE) | nvme0(.115), nvme2(.113) | Primary + sec2 — 2 paths |
| vol2 (45848555) | .112 (OFFLINE) | nvme4(.113) | **sec1 (non-optimized)** — 1 path |
| vol3 (31a87393) | .113 (ONLINE) | nvme6(.113), nvme8(.115) | Primary + sec2 — 2 paths |
| vol4 (87e0453d) | .114 (OFFLINE) | nvme10(.115) | **sec1 (non-optimized)** — 1 path |

vol1 and vol3 have their primaries online — fully functional with 2 paths each.
vol2 and vol4 each have 1 surviving path on sec1 (non-optimized ANA). A single non-optimized path should serve IO reliably.

**Open question:** If vol1/vol3 primaries are online and vol2/vol4 have a working non-optimized path, why did ALL fio processes die?

### Step 2: Check SPDK logs on ONLINE nodes (.113, .115)

**Approach:** Look at `spdk_808x` container logs (NOT SNodeAPI, NOT spdk_proxy). These are the actual SPDK data-plane logs.

**Note on dmesg:** Client (.111) dmesg ring buffer has wrapped — only covers 10:12-10:47 UTC. The failure at 10:46 UTC (12:46 local) is partially visible but the connect events (NQN mapping) are gone. To correlate NVMe devices to volumes in general, search dmesg for `new ctrl.*NQN` at connect time.

**Note on timestamps:** Lab nodes use UTC. Local time = UTC + 2h. Failure at local 12:46 = UTC 10:46.

**Findings on .113 (spdk_8081) at UTC 10:46:00:**

1. `JC (jm_vuid=8233) reset lock ********** dt=30156.350368` — journal lock reset after 30156s (~8.4h) of inactivity
2. **ERROR:** `JC [remote_jm_c5aa6c57-6f9b-4309-8439-f0766769fa0dn1] helper_sync_setter: jfi_w_set_crossjm_sync_id failed, JM is excluded from further operation.`
   - This is the journal controller detecting that the remote JM on c5aa6c57 (.112, shut down) is gone
   - **EXPECTED:** With .112 down, its remote JM on .113 should fail. Each node has 4 journals (1 local + 3 remote). With 2 nodes down (.112 + .114), exactly 2 remote JMs should fail — no more.
3. After the error: all 3 `alceml_*` io_channels released (`spdk_put_io_channel`)
4. At 10:46:02: `bdev_open_ext: Currently unable to find bdev with name: remote_jm_c5aa6c57-6f9b-4309-8439-f0766769fa0dn1` — retry connect failing
5. IO redirect continues: `t[128] c[0] f[0] tc[0]` — 128 total redirects, 0 completed, 0 failed

**Note:** The io_channel teardown for all 3 alceml devices after remote JM failure is **expected** — the remote ALCEML devices on the offline nodes are all disconnected.

**Expected errors (both nodes show same pattern):**
- JM retry connect warnings for both offline nodes (c5aa6c57/.112 and 08dc25cb/.114) — every ~5s, expected
- `helper_sync_setter: jfi_w_set_crossjm_sync_id failed, JM is excluded` — expected, 2 remote JMs gone

**Unexpected DISTRIB errors (repeating every ~10s):**

On .113:
```
DISTRIB : failure on cb_jc_read_next_completed: vuid=8261  res=-33  res_bdev=-1,-1,-1
DISTRIB : failure on cb_jc_read_next_completed: vuid=7395  res=-33  res_bdev=-1,-1,-1
```

On .115:
```
DISTRIB : failure on cb_jc_read_next_completed: vuid=9642  res=-33  res_bdev=-1,-1,-1
DISTRIB : failure on cb_jc_read_next_completed: vuid=4222  res=-33  res_bdev=-1,-1,-1
```

Each online node has 2 distribs failing with `res=-33 res_bdev=-1,-1,-1`. These are distrib read failures because the underlying journal bdevs (on offline nodes) are gone.

**Key question:** Are these distrib errors causing IO failures to the NVMe-oF clients? The distribs failing are likely the ones that have chunks on the offline nodes — but the volumes with primaries on .113/.115 should still have their local distrib working. Need to understand if `res=-33` causes the entire volume IO path to fail or just the chunks mapped to offline nodes.

### Step 3: Check journal copy count (ha_jm_count)

**Finding: `ha_jm_count=3` — this is the root cause.**

Cluster was created WITHOUT `--ha-jm-count 4`. The default is 3.

Node .113 (d4339392) journal config:
- `ha_jm_count: 3`
- `jm_names`: local + remote on .112 + remote on .115 (only 3 copies, .114 not included)

With FT=2 and 4 nodes, we need `ha_jm_count=4` so every node has a journal copy on every other node (1 local + 3 remote = 4 total). With only 3 copies, each node is missing a journal replica on one other node.

When 2 nodes go down simultaneously, a node may lose 2 of its 3 journal copies (if the missing copy happens to be on the surviving node), leaving it with only the local copy — below quorum.

**This matches the AWS perf cluster issue** (see memory: first deployment failed because ha-jm-count was default 3 instead of 4).

**Fix:** Redeploy cluster with `--ha-jm-count 4` (not available as a CLI flag — need to check how to pass it).

### Step 4: Check for placement errors

Searched `spdk_8081` (.113) and `spdk_8083` (.115) for "placement" — no `*ERROR*` entries found. Only normal NOTICE-level "update placement map" messages from non-leader distribs.

**Note for future investigations:** A known issue exists where DISTRIB cannot place all chunks. Search for "placement" + `*ERROR*` in spdk logs if IO failures occur without journal issues. Not the case here.
