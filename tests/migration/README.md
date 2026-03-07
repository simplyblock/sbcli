# Migration Integration Test Suite

End-to-end integration tests for the live volume migration feature.
Tests drive the real migration controller and task runner against in-process
mock JSON-RPC servers, with a real FoundationDB instance for control-plane state.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Python 3.10+ | Same environment used to run `sbcli` |
| FoundationDB client | `fdb` Python package + `fdb.cluster` file at `/etc/foundationdb/fdb.cluster` |
| **Existing cluster in FDB** | Run normal control-plane setup first (`sbcli cluster create …`). Tests attach to the pre-existing cluster — they do **not** create one. |
| `pytest` | `pip install pytest` |
| `requests` | Already a project dependency |

No SPDK or real storage nodes are needed — the mock RPC servers replace them entirely.

---

## Running the tests

From the repository root:

```bash
# Run all migration tests
pytest tests/migration/ -v

# Run a specific test class
pytest tests/migration/test_migration_flow.py::TestBasicMigration -v

# Run a single test
pytest tests/migration/test_migration_flow.py::TestHASecondaryRegistration::test_lvol_registered_and_exposed_on_secondary -v

# Run with verbose logging (useful when a test hangs)
pytest tests/migration/ -v -s --log-cli-level=DEBUG

# Run in parallel (requires pytest-xdist)
pip install pytest-xdist
pytest tests/migration/ -v -n 3
```

---

## Test architecture

```
tests/migration/
├── README.md                    ← this file
├── conftest.py                  ← pytest fixtures (servers, topologies, task runner)
├── mock_rpc_server.py           ← in-process JSON-RPC 2.0 mock for one SPDK node
├── topology_loader.py           ← JSON spec → FDB objects + TestContext
├── db_setup.py                  ← lower-level FDB helpers (used by some utilities)
├── test_migration_flow.py       ← integration tests
├── test_ctl.py                  ← CLI inspector for interactive debugging
└── topologies/
    ├── two_node.json            ← src + tgt, single volume + one snapshot
    ├── two_node_ha.json         ← src + tgt primary + tgt secondary (HA pair)
    └── clone_chain.json         ← two volumes sharing a snapshot ancestry chain
```

### How a test works

1. **Topology fixture** loads a JSON spec, discovers the pre-existing cluster in
   FDB, then writes `StorageNode`, `Pool`, `LVol`, and `SnapShot` records and
   returns a `TestContext`.
2. **Mock server fixtures** (session-scoped) provide HTTP servers on loopback that
   speak JSON-RPC 2.0 and keep all bdev / subsystem state in memory.
3. The test **seeds** the source mock server with the bdev state that matches the
   FDB records (so the migration controller sees a consistent view).
4. `migration_controller.start_migration()` is called — this writes an
   `LVolMigration` record to FDB and enqueues a `JobSchedule` task.
5. `run_migration_task()` drives the task runner synchronously in a loop until
   the migration reaches a terminal state or the step budget is exhausted.
6. Assertions inspect the final FDB state and/or mock server in-memory state.
7. Topology teardown removes all FDB keys written for that test (nodes, pools,
   lvols, snapshots). The cluster is **not** removed.

---

## Topology JSON format

A topology spec is a JSON object with five top-level keys:
`cluster`, `nodes`, `pools`, `volumes`, `snapshots`.

Every `id` field is a **symbolic name** — it only exists within the spec file.
Cross-references (e.g. `volume.node_id`) use symbolic names; the loader resolves
them to actual UUIDs before writing to FDB.

### `cluster`

The cluster must already exist in FDB before running tests.  The `cluster`
object in the spec only selects which cluster to attach objects to.

```json
{
  "cluster": {
    "id": "existing-cluster-uuid"   // optional; auto-discovers first cluster if omitted
  }
}
```

Omit `id` (or leave the object empty: `"cluster": {}`) to let the loader
pick up whichever cluster is already present in FDB.  This is the recommended
form for the bundled topology files, since the cluster UUID is not known in
advance.

### `nodes`

```json
{
  "nodes": [
    {
      "id":               "src",         // symbolic name (required)
      "mgmt_ip":          "127.0.0.1",
      "rpc_port":         9901,          // must match a running mock server port
      "rpc_username":     "spdkuser",
      "rpc_password":     "spdkpass",
      "lvstore":          "lvs_src",     // lvol-store name on this node
      "lvol_subsys_port": 9090,
      "max_lvol":         256,
      "max_snap":         5000,
      "hostname":         "host-src",
      "ha_type":          "single",      // "single" | "ha"
      "fabric":           "tcp",
      "status":           "online",      // "online" | "offline" | "down" | ...
      "is_secondary":     false,         // true for HA secondary nodes
      "secondary_node_id": "",           // symbolic id of the secondary (primary only)
      "hub_port":         4420,
      "data_nics": [
        {
          "if_name": "eth0",
          "ip":      "127.0.0.1",
          "trtype":  "TCP"               // "TCP" | "RDMA"
        }
      ]
    }
  ]
}
```

**HA / secondary replica**: every physical node runs a single SPDK process that
hosts both its own primary lvstore and a secondary replica of another node's
primary lvstore.  `secondary_node_id` on node X points to the node Y whose
secondary replica **lives on X** — i.e. after migrating a volume to X, the
migration runner also registers the objects on Y because Y is responsible for
the secondary copy.

No node has a special "secondary" flag — all nodes are full peers with their own
primary lvstore.

```json
{
  "nodes": [
    {"id": "tgt",     "lvstore": "lvs_tgt",     "secondary_node_id": "tgt-sec", ...},
    {"id": "tgt-sec", "lvstore": "lvs_tgt_sec", "secondary_node_id": "",        ...}
  ]
}
```

In this example `tgt.secondary_node_id = "tgt-sec"` means: the secondary replica
of `lvs_tgt` is hosted on node `tgt-sec`.  Node `tgt-sec` is itself a full node
with its own primary lvstore `lvs_tgt_sec`.

### `pools`

```json
{
  "pools": [
    {
      "id":       "pool1",           // symbolic name (required)
      "name":     "default-pool",
      "status":   "active",
      "pool_max": "10T",             // optional; max pool capacity (same format as CLI)
      "lvol_max": "1T"               // optional; max per-lvol size
    }
  ]
}
```

### `volumes`

```json
{
  "volumes": [
    {
      "id":                    "l1",         // symbolic name (required)
      "name":                  "vol1",       // lvol name shown in CLI
      "size":                  "1G",         // "1G", "512M", "1073741824" (raw bytes)
      "max_size":              "1000T",      // optional; default 1000T
      "node_id":               "src",        // symbolic node id (required)
      "pool_id":               "pool1",      // symbolic pool id
      "ha_type":               "single",
      "fabric":                "tcp",
      "status":                "online",
      "max_namespace_per_subsys": 1,
      "ns_id":                 1,            // NVMe namespace id within subsystem
      "namespace_group":       "grp1",       // optional; volumes with the same group share an NQN
      "cloned_from_snap":      "s2",         // optional; symbolic snapshot id (clone scenario)
      "max_rw_iops":           0,
      "max_rw_mbytes":         0,
      "max_r_mbytes":          0,
      "max_w_mbytes":          0
    }
  ]
}
```

**Namespace sharing**: when multiple volumes should share the same NVMe-oF subsystem
(as happens when `max_namespace_per_subsys > 1`), give them the same `namespace_group`
string. The loader assigns them a common NQN automatically.

**Clone volumes**: set `cloned_from_snap` to the symbolic id of the parent snapshot.
The loader resolves this to the actual snapshot UUID after the snapshot loop completes.

### `snapshots`

```json
{
  "snapshots": [
    {
      "id":          "s1",     // symbolic name (required)
      "name":        "snap1",  // snapshot name
      "lvol_id":     "l1",     // symbolic volume id this snapshot belongs to (required)
      "snap_ref_id": "",       // symbolic id of parent snapshot (empty = root)
      "status":      "online",
      "created_at":  0         // optional unix timestamp; defaults to now
    }
  ]
}
```

**Snapshot chains**: use `snap_ref_id` to link child → parent.
The oldest (root) snapshot has an empty `snap_ref_id`.

```json
"snapshots": [
  {"id": "s1", "name": "snap1", "lvol_id": "l1", "snap_ref_id": ""},
  {"id": "s2", "name": "snap2", "lvol_id": "l1", "snap_ref_id": "s1"},
  {"id": "s3", "name": "snap3", "lvol_id": "l1", "snap_ref_id": "s2"}
]
```

---

## Size strings

Volume and pool sizes follow the same format as the `sbcli` CLI:

| String | Meaning |
|--------|---------|
| `"1G"` | 1 GiB |
| `"512M"` | 512 MiB |
| `"1073741824"` | 1 073 741 824 bytes (raw) |
| `"2T"` | 2 TiB |

---

## Mock server ports

The session-scoped mock servers use fixed ports:

| Server | Default port | Worker offset (xdist) |
|--------|--------------|-----------------------|
| source (`mock_src_server`) | 9901 | `+ worker_id * 10` |
| target (`mock_tgt_server`) | 9902 | `+ worker_id * 10` |
| secondary (`mock_sec_server`) | 9903 | `+ worker_id * 10` |

Topology fixtures patch the `rpc_port` of each node spec to match the
session-scoped server so that the migration controller connects to the right mock.

---

## Failure injection

Tests can inject random failures into any mock server:

```python
# 15 % of RPC calls fail (error or timeout)
mock_src_server.set_failure_rate(0.15, timeout_seconds=0.1)

# Re-enable after the test
mock_src_server.set_failure_rate(0.0)
```

`set_failure_rate` is also available via RPC (useful from `test_ctl.py`):

```bash
python -m tests.migration.test_ctl mock set-failure-rate \
    --host 127.0.0.1 --port 9901 --rate 0.2 --timeout 0.1
```

---

## Interactive debugging with `test_ctl.py`

Run in a second terminal while a test is executing (or after a failure):

```bash
# List all migrations in a test cluster
python -m tests.migration.test_ctl migration list --cluster-id <id>

# Show full migration record
python -m tests.migration.test_ctl migration show <migration-id>

# Cancel a running migration
python -m tests.migration.test_ctl migration cancel <migration-id>

# Force a node offline
python -m tests.migration.test_ctl node set-status <node-id> offline

# Set cluster status
python -m tests.migration.test_ctl cluster set-status <cluster-id> degraded

# List volumes on a node
python -m tests.migration.test_ctl lvol list --node-id <node-id>

# Dump bdevs and subsystems from a mock server
python -m tests.migration.test_ctl mock state --host 127.0.0.1 --port 9901
```

---

## Creating and deleting objects during a test

The topology JSON defines objects that exist at test start.  For concurrency
tests — where you need to create or delete volumes and snapshots *while* a
migration is already running — `TestContext` exposes mutation helpers that
write directly to FDB and register the object for teardown automatically.

### `ctx.add_lvol(sym_id, node_sym, …)`

```python
lvol = ctx.add_lvol(
    "l_new",           # symbolic id for later lookup
    "src",             # symbolic node id (already in ctx)
    size="512M",       # default "1G"
    pool_sym="pool1",  # optional; symbolic pool id
    name="vol_new",    # optional; defaults to sym_id
)
# seed it into the mock server if needed:
_seed_lvol(mock_src_server, lvol, ctx.node("src"))
```

### `ctx.add_snapshot(sym_id, lvol_sym, …)`

```python
snap = ctx.add_snapshot(
    "s_new",           # symbolic id
    "l1",              # symbolic lvol id (the parent)
    snap_ref_sym="s1", # optional; parent snapshot for chaining
    name="snap_new",   # optional; defaults to sym_id
)
_seed_snapshot(mock_src_server, snap, ctx.node("src"))
```

### `ctx.remove_lvol(sym_id)` / `ctx.remove_snapshot(sym_id)`

```python
ctx.remove_snapshot("s_new")   # removes from FDB; safe if already gone
ctx.remove_lvol("l_new")
```

Both are idempotent and safe to call even if the migration runner already
deleted the object during cleanup.

### Concurrency test pattern

```python
import threading
from tests.migration.conftest import run_migration_task

def test_snapshot_during_migration(topology_two_node, mock_src_server, mock_tgt_server):
    ctx = topology_two_node
    _seed_all(mock_src_server, ctx, "src")

    mig_id, err = migration_controller.start_migration(
        ctx.lvol_uuid("l1"), ctx.node_uuid("tgt"))
    assert err is None

    results = {}

    def _run_migration():
        results["migration"] = run_migration_task(mig_id, max_steps=500)

    def _concurrent_ops():
        # Try creating a snapshot while migration holds the snapshot freeze
        snap = ctx.add_snapshot("s_concurrent", "l1")
        _seed_snapshot(mock_src_server, snap, ctx.node("src"))
        # … assert expected rejection or success depending on policy …
        ctx.remove_snapshot("s_concurrent")

    t_mig = threading.Thread(target=_run_migration)
    t_ops = threading.Thread(target=_concurrent_ops)
    t_mig.start()
    t_ops.start()
    t_mig.join(timeout=30)
    t_ops.join(timeout=5)

    _assert_migration_done(mig_id)
```

---

## Writing a new test

```python
def test_my_scenario(custom_topology, mock_src_server, mock_tgt_server):
    spec = {
        "cluster": {},          # auto-discovers the pre-existing cluster from FDB
        "nodes": [
            {"id": "src", "mgmt_ip": "127.0.0.1", "rpc_port": 9901,
             "lvstore": "lvs_src", "status": "online",
             "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
            {"id": "tgt", "mgmt_ip": "127.0.0.1", "rpc_port": 9902,
             "lvstore": "lvs_tgt", "status": "online",
             "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
        ],
        "pools": [{"id": "p1", "name": "pool"}],
        "volumes": [
            {"id": "l1", "name": "vol1", "size": "1G", "node_id": "src", "pool_id": "p1"},
        ],
        "snapshots": [
            {"id": "s1", "name": "snap1", "lvol_id": "l1"},
        ],
    }
    ctx = custom_topology(spec)

    # Seed source mock with bdev state matching FDB
    _seed_all(mock_src_server, ctx, "src")

    mig_id, err = migration_controller.start_migration(
        ctx.lvol_uuid("l1"), ctx.node_uuid("tgt"))
    assert err is None

    run_migration_task(mig_id, max_steps=500, step_sleep=0.02)

    m = db.get_migration_by_id(mig_id)
    assert m.status == LVolMigration.STATUS_DONE
```

Use the predefined topology fixtures (`topology_two_node`, `topology_two_node_ha`,
`topology_clone_chain`) for tests that match an existing topology — they are faster
because the JSON is loaded once and patched at fixture time rather than parsed inline.

---

## Topology JSON full examples

### Minimal two-node, single snapshot

```json
{
  "cluster": {},
  "nodes": [
    {"id": "src", "mgmt_ip": "127.0.0.1", "rpc_port": 9901,
     "lvstore": "lvs_src", "status": "online",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
    {"id": "tgt", "mgmt_ip": "127.0.0.1", "rpc_port": 9902,
     "lvstore": "lvs_tgt", "status": "online",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]}
  ],
  "pools": [{"id": "p1", "name": "pool"}],
  "volumes": [
    {"id": "l1", "name": "vol1", "size": "1G", "node_id": "src", "pool_id": "p1"}
  ],
  "snapshots": [
    {"id": "s1", "name": "snap1", "lvol_id": "l1"}
  ]
}
```

### HA — target node with secondary replica on a peer

Each node runs one SPDK process and hosts both a primary lvstore and a secondary
replica of another node's lvstore.  `secondary_node_id` on `tgt` points to
`tgt-sec`, meaning the secondary replica of `lvs_tgt` lives on `tgt-sec`.
`tgt-sec` is a full peer node with its own primary lvstore (`lvs_tgt_sec`).

```json
{
  "cluster": {},
  "nodes": [
    {"id": "src",     "rpc_port": 9901, "lvstore": "lvs_src",
     "mgmt_ip": "127.0.0.1",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
    {"id": "tgt",     "rpc_port": 9902, "lvstore": "lvs_tgt",
     "mgmt_ip": "127.0.0.1", "secondary_node_id": "tgt-sec",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
    {"id": "tgt-sec", "rpc_port": 9903, "lvstore": "lvs_tgt_sec",
     "mgmt_ip": "127.0.0.1",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]}
  ],
  "pools": [{"id": "p1", "name": "pool"}],
  "volumes": [
    {"id": "l1", "name": "vol1", "size": "1G", "node_id": "src", "pool_id": "p1"}
  ],
  "snapshots": [
    {"id": "s1", "name": "snap1", "lvol_id": "l1"}
  ]
}
```

### Clone chain (l1 → s3 → s2 → s1, clone c1 from s2)

```json
{
  "cluster": {},
  "nodes": [
    {"id": "src", "rpc_port": 9901, "lvstore": "lvs_src",
     "mgmt_ip": "127.0.0.1",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
    {"id": "tgt", "rpc_port": 9902, "lvstore": "lvs_tgt",
     "mgmt_ip": "127.0.0.1",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]}
  ],
  "pools": [{"id": "p1", "name": "pool"}],
  "volumes": [
    {"id": "l1", "name": "vol1",   "size": "2G", "node_id": "src", "pool_id": "p1"},
    {"id": "c1", "name": "clone1", "size": "2G", "node_id": "src", "pool_id": "p1",
     "cloned_from_snap": "s2"}
  ],
  "snapshots": [
    {"id": "s1", "name": "snap1", "lvol_id": "l1", "snap_ref_id": ""},
    {"id": "s2", "name": "snap2", "lvol_id": "l1", "snap_ref_id": "s1"},
    {"id": "s3", "name": "snap3", "lvol_id": "l1", "snap_ref_id": "s2"}
  ]
}
```

### Shared NQN subsystem (two volumes, same namespace group)

```json
{
  "cluster": {},
  "nodes": [
    {"id": "src", "rpc_port": 9901, "lvstore": "lvs_src",
     "mgmt_ip": "127.0.0.1",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
    {"id": "tgt", "rpc_port": 9902, "lvstore": "lvs_tgt",
     "mgmt_ip": "127.0.0.1",
     "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]}
  ],
  "pools": [{"id": "p1", "name": "pool"}],
  "volumes": [
    {"id": "l1", "name": "vol1", "size": "1G", "node_id": "src", "pool_id": "p1",
     "namespace_group": "grp1", "ns_id": 1},
    {"id": "l2", "name": "vol2", "size": "1G", "node_id": "src", "pool_id": "p1",
     "namespace_group": "grp1", "ns_id": 2}
  ],
  "snapshots": [
    {"id": "s1", "name": "snap1", "lvol_id": "l1"},
    {"id": "s2", "name": "snap2", "lvol_id": "l2"}
  ]
}
```
