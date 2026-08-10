# Failure-domain and physical labels

Operators name their topology (`RACK1`, `AZ1`, `HOST1`); the control plane keeps
integers. This document is the contract between the two.

## What exists today

| Field | Type | Set by | Meaning |
|---|---|---|---|
| `StorageNode.failure_domain` | `int`, default `-1` | operator, `sn add-node --failure-domain <int>` | rack/cabinet/DC anti-affinity key. Mandatory when the cluster has `enable_failure_domain`; immutable per host (FD migration is refused, `storage_node_ops.py:2468`); emitted verbatim into the distrib cluster map for the data plane |
| `StorageNode.physical_label` | `int`, default `0` | control plane, `get_next_physical_device_order()` (`storage_node_ops.py:801`) | cluster-unique per-host ordinal, keyed on `mgmt_ip`; copied onto each `NVMeDevice.physical_label` |

Both are integers everywhere: CLI (`--failure-domain`, `type: int`), the v2 DTO
(`_dtos.py:355`), the `sn list` column (`storage_node_ops.py:5034`), the expansion
preconditions (`cluster_expansion/preconditions.py:113`), and the distrib map.

## Design

**The integer stays the internal identity.** Nothing in placement, the distrib
map, or the expansion planner changes. Labels are a naming layer on top, resolved
at ingress and rendered at egress.

### Registry

Labels are cluster-scoped, so the map lives on the `Cluster` record:

```python
failure_domain_labels: dict = {}   # "RACK1" -> 0
physical_labels: dict = {}         # "HOST1" -> 1
```

Label → id is the stored direction (ids are unique per kind, so the reverse map is
derived by scan; these hold tens of entries, not thousands).

### Allocation

A new label gets `max(used) + 1`, where *used* is the union of the registry's ids
and the ids already on node records — so an id explicitly chosen by a legacy
integer call can never be handed out again.

Allocation is an **FDB transaction** on the cluster record, following
`DBController._claim_lvol_ns_slot_tx`: read the registry, return the existing id
if the label is known, otherwise allocate and write the registry in the same
transaction. Two concurrent `sn add-node` calls naming the same new label
therefore agree on one id, and two naming different new labels cannot collide.

### Syntax

`^[A-Z][A-Z0-9_-]{0,31}$` after upper-casing the operator's input. `rack1`,
`Rack1` and `RACK1` are the same label; anything else is refused at ingress.

### Compatibility

`--failure-domain` becomes a string. An all-digits value keeps its legacy
meaning — *that* integer id, not a label named `"7"` — so existing scripts, CI
bootstraps and the k8s operator keep working unchanged. Anything else is a label.

### Upgrade

`cluster update` backfills the registry for clusters that predate labels: each
distinct `failure_domain` id in use becomes `FD<id>`, each `physical_label`
becomes `HOST<id>`. Derived names, not guesses at intent — an operator who wants
`RACK7` renames it afterwards. Backfill is idempotent: a label already present
for that id is left alone.

Note `--enable-failure-domain` remains deploy-time only (a cluster still cannot
be upgraded *into* failure domains); the backfill only names domains that already
exist.

### Egress

- `sn list` / `sn get`: show the label where the integer is shown today, falling
  back to the integer when no label is registered.
- v2 API: keep `failure_domain` (int) and add `failure_domain_label` /
  `physical_label_name`, so API consumers are not broken by the rename.

## Open

- `physical_label` is control-plane-assigned today. This design keeps that
  (auto `HOST<n>`) and does **not** add an operator override at `sn add-node`
  until there is a reason to; the label is a rename of what the CP already
  chose.
- No `rename` command yet — the registry supports it (rewrite the label key for
  an existing id), but it needs its own CLI surface and audit event.
