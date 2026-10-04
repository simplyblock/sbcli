# AGENTS.md — simplyblock_core

Core business logic, data models, and background services for the Simplyblock control plane.

## Package Structure

- `controllers/` — Business logic per resource domain (lvol, snapshot, backup, device, migration, pool, health, tasks, qos). Each `*_events.py` defines event types for its domain.
- `models/` — Data models inheriting from `BaseModel` (see below), plus the two stdlib-only
  modules that define the keyspaces those records live in: `watches.py` (the version index
  watchers wake on) and `indices.py` (the declared secondary indices). Both are leaves — the
  models import them, nothing in them imports back — which is what lets a model declare its
  keys without dragging in `fdb`.
- `services/` — Background services for monitoring and async task execution (health checks, snapshot/lvol/storage-node monitors, task runners for backup, migration, restart, etc.).
- `db_controller.py` — Singleton `DBController` wrapping FoundationDB. All data access goes through this class.
- `rpc_client.py` — JSON-RPC client for communicating with storage node SPDK processes. `Session` construction is pooled by `RPCSessionPool` (keyed on identity + retry; `timeout` stays per-call). `services/spdk_http_proxy_server.py`, the receiving end, supports HTTP/1.1 keep-alive so those pooled connections are actually reused end-to-end. It is a FastAPI app on uvicorn: `create_app()` builds it, importing the module has no side effects, and it exposes a Prometheus endpoint on `/_meta/metrics` (same path as `simplyblock_web`, behind the same basic-auth credentials as the RPCs) alongside a periodic timing summary in its log. Per-request logging follows `simplyblock_web/app.py`: uvicorn's access log is off and an `AccessLogMiddleware` replaces it, enriched with the JSON-RPC method and the id that ties the access line to the request's own `Request:<id>` line.
- `kms/` — Key management abstraction: HashiCorp Vault (`_hcp.py`) and FDB-based (`_fdb.py`) backends.

## Data Model Pattern

All models extend `BaseModel` (`models/base_model.py`). Key conventions:

- `BaseModel` is hand-rolled, **not** a Pydantic model. Models define fields as **class-level type annotations with defaults**; `BaseModel.from_dict()` / `to_dict()` handle serialization automatically via introspection of annotations.
- **Mutable defaults** (`list`, `dict`, `set`) must be declared with `default_factory` from `models/base_model.py`: `nodes: List[str] = default_factory(list)`. A literal `= []` stores one object on the class that every instance without a value for the field would share and mutate — `ruff`'s RUF012 rejects it, and `tests/unit/models/test_mutable_defaults.py` fails on any model field that reintroduces one. A genuine class constant shared on purpose (`_STATUS_CODE_MAP`) is annotated `ClassVar[...]` instead.
- Identity: `uuid` field; `get_id()` returns it. `get_db_id()` returns the FDB key as `<object_type>/<class_name>/<uuid>`.
- Persistence: `write_to_db(kv_store)` and `read_from_db(kv_store)` serialize to/from JSON in FDB.
- `BaseNodeObject` extends `BaseModel` with standard node status constants (`STATUS_ONLINE`, `STATUS_OFFLINE`, etc.) and a status code map.
- **Secret fields** use `SecretStr` (from `pydantic`) as the type annotation with `SecretStr("")` default. `from_dict()` auto-wraps plain strings from FDB into `SecretStr`. `to_dict()` keeps wrappers (safe for logging); only `write_to_db()` calls `to_dict(unwrap_secrets=True)` to persist plaintext. When adding a new secret field, follow existing examples in `cluster.py`, `storage_node.py`, or `pool.py`.

## Secondary Indices

`db_controller.py` used to answer every non-primary-key lookup by scanning a table
and filtering it in memory. It now answers them from declared secondary indices
(`models/indices.py`, a stdlib-only leaf module, like the `models/watches.py` beside it).

**Declaring one.** `_INDEXES` is a plain class attribute on the model, next to the
fields it indexes — like `_WATCHED`, it stays out of `get_attrs_map()` and is never
serialized:

```python
class LVol(BaseModel):
    _INDEXES: ClassVar[tuple] = (
        Index('pool_uuid'),                                 # a field
        Index(('target_type', 'target_id')),                # a tuple of fields
        Index('device_id', arity=2, extract=lambda node: [...]),  # many entries per record
        Unique(('pool_uuid', 'lvol_name')),                 # also a constraint
    )
```

A callable extractor declares `arity`, the width of every tuple it returns; the field
forms derive it from the fields. It is what lets a reader skip the value segments of a
stored key and land on the entity id.

Keys live in their own namespace, disjoint from the `object/` scans:

```
index/<Class>/<index-name>/<value...>/<entity-id>   -> b''
index/<Class>/<index-name>/<value...>               -> entity id   (Unique)
index_meta/<Class>/<index-name>                     -> state record
```

A non-unique entry stores **nothing**: its key already ends in the id, and a second copy
in the value could only ever disagree with it — which is a drift class the design simply
does not have. `Index.entry_id(cls, key, value)` is the one way to read an entry's owner
(off the key for `Index`, out of the value for `Unique`, whose key omits it), and
`Index.entry_value(id)` the one way to write one. Never decode an entry by hand.

**Reading.** `DBController.query(model_cls, index, *values, limit=, reverse=)` is the
only read primitive; `query_one` is its `single_or_none` form and `query_ids` skips the
entity reads. Every `get_*_by_*` helper is a one-liner over it. `limit`/`reverse` order
by the index key, so they only mean something on an `ordered=True` index.

**Maintenance is transactional.** `write_to_db` / `remove` / `atomic_update` update the
index entries in the SAME FDB transaction as the entity, so an index can never describe a
record that was never written. Any new code path that writes a record key directly
(`tr[key] = ...`) has to call `BaseModel._apply_index_diff` too — `_try_set_node_restarting_tx`
is the one such site, and shows the shape.

**A write becomes a read-write transaction**, which is what makes the diff correct — the
"before" keys come from the record actually stored, re-read on each conflict retry — and what
makes `Unique` a constraint: the index key is in the read conflict set, so two concurrent
creates of one value cannot both commit. It does not make a full-object write safe against a
stale copy: `value` is serialized before the transaction, so a retry rewrites the same bytes.
`atomic_update` is still the only compare-and-set, and still the answer to `[NODE-WRITE]`.

**`Unique` is a backstop, never a user-facing error path.** The clean "name already exists"
answer still comes from the pre-check (`lvol_name_taken`, `snap_name_taken`) — now a point
read on a transactionally-maintained key rather than a best-effort one. A
`UniqueIndexViolation` means the pre-check did not run or the data is already inconsistent:
it propagates to a 500, and `sbctl cluster check-indices` is the diagnostic. Do not catch it
at a create site.

**Rollout.** Each index carries a state at `index_meta/<Class>/<name>`, read through a
short-TTL cache:

- `building` (the default) — writes maintain it, reads fall back to a filtered scan;
- `ready` — reads use it;
- `disabled` — writes skip it, reads fall back. The kill switch, thrown with
  `sbctl cluster index-state <Class>.<index> --set disabled` and released with
  `--set building`, which discards the index's entries on the way back in so the backfill
  that follows leaves nothing stale behind. Both switches block for
  `ttl_cache.INDEX_STATE_CONVERGENCE_SEC` while the new state reaches the other processes:
  the state is TTL-cached per process, so until it has converged a reader can still be
  trusting an index nothing maintains — or, on the way back in, reading a keyspace that was
  emptied out from under it. `sbctl cluster index-state` with no index lists
  where every index stands.

The fallback's predicate and ordering come from the same declaration
(`Index.match_paths`), so index and scan cannot drift into different answers. Shipping a new
index is therefore: declare it → `sbctl cluster build-indices` (or an upgrade, which runs
`release_upgrades/database_indices.py`) → it flips to `ready` on its own. A `Unique` index
over data that already holds duplicates is the one that does not: the backfill leaves it
`building`, names the colliding records and exits non-zero, because the key holds one id and
flipping it would hide every other record carrying that value.
`sbctl cluster check-indices [--repair]` walks both directions and is safe against a live
cluster. `--repair` writes back every entry it can *derive* — and refuses the one thing it
cannot, a unique value two live records both carry, which is a defect in the data. It
fails (non-zero) while anything is left unresolved, so a repair run that exits 0 means the
indices are clean, not merely that something was repaired. `index_ops.py` holds the backfill and the verifier. A read that falls back to a
scan logs an info line (rate-limited per index) — one that survives a `ready` flip is a bug.

**The backfill walks keys, not records.** It reads each record inside the transaction that
writes what that record derives (`_index_record`, over the same `_apply_index_diff` live
writes use), so one record is one transaction and no derived value ever outlives the version
it came from. A walk that handed out materialized records instead would let a record be
indexed under a value it no longer carries, permanently — nothing clears an entry the record
does not derive. Anything new that derives keys from a record must read that record in the
transaction it writes to; a conflict there means re-derive, never retry the derivation you
already have.

**When an index is worth it.** It turns a full scan into a range read plus one pipelined
point read per hit: a win when the result is a small fraction of the table, a mild loss when
it is most of it. Add one when the predicate is selective *and* the call site is hot or in a
loop. A lookup that returns exactly one row is always worth it.

**`LVolMini` / `SnapShotMini` are deliberately not indexed.** They are a workaround for the
missing query layer and are scheduled for deletion; indexing them would entrench them.

## Pydantic Models

Genuine Pydantic models in this package — `settings.py` (`pydantic-settings`) and any new validated payload or config object — follow the annotated pattern: constraints and metadata inside `Annotated[...]`, the default on the right-hand side of the assignment. `settings.py` is the reference example. Reusable constrained types live next to their domain (`utils/pci.py` defines `PCIAddress`). See root `AGENTS.md` § Pydantic Fields. This does not apply to `BaseModel` subclasses in `models/`, which are not Pydantic.

## Display & Logging JSON

`json.dumps()` raises `TypeError` on a dict containing `SecretStr`/`SecretBytes`. Use the helpers in `utils/__init__.py` for every controller / CLI / logging path that serializes a model dict (`get_clean_dict()`, `to_dict()`):

- `utils.dump_json(data, ..., unwrap_secrets=...)` — JSON output.
- `utils.print_table(rows, ..., unwrap_secrets=...)` — pretty-table output.

Choose the flag by destination:

- **Operator display** (`sbctl X get`, `--json` CLI output): pass `unwrap_secrets=True`. The user is authorized to see plaintext; without the flag they see `**********` and can't recover the value.
- **Logging, debug dumps, error paths, event payloads**: omit the flag (defaults to `False`). Secrets render as `**********`. Use this for anything that may end up in a log file, stderr, or a captured exception.

When adding a new `SecretStr` field to an existing model, grep for `json.dumps`, `dump_json(`, and `print_table(` callsites that touch that model and audit each one. The display-vs-log distinction is per-callsite, not per-model.

## Client Pattern (RPC, SNode, Firewall API)

Clients in `rpc_client.py`, `snode_client.py`, and `fw_api_client.py` accept `SecretStr` parameters and follow the **log-then-unwrap** pattern:

1. Log the payload dict containing `SecretStr` wrappers (masked by Pydantic's `__repr__`).
2. Call `unwrap_secrets_for_send(payload)` from `utils/secrets.py` to produce a plaintext dict.
3. Send the plaintext dict as JSON on the wire.

Response-body logging is gated by `Settings().log_response_bodies` (default `False`). When off, only status code and content-length are logged.

The request-side `logger.debug` in `_request2` / `_request3` masks by type for the params that are `SecretStr`, and passes every params dict through `redact_rpc_params` (`utils/secrets.py`) to cover the ones that arrive as plain `str` — the v1 API hands controllers raw JSON. The SPDK proxy applies the same redactor, since by the time a body reaches it the wrappers are gone.

## Tests

```bash
pytest simplyblock_core/test/    # controller / service logic
pytest tests/unit/models/       # every BaseModel test lives here
```
