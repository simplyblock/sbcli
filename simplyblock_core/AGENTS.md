# AGENTS.md — simplyblock_core

Core business logic, data models, and background services for the Simplyblock control plane.

## Package Structure

- `controllers/` — Business logic per resource domain (lvol, snapshot, backup, device, migration, pool, health, tasks, qos). Each `*_events.py` defines event types for its domain.
- `models/` — Data models inheriting from `BaseModel` (see below).
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
