# AGENTS.md — simplyblock_core

Core business logic, data models, and background services for the Simplyblock control plane.

## Package Structure

- `controllers/` — Business logic per resource domain (lvol, snapshot, backup, device, migration, pool, health, tasks, qos). Each `*_events.py` defines event types for its domain.
- `models/` — Data models inheriting from `BaseModel` (see below).
- `services/` — Background services for monitoring and async task execution (health checks, snapshot/lvol/storage-node monitors, task runners for backup, migration, restart, etc.).
- `db_controller.py` — Singleton `DBController` wrapping FoundationDB. All data access goes through this class.
- `rpc_client.py` — JSON-RPC client with caching for communicating with storage node SPDK processes.
- `kms/` — Key management abstraction: HashiCorp Vault (`_hcp.py`) and FDB-based (`_fdb.py`) backends.

## Data Model Pattern

All models extend `BaseModel` (`models/base_model.py`). Key conventions:

- Models define fields as **class-level type annotations with defaults**. `BaseModel.from_dict()` / `to_dict()` handle serialization automatically via introspection of annotations.
- Identity: `uuid` field; `get_id()` returns it. `get_db_id()` returns the FDB key as `<object_type>/<class_name>/<uuid>`.
- Persistence: `write_to_db(kv_store)` and `read_from_db(kv_store)` serialize to/from JSON in FDB.
- `BaseNodeObject` extends `BaseModel` with standard node status constants (`STATUS_ONLINE`, `STATUS_OFFLINE`, etc.) and a status code map.
- **Secret fields** use `SecretStr` (from `pydantic`) as the type annotation with `SecretStr("")` default. `from_dict()` auto-wraps plain strings from FDB into `SecretStr`. `to_dict()` keeps wrappers (safe for logging); only `write_to_db()` calls `to_dict(unwrap_secrets=True)` to persist plaintext. When adding a new secret field, follow existing examples in `cluster.py`, `storage_node.py`, or `pool.py`.

## Client Pattern (RPC, SNode, Firewall API)

Clients in `rpc_client.py`, `snode_client.py`, and `fw_api_client.py` accept `SecretStr` parameters and follow the **log-then-unwrap** pattern:

1. Log the payload dict containing `SecretStr` wrappers (masked by Pydantic's `__repr__`).
2. Call `unwrap_secrets_for_send(payload)` from `utils/secrets.py` to produce a plaintext dict.
3. Send the plaintext dict as JSON on the wire.

Response-body logging is gated by `Settings().log_response_bodies` (default `False`). When off, only status code and content-length are logged.

## Tests

```bash
pytest simplyblock_core/test/
```
