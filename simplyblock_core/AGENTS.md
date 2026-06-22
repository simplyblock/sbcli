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

## Tests

```bash
pytest simplyblock_core/test/
```
