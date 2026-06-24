# AGENTS.md — simplyblock_web

REST API server for the Simplyblock control plane. Runs as a single uvicorn process hosting both API versions.

## Dual API Stack

- **v2** (`api/v2/`) — FastAPI. Routers mounted at `/api/v2`. Uses Pydantic DTOs, FastAPI dependency injection, and bearer-token auth.
- **v1** (`api/v1/`) — Flask. Mounted at `/api/v1` via `WSGIMiddleware`. Uses Flask Blueprints and a `@token_required` decorator.
- Legacy root paths (`/cluster/`, `/lvol/`, etc.) redirect to `/api/v1/` with HTTP 308.

`app.py` is the entry point. It wires both API versions depending on `WebSettings.api_versions`.

## v2 API Patterns

Use these patterns when adding new v2 endpoints:

**DTOs** (`_dtos.py`): Pydantic `BaseModel` subclasses. Each DTO has a `from_model(model, ...)` static method that converts a core model into the DTO. Never expose core models directly in API responses. DTO fields that carry secrets use `SecretStr` with a `@field_serializer('field', when_used='json')` that calls `value.get_secret_value()` — this keeps wrappers in Python-mode `model_dump()` (safe for logging) while unwrapping to plaintext in `model_dump_json()` (for wire responses).

**Dependencies** (`_dependencies.py`): FastAPI `Depends()`-based resource lookup. Typed aliases like `Cluster`, `StorageNode`, `Volume`, `Snapshot` resolve path parameters to core model objects (raising 404 on miss). Dependencies chain — e.g., `Volume` depends on `StoragePool` which depends on `Cluster` — enforcing hierarchical ownership.

**Auth** (`_auth.py`): `verify_api_token` dependency on all routers. Supports k8s service account tokens (via TokenReview) and cluster-secret bearer tokens. Admin service accounts bypass per-cluster checks. Secret comparison uses `hmac.compare_digest(secret.get_secret_value(), token)` for timing safety.

**Access logging** (`app.py`): The `AccessLogMiddleware` logs only `request.url.path`, never the query string — query parameters can carry credentials (`?secret=…`, `?token=…`) and have no type info to mask by.

**Route naming**: Routes use `name="resource:action"` format (e.g., `"clusters:pools:volumes:detail"`) for `request.url_for()` cross-references in DTOs.

## v1 API Patterns

Flask Blueprints registered in `api/v1/__init__.py`. Each resource module (`cluster.py`, `lvol.py`, etc.) defines a Blueprint with route handlers. Auth is applied globally via `@before_request` + `@token_required`.

## Adding an Endpoint

**v2**: Add a route function in the appropriate `api/v2/` module (or create a new router). Define a Pydantic DTO in `_dtos.py` with a `from_model()`. Add any lookup dependencies to `_dependencies.py`. Register the router in `api/v2/__init__.py`.

**v1**: Add a route to the relevant Blueprint in `api/v1/`.

## Tests

```bash
pytest simplyblock_web/test/
```
