# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

Simplyblock Control Plane and CLI (`sbctl`) — a Kubernetes-native distributed block storage solution. Python 3.13+, FoundationDB backend.

## Build & Install

```bash
pip install -e .                    # Editable install
pip install -r requirements.txt     # Install dependencies
```

## Testing

Two tiers via tox: `tox run -e unit` (fast, no infra) and `tox run -e integration` (Docker + `libfdb_c` required). See `tests/AGENTS.md` for tier criteria, the testcontainers FDB fixture, and how to reuse an existing dev-compose FDB instance.

## Linting & Type Checking

```bash
ruff check                          # Lint (or: tox -e lint)
mypy simplyblock_web simplyblock_cli simplyblock_core  # Type check (or: tox -e types)
```

## Architecture

Three packages, one entry point:

| Package | Role |
|---------|------|
| `simplyblock_cli/` | `sbctl` command-line interface (auto-generated entry point) |
| `simplyblock_core/` | Business logic, data models, background services, FDB access |
| `simplyblock_web/` | REST API — FastAPI (v2) + Flask (v1) hybrid on a single uvicorn process |

Data flows: **CLI → Web API → Core controllers → FoundationDB**. Storage nodes are reached via JSON-RPC (`rpc_client.py`).

## Coding Conventions

- **Error handling**: Raise specific exceptions — never return `None`/booleans for errors, never bare `except Exception`. See `CONTRIBUTING.md`.
- **Ruff** and **mypy** are enforced in CI. `simplyblock_cli/cli.py` is excluded from ruff (auto-generated).
- `tests/perf/` is excluded from pytest discovery.

### Secret Handling

Secrets (passwords, tokens, keys, connection strings) are wrapped in Pydantic's `SecretStr` / `SecretBytes` throughout the codebase. The core principle is **wrap early, unwrap late**: secrets enter the system wrapped at the boundary (CLI parse, API ingress, DB read) and are only unwrapped to plaintext at the final wire-send moment. Every layer in between sees only masked values in `repr`/`str`/logging.

Key rules:

- **Model fields**: Declare secret fields as `SecretStr` with a `SecretStr("")` default. `BaseModel.from_dict()` auto-wraps inbound plaintext for backward compatibility with existing FDB records. `to_dict()` keeps wrappers by default (safe for logging); `to_dict(unwrap_secrets=True)` produces plaintext for persistence — only `write_to_db()` should call this.
- **Display / logging JSON**: Never call `json.dumps()` directly on a dict that may carry `SecretStr`/`SecretBytes` — it raises `TypeError`. Use `utils.dump_json(data, ...)` (controllers/CLI) or `utils.print_table(data, ...)` (table mode). Both accept `unwrap_secrets=`: pass `True` at operator-display sites (CLI `X get`, `--json` outputs) where the user is authorized to see plaintext; omit it (default `False`, masks as `**********`) for logging, debug dumps, error paths, and anywhere that might land in a log file. When adding a new secret-bearing field to a model, audit every existing `dump_json` / `print_table` callsite that serializes that model and choose the right `unwrap_secrets` value.
- **Clients (RPC, SNode, Firewall API)**: Accept `SecretStr` parameters. Log the payload dict *before* unwrapping (wrappers mask in log output), then call `unwrap_secrets_for_send(payload)` from `simplyblock_core/utils/secrets.py` right before `requests.post(json=...)`.
- **v2 DTOs**: Use `@field_serializer('field', when_used='json')` to unwrap for JSON wire responses while keeping wrappers in Python-mode `model_dump()`.
- **CLI arguments**: Declare the argument type as `secret` in `cli-reference.yaml`. The generator produces `SecretStr` as the argparse type converter, so the value is wrapped at parse time.
- **Logging**: Never log unwrapped secret values. Response-body logging is gated by `Settings().log_response_bodies` (env `SB_LOG_RESPONSE_BODIES`, default `False`). External libraries that log HTTP bodies (`urllib3`, `kubernetes.client.rest`) are silenced to WARNING. The web access log records only `request.url.path`, never the query string.
- **Comparison**: Use `hmac.compare_digest(secret.get_secret_value(), other)` for timing-safe comparison.
- **Testing**: New secret-bearing code needs masking, wire-delivery, and FDB round-trip tests. See `tests/AGENTS.md` § Secret-handling tests for the required assertions and canonical examples.

## Verification

After any code change, run the `tox-verify` skill (`.agents/skills/tox-verify.md`). The short version:

1. `tox run-parallel -e lint,types` — fix lint/type errors first.
2. `tox run -e unit -- tests/unit/path/to/relevant_test.py` — run targeted tests while iterating.
3. `tox run` — full suite before finishing. Never mark work done without a green run.

## Local Development

```bash
sudo docker compose -f docker-compose-dev.yml up --build -d
```

Requires FoundationDB 7.3.3 client library installed on the host for the Python bindings.

## Agent Instructions Layout

`AGENTS.md` is the source of truth at every level. Tool-specific files are symlinks:

```
AGENTS.md                          ← root instructions (this file)
CLAUDE.md → AGENTS.md              ← Claude Code
.github/copilot-instructions.md → ../AGENTS.md  ← GitHub Copilot

simplyblock_cli/AGENTS.md         ← CLI-specific instructions
simplyblock_cli/CLAUDE.md → AGENTS.md
simplyblock_core/AGENTS.md        ← Core-specific instructions
simplyblock_core/CLAUDE.md → AGENTS.md
simplyblock_web/AGENTS.md         ← Web API-specific instructions
simplyblock_web/CLAUDE.md → AGENTS.md
tests/AGENTS.md                   ← Test-suite layout, tiers, fixtures
tests/CLAUDE.md → AGENTS.md

.agents/skills/                    ← shared skills (source of truth)
  tox-verify.md                    ← tox verification workflow
.claude/skills → ../.agents/skills ← Claude Code skill symlink
```

Edit only `AGENTS.md` files and `.agents/skills/` contents. Never edit the symlink targets directly.

### Local overrides

At every level where an `AGENTS.md` exists, also check for a sibling `AGENTS.local.md`. If present, load it in addition to `AGENTS.md` — its contents extend or override the checked-in instructions. `AGENTS.local.md` is gitignored and intended for per-developer notes that should not be committed.
