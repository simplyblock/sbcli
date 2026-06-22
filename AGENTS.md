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

```bash
pytest                              # All unit/integration tests
pytest simplyblock_core/test/       # Core tests only
pytest simplyblock_web/test/        # Web API tests only
pytest tests/                       # Integration tests only
pytest path/to/test.py::TestClass::test_method  # Single test
tox -e tests                        # Via tox
```

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
- **Ruff** and **mypy** are enforced in CI. `simplyblock_cli/cli.py` is excluded from ruff (auto-generated). `simplyblock_web/test` is excluded from mypy.
- `tests/perf/` is excluded from pytest discovery.

## Verification

After any code change, run the `tox-verify` skill (`.agents/skills/tox-verify.md`). The short version:

1. `tox run-parallel -e lint,types` — fix lint/type errors first.
2. `tox run -e tests -- tests/path/to/relevant_test.py` — run targeted tests while iterating.
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

.agents/skills/                    ← shared skills (source of truth)
  tox-verify.md                    ← tox verification workflow
.claude/skills → ../.agents/skills ← Claude Code skill symlink
```

Edit only `AGENTS.md` files and `.agents/skills/` contents. Never edit the symlink targets directly.
