# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

Simplyblock Control Plane and CLI (`sbctl`) — a Kubernetes-native distributed block storage solution. FoundationDB backend.

Packaging is PEP 621 (`pyproject.toml`, setuptools backend) and dependencies are locked in
`uv.lock`. `requires-python` is `>=3.9` because the published `sbctl` wheel is installed on
management nodes with whatever system python they have; **the container image runs free-threaded
3.14 (`3.14t`)**, so the supported range spans both and CI tests both ends.

The version lives in `simplyblock_core/env_var` (`SIMPLY_BLOCK_VERSION`), which `release.yml`
rewrites and `constants.py` reads at runtime for `sbctl --version`. `simplyblock_core/_version.py`
is the single reader and is what `[tool.setuptools.dynamic]` imports at build time — keep it free
of third-party imports or the build breaks.

## Build & Install

```bash
uv sync                             # create .venv from uv.lock, project installed editable
uv build                            # sdist + wheel into dist/
uv lock                             # re-resolve after editing [project.dependencies]
uv lock --check                     # CI: fail if uv.lock is stale
```

Dependency groups (PEP 735) replace the old `*-requirements.txt` files: `test`, `types`,
`generate`. Install one with `uv sync --group test`. `e2e/requirements.txt` is separate and
unaffected.

## Testing

Two tiers via tox: `tox run -e unit` (fast, no infra) and `tox run -e integration` (Docker + `libfdb_c` required). See `tests/AGENTS.md` for tier criteria, the testcontainers FDB fixture, and how to reuse an existing dev-compose FDB instance.

Both tiers have a `py314t-` twin (`tox run -e py314t-unit`) running the image's free-threaded
interpreter. The un-prefixed envs use python3.9, the floor the published wheel must keep working
on. tox-uv fetches both interpreters, so neither needs to be installed on the host. **A change
that touches runtime behaviour must be green on both** — the GIL-off build is where a
previously-masked data race surfaces.

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
- **Retries**: Use `tenacity` (`@retry` decorator, or `Retrying`/`AsyncRetrying` for a single call site) instead of hand-written attempt loops with `time.sleep()`. Always set an explicit `stop=` and `wait=`, and log attempts via `before_sleep=before_sleep_log(logger, logging.WARNING)`. Refactor hand-rolled retry loops you touch.
- **Pydantic fields**: Use the [annotated pattern](https://pydantic.dev/docs/validation/latest/concepts/fields/#the-annotated-pattern) for field metadata, not the assignment form. See below.
- **Ruff** and **mypy** are enforced in CI. `simplyblock_cli/cli.py` is excluded from ruff (auto-generated).
- `tests/perf/` is excluded from pytest discovery.

### Pydantic Fields

Applies to every Pydantic model: v2 DTOs and request bodies, internal-API payloads, and `pydantic-settings` classes. It does **not** apply to `simplyblock_core.models.base_model.BaseModel`, which is hand-rolled — its fields stay plain annotations with plain defaults, except that a mutable default is declared as `default_factory(list)` / `default_factory(dict)` (see `simplyblock_core/AGENTS.md` § Data Model Pattern).

Constraints and metadata belong inside `Annotated[...]`. The default stays on the right-hand side of the assignment; a field with no default is required.

```python
# Good
size: Annotated[int, Field(ge=0)]                                   # required
jm_percent: Annotated[int, Field(ge=0, le=100)] = 3                 # optional, default 3
host_nqn: Annotated[Optional[str], Field(pattern=NQN_PATTERN)] = None
name: Annotated[str, Field(description="Key name (used as filename)")]

# Bad
size: int = Field(ge=0)
jm_percent: int = Field(3, ge=0, le=100)
host_nqn: Optional[str] = Field(default=None, pattern=NQN_PATTERN)
name: str = Field(..., description="Key name (used as filename)")
```

Rules:

- `default`, `default_factory` and `alias` are the exception — static type checkers only understand them in assignment form, so never pass them inside `Annotated`. `Annotated[Optional[int], Field(None, ge=0)] = None` declares the default twice; drop it from `Field()`.
- A field carrying no metadata needs no `Field()` at all: write `spdk_debug: bool = False`, not `spdk_debug: Optional[bool] = Field(False)`.
- Required fields need no `Field(...)` sentinel. Omitting the assignment already says "required".
- Put `Optional` inside `Annotated` (`Annotated[Optional[str], Field(...)]`), so the metadata attaches to the field rather than to an inner type. Both forms validate, but only one is the house style.
- Prefer an existing reusable alias over repeating a constraint: `simplyblock_web/api/v2/util.py` defines `Unsigned`, `Size`, `Percent`, `Port` and `UrlPath`; `simplyblock_core/utils/pci.py` defines `PCIAddress`. A constraint you are writing for the third time belongs beside them as a named alias — reusability is the main payoff of the annotated pattern.
- Existing assignment-form declarations are legacy. Convert the ones in a model you are already editing; do not sweep the codebase in an otherwise unrelated change.

### Secret Handling

Secrets (passwords, tokens, keys, connection strings) are wrapped in Pydantic's `SecretStr` / `SecretBytes` throughout the codebase. The core principle is **wrap early, unwrap late**: secrets enter the system wrapped at the boundary (CLI parse, API ingress, DB read) and are only unwrapped to plaintext at the final wire-send moment. Every layer in between sees only masked values in `repr`/`str`/logging.

Key rules:

- **Model fields**: Declare secret fields as `SecretStr` with a `SecretStr("")` default. `BaseModel.from_dict()` auto-wraps inbound plaintext for backward compatibility with existing FDB records. `to_dict()` keeps wrappers by default (safe for logging); `to_dict(unwrap_secrets=True)` produces plaintext for persistence — only `write_to_db()` should call this.
- **Display / logging JSON**: Never call `json.dumps()` directly on a dict that may carry `SecretStr`/`SecretBytes` — it raises `TypeError`. Use `utils.dump_json(data, ...)` (controllers/CLI) or `utils.print_table(data, ...)` (table mode). Both accept `unwrap_secrets=`: pass `True` at operator-display sites (CLI `X get`, `--json` outputs) where the user is authorized to see plaintext; omit it (default `False`, masks as `**********`) for logging, debug dumps, error paths, and anywhere that might land in a log file. When adding a new secret-bearing field to a model, audit every existing `dump_json` / `print_table` callsite that serializes that model and choose the right `unwrap_secrets` value.
- **Clients (RPC, SNode, Firewall API)**: Accept `SecretStr` parameters. Log the payload dict *before* unwrapping (wrappers mask in log output), then call `unwrap_secrets_for_send(payload)` from `simplyblock_core/utils/secrets.py` right before `requests.post(json=...)`.
- **v2 DTOs**: Use `@field_serializer('field', when_used='json')` to unwrap for JSON wire responses while keeping wrappers in Python-mode `model_dump()`.
- **CLI arguments**: Declare the argument type as `secret` in `cli-reference.yaml`. The generator produces `SecretStr` as the argparse type converter, so the value is wrapped at parse time.
- **Logging**: Never log unwrapped secret values. Response-body logging is gated by `Settings().log_response_bodies` (env `SB_LOG_RESPONSE_BODIES`, default `False`). External libraries that log HTTP bodies (`urllib3`, `kubernetes.client.rest`) are silenced to WARNING. The web access log records only `request.url.path`, never the query string.
- **Downstream of the unwrap**: `services/spdk_http_proxy_server.py` receives JSON-RPC bodies that have already been through `unwrap_secrets_for_send`, so no `SecretStr` survives to mask by. Log those through `redact_rpc_params` from `simplyblock_core/utils/secrets.py`, which masks by parameter name (`SENSITIVE_RPC_PARAMS`). An RPC that carries new key material or a new credential adds its parameter name to that set — masking by type in `rpc_client` alone does not reach the proxy.
- **Comparison**: Use `hmac.compare_digest(secret.get_secret_value(), other)` for timing-safe comparison.
- **Testing**: New secret-bearing code needs masking, wire-delivery, and FDB round-trip tests. See `tests/AGENTS.md` § Secret-handling tests for the required assertions and canonical examples.

## Fixing Issues

A reported fault is a symptom. Fix the symptom as asked — but a fix is only complete once you have also answered **why this class of fault was possible here**, and reported that answer.

Always report the systemic cause, even when you are not asked to fix it and even when the fix itself is a one-liner. Silence implies the code was fine apart from one typo, which is usually false.

### What to look for

While tracing the fault, ask:

- **Is an abstraction missing, or is the wrong one in place?** A concept the domain has but the code does not (so it lives as scattered tuples, dicts, or parallel lists), logic that belongs to one owner but is re-implemented at each caller, or a leaky boundary that forces callers to know a lower layer's details — CLI code reaching into FDB shapes, a controller hand-assembling JSON-RPC payloads. Equally, an abstraction that no longer fits: a base class or helper stretched by special-cases and flags until each caller needs different behaviour from it. If the correct fix reads as "remember to do X here too", the missing thing is a place where X happens once.
- **Could the invalid state have been made unrepresentable?** A field that must never be empty, a status that must never be reached from another status, two fields that must agree — if an invariant is enforced by convention rather than by the type or a single guarded accessor, that is the real defect.
- **Does the same latent bug exist elsewhere?** Copy-pasted call sites, sibling controllers, the other half of a create/delete pair, v1 vs. v2 of an endpoint. Grep for the pattern, not just the line.
- **Did an error-handling pattern hide it?** A swallowed exception, a `None`/boolean return standing in for an error, a bare `except`, a retry loop that masks a permanent failure as a transient one. See `CONTRIBUTING.md` and the retry conventions above.
- **Why did no test catch it?** A missing tier (unit vs. integration), a fixture that mocks away the failing layer, an assertion on the happy path only. A regression test for this bug is the minimum; a gap that would let the *next* bug through is a finding.
- **Was the failure observable?** If reproducing it required adding logging, or if the logs pointed at the wrong component, the missing signal is part of the fault.
- **Did an interface invite the mistake?** Positional arguments that are easy to swap, a parameter whose meaning depends on another, a helper that is correct only when called in a specific order.

### How to report it

End the work with a short **Systemic causes** section separate from the description of the fix. For each cause: state it in one or two sentences, point at the code (`file_path:line`), and say what a durable fix would be and roughly what it costs.

Do not silently widen the change to include those fixes. Fix the reported fault plus anything genuinely inseparable from it; propose the rest and let the user decide. The exceptions — apply these without asking — are a regression test covering the fault, and identical instances of the same bug found by grep, which are part of the fix rather than an expansion of it.

## Verification

After any code change, run the `tox-verify` skill (`.agents/skills/tox-verify/SKILL.md`). The short version:

1. `tox run-parallel -e lint,types` — fix lint/type errors first.
2. `tox run -e unit -- tests/unit/path/to/relevant_test.py` — run targeted tests while iterating.
3. `tox run` — full suite before finishing. Never mark work done without a green run.

## Local Development

```bash
sudo docker compose -f docker-compose-dev.yml up --build -d
```

Requires FoundationDB 7.3.3 client library installed on the host for the Python bindings.

## Container Image

`docker/Dockerfile` is a single multi-stage build (`base` -> `builder` -> `runtime`) producing the
control-plane image: UBI 10 plus the OS tooling the runtime shells out to, and `/opt/venv` built by
`uv` from a managed **free-threaded 3.14** interpreter, so the application's Python version is
independent of the platform's.

```bash
docker buildx build -f docker/Dockerfile -t simplyblock:dev --load .   # local
./build_image.sh                                                       # both arches, pushed
```

See **`docker/AGENTS.md`** before editing the Dockerfile. It covers the stage layout and cache
policy (`CACHE_KEY`), and the constraints that are not obvious from reading the file — the sudoers
`secure_path` line the `sudo -E python3` entry points depend on, why the source tree stays at
`/app` with an editable install and what the exit from that looks like, and why there is no
compiler toolchain or `pip` in the image.

## Agent Instructions Layout

`AGENTS.md` is the source of truth at every level. Each `CLAUDE.md` is a one-line stub
whose only content is the `@AGENTS.md` import directive, which makes Claude Code inline the
sibling `AGENTS.md`. Stubs are used rather than symlinks so the checkout works on Windows.

```
AGENTS.md                          ← root instructions (this file)
CLAUDE.md                          ← Claude Code stub: `@AGENTS.md`
.github/copilot-instructions.md → ../AGENTS.md  ← GitHub Copilot (symlink)

simplyblock_cli/AGENTS.md         ← CLI-specific instructions
simplyblock_cli/CLAUDE.md          ← `@AGENTS.md`
simplyblock_core/AGENTS.md        ← Core-specific instructions
simplyblock_core/CLAUDE.md         ← `@AGENTS.md`
simplyblock_web/AGENTS.md         ← Web API-specific instructions
simplyblock_web/CLAUDE.md          ← `@AGENTS.md`
tests/AGENTS.md                   ← Test-suite layout, tiers, fixtures
tests/CLAUDE.md                    ← `@AGENTS.md`
docker/AGENTS.md                  ← Container image: stages, cache policy, Dockerfile constraints
docker/CLAUDE.md                   ← `@AGENTS.md`

.agents/skills/                    ← shared skills (source of truth)
  tox-verify/SKILL.md              ← tox verification workflow
.claude/skills → ../.agents/skills ← Claude Code skill symlink

.agents/hooks/                     ← shared, runner-neutral guard scripts (source of truth)
  guard-dev-compose.py             ← gates use of docker-compose-dev.yml (see tests/AGENTS.md)
.claude/settings.json              ← Claude Code wiring: references the .agents/hooks/ scripts
```

Edit only `AGENTS.md` files and `.agents/skills/` contents. Never edit a `CLAUDE.md` stub or a
symlink target directly.

### Guard hooks

`.agents/hooks/` holds runner-neutral guard scripts that enforce instructions which agents otherwise tend to ignore. Each script reads a pre-tool-execution payload as JSON on stdin and emits a decision on stdout; keep the matching logic in the script (the source of truth) so any agent runner can wire it. Claude Code wires them via `.claude/settings.json` (`PreToolUse` hooks). Other runners (Codex, etc.) that lack a hook system still get the underlying rule because it is also stated in the relevant `AGENTS.md`; wire the same script into their pre-exec hook if/when one is available.

### Local overrides

At every level where an `AGENTS.md` exists, also check for a sibling `AGENTS.local.md`. If present, load it in addition to `AGENTS.md` — its contents extend or override the checked-in instructions. `AGENTS.local.md` is gitignored and intended for per-developer notes that should not be committed.
