---
name: tox-verify
description: >
  Enforce tox as the local test gate before completing any coding task. Use this skill
  whenever making code changes to a Python project that has a tox.ini or pyproject.toml
  with tox config. Triggers on: implementing features, fixing bugs, refactoring, editing
  any .py file, modifying dependencies, or any task that could affect test outcomes.
  Always run tox at the end of a task — never mark work as done without a passing tox run.
  If tox is not yet set up, help the user configure it.
---

# Tox Verification Skill

Always verify your work with `tox` before considering any coding task complete.

**Rationale:** run cheap, simple checks early and frequently — they catch most mistakes
for near-zero cost. Save expensive checks for later, and keep them targeted even then.
Each step below is more expensive than the last; don't skip ahead to a slow step to avoid
a fast one, and don't run a slow step untargeted when a targeted run would do.

## Core Rule

**Never finish a task without a fully green `tox` run.** If anything fails, fix it and
re-run. Do not hand back to the user with a broken suite.

## Workflow

### Step 1 — After every change: lint + types in parallel

Cheapest checks, purely static. Run them after every change, in parallel:

```bash
tox run-parallel -e lint,types
```

Fix any errors before proceeding. Iterating on the test suite with broken types or lint
is wasteful.

### Step 2 — During iteration: targeted unit tests

`unit` (`tests/unit/` + `simplyblock_core/test/`) is fast and needs no infra — the cheapest
test tier. Use it for the fast feedback loop.

```bash
tox run -e unit -- tests/unit/test_secrets.py
tox run -e unit -- tests/unit/test_secrets.py -k "keyword_matching_affected_area"
tox run -e unit -- tests/unit/ --last-failed
```

**Always include a tier path after `--`.** Posargs *replace* the env's default path
(`tests/unit/ simplyblock_core/test/`), not add to it — a bare `-- --last-failed` or
`-- -k foo` falls through to pytest's `testpaths`, which also covers `tests/integration/`,
silently pulling in the other tier.

Repeat steps 1–2 until the targeted unit tests pass.

### Step 3 — Only if the change touches FDB-backed paths: targeted integration tests

`integration` (`tests/integration/`) is the next tier up in cost: it requires Docker
(testcontainers boots a real FoundationDB). Only reach for it when the change is in the
FDB / cluster / migration paths, and keep the run targeted:

```bash
tox run -e integration -- tests/integration/migration/test_migration_flow.py
```

**Podman / rootless Docker socket:** if `DOCKER_HOST` is set in the shell (rootless podman
socket), tox won't forward it by default — the FDB testcontainer falls back to
`/var/run/docker.sock` and the whole tier fails before collecting a test. Pass it through
explicitly:

```bash
tox run -e integration -x testenv:integration.passenv+=DOCKER_HOST -- tests/integration/migration/test_migration_flow.py
```

Use `+=`, not `=` — it appends to the env's existing `passenv` (`PYTEST_ADDOPTS`) instead of
replacing it.

Do not touch `docker-compose-dev.yml` to work around this — that stack is off-limits unless
the user explicitly says to use it.

The `slow`-marked migration suite (`tests/integration/migration/`, ~138 tests, ~20min) is
excluded from `tox run -e integration` by default. Only run it (`tox run -e integration-slow`,
or `-e integration -- <path> -m slow`) when the change specifically touches migration flows —
it's the most expensive tier here.

### Step 4 — Before finishing: full suite

Once targeted checks pass, run everything:

```bash
tox run
```

This covers `lint`, `types`, `unit`, `integration` (still excluding `slow`), and their
`py314t-*` twins on the free-threaded interpreter — the most expensive step, so it runs once,
last. A change that touches runtime behaviour must be green on both the py3.9 and py314t
twins; the GIL-off build is where a previously-masked data race surfaces.

Only report success after this passes clean.

## Handling Failures

- **Lint errors (ruff)**: auto-fix where possible (`ruff check --fix`), then re-run.
- **Type errors (mypy)**: fix the annotations. Don't add `# type: ignore` unless it was pre-existing.
- **Import errors / missing deps**: check `dependency_groups` in `tox.ini` / `pyproject.toml`, fix and re-run.
- **Failing tests**: read the traceback carefully. Fix the root cause — don't patch the assertion unless the test itself is wrong.
- **Environment setup failures**: check the Python version constraint (`basepython` in `tox.ini`).

## What to Report

When handing back to the user, always include:
- What you changed and why
- The final tox result (e.g. `lint: OK  types: OK  unit: OK  integration: OK`)
- Any pre-existing failures you found but did not introduce — flag these explicitly

## Pre-existing Failures

If `tox` was already failing before your changes, say so:
> "⚠️ tox was failing before my changes. I also fixed the pre-existing failure in `test_foo.py`. See diff."

Do not silently inherit broken tests.
