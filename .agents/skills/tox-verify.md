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

Always verify your work with `tox` before considering any coding task complete. Use a
fast feedback loop during iteration, and the full suite only at the end.

## Core Rule

**Never finish a task without a fully green `tox` run.** If anything fails, fix it and
re-run. Do not hand back to the user with a broken suite.

## Workflow

### Step 1 — After every change: lint + types in parallel

Run lint and type checking first. They're fast and catch most mistakes immediately.
Always run them in parallel:

```bash
tox run-parallel -e lint,types
```

Fix any errors before proceeding. Iterating on the test suite with broken types or lint
is wasteful.

### Step 2 — During iteration: targeted tests only

There are two test envs:

- `unit` — pure-logic tests under `tests/unit/` and `simplyblock_core/test/`. Fast, no infra. Use for iteration.
- `integration` — FDB-backed tests under `tests/integration/`. Requires Docker (testcontainers brings up FoundationDB). Slower; only run when the change touches paths exercised there.

Run only the tests relevant to your current changes:

**Previously failing tests** (re-run what was broken):
```bash
tox run -e unit -- --last-failed
```

**Tests related to files you changed** (pytest-based, using path or keyword):
```bash
tox run -e unit -- tests/unit/test_secrets.py
tox run -e unit -- -k "keyword_matching_affected_area"
tox run -e integration -- tests/integration/migration/test_migration_flow.py
```

Use your judgment to pick the right scope. If you changed `simplyblock_core/foo.py`, run the relevant `tests/unit/` files; only reach for `integration` if your change is in the FDB / cluster / migration paths.

Repeat steps 1–2 until the targeted tests pass.

### Step 3 — Before finishing: full suite

Once you believe the task is complete, run everything:

```bash
tox run
```

Only report success after this passes clean.

## Handling Failures

- **Lint errors (flake8/ruff/black)**: auto-fix where possible (`ruff check --fix`, `black .`), then re-run.
- **Type errors (mypy/pyright)**: fix the annotations. Don't add `# type: ignore` unless it was pre-existing.
- **Import errors / missing deps**: check the `deps` section in `tox.ini`, fix and re-run.
- **Failing tests**: read the traceback carefully. Fix the root cause — don't patch the assertion unless the test itself is wrong.
- **Environment setup failures**: check the Python version constraint in `tox.ini`.

## What to Report

When handing back to the user, always include:
- What you changed and why
- The final tox result (e.g. `lint: OK  types: OK  unit: OK  integration: OK`)
- Any pre-existing failures you found but did not introduce — flag these explicitly

## Pre-existing Failures

If `tox` was already failing before your changes, say so:
> "⚠️ tox was failing before my changes. I also fixed the pre-existing failure in `test_foo.py`. See diff."

Do not silently inherit broken tests.
