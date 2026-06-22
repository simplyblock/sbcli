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

The `tests` environment is too slow for iteration.
Run only the tests relevant to your current changes:

**Previously failing tests** (re-run what was broken):
```bash
tox run -e tests -- --last-failed
```

**Tests related to files you changed** (pytest-based, using path or keyword):
```bash
tox run -e tests -- tests/path/to/relevant_test.py
tox run -e tests -- -k "keyword_matching_affected_area"
```

Use your judgment to pick the right scope. If you changed `src/foo/bar.py`, run
`tests/foo/test_bar.py` plus any integration tests that import from that module.

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
- The final tox result (e.g. `lint: OK  types: OK  tests: OK`)
- Any pre-existing failures you found but did not introduce — flag these explicitly

## Pre-existing Failures

If `tox` was already failing before your changes, say so:
> "⚠️ tox was failing before my changes. I also fixed the pre-existing failure in `test_foo.py`. See diff."

Do not silently inherit broken tests.
