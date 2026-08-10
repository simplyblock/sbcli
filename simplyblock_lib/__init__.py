# coding=utf-8
"""simplyblock_lib — infrastructure shared across simplyblock services.

Generic, sbcli-agnostic building blocks extracted from simplyblock_core /
simplyblock_web so that new services (e.g. edge clusters) can reuse them
without duplicating code:

- ``simplyblock_lib.tasks``   — task lease/claim primitives and the poll-loop
  runner base class for DB-backed background tasks.
- ``simplyblock_lib.monitors`` — the two monitor-service skeletons (flat sweep
  loop, thread-per-item supervisor).
- ``simplyblock_lib.events``  — level-mirrored event logging helper.
- ``simplyblock_lib.api``     — FastAPI scaffolding (typed scalars, creation
  response helper, access-log middleware).
- ``simplyblock_lib.units``   — data-size parsing.
- ``simplyblock_lib.secrets`` — SecretStr/SecretBytes unwrap helpers.

Rules for this package:
- No imports from ``simplyblock_core`` / ``simplyblock_web`` / ``simplyblock_cli``
  — dependencies flow the other way. Persistence and models are injected
  (duck-typed) by the caller.
- Heavy third-party imports (fastapi/starlette) stay confined to the submodule
  that needs them so task-runner consumers don't pay for web dependencies.
"""
