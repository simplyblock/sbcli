# AGENTS.md — tests

Two-tier test suite for the Simplyblock control plane. Unit tests run as pure logic with the FDB module stubbed; integration tests exercise full controller flows against a **real FoundationDB** (storage nodes stay mocked).

## Layout

```
tests/
├── conftest.py          # Clears DBController/RPC caches before each test. Does NOT stub `fdb`.
├── _mocks.py            # Shared mock factories (e.g. `make_mock_cluster`).
├── unit/                # Pure-logic tests; single module under test, no model state, no flows.
│   ├── conftest.py      # Stubs the native `fdb` module so unit tests run without libfdb_c / a live cluster.
│   ├── models/          # Every `BaseModel` test: field defaults, secrets, chunked reads, serialization.
│   └── web/             # Unit tests for simplyblock_web (settings, v2 auth).
├── integration/         # Flow/controller tests — ALL run against real FDB.
│   ├── conftest.py      # `pytest_configure` provisions FDB (testcontainers) before collection; autouse per-test keyspace wipe.
│   ├── ftt2/            # FTT=2 restart scenarios.
│   ├── migration/       # Live volume migration.
│   └── expansion_sim/   # Cluster-expansion simulator (rebinds the real fdb client, routes RPC to simulators).
└── perf/                # Performance scripts; excluded from pytest discovery via `norecursedirs`.
```

## Tiers

### `tests/unit/`

Pure-logic tests. Pick this tier when the test:

- Imports a single module under test and mocks its dependencies (`unittest.mock`).
- Doesn't build full `Cluster` / `StorageNode` / `LVol` / `Pool` state.
- Doesn't drive controller flows (failover, restart, takeover, migration, expand, …).

`tests/unit/conftest.py` stubs out `fdb` (returning `None` from `fdb.open`), so unit tests never touch a real database. Run with `tox run -e unit` — no Docker, no infra.

**Never stand in for the database in a unit test.** The tier split is *about* the database, and there are only two positions:

- the test does not interact with the database API **at all** → `tests/unit/`;
- the test interacts with the **real** database API → `tests/integration/`.

A `_FakeDB`, a patched `DBController`, a module's `db` / `db_controller` singleton swapped for a mock, an assigned `kv_store` — each of these invents a third position that does not exist. It neither avoids the database nor exercises it: it asserts against a second, undeclared copy of the `DBController` interface that nothing keeps in sync with the real one. Such a test stays green until production code calls an accessor the fake never implemented, and then fails on an `AttributeError` that says nothing about the behaviour under test.

That is not hypothetical. The snapshot-replication retention suite had a `_FakeDB` implementing the four accessors retention used when it was written; the moment retention began consulting `get_replication_policy_for_lvol`, twelve tests failed on a missing attribute rather than on anything about retention. They are now `tests/integration/test_snapshot_replication_retention.py`, seeding real models and reading them back through a live `DBController`.

So, when a unit test seems to need a fake DB: **the need is the signal that it is not a unit test.** Move it to `tests/integration/`, seed real models with `write_to_db(db.kv_store)`, and mock only what sits above the database. If the code under test does not actually need the database, delete the stand-in rather than feeding it a fake.

`.agents/hooks/guard-fake-db-tests.py` enforces this (see the root `AGENTS.md` § Guard hooks). It is a **ratchet**: the suite carries ~400 pre-existing stand-ins, so the guard compares each file before and after an edit and refuses only what that edit *introduces*. Editing or cleaning up a grandfathered file is unaffected. `python3 .agents/hooks/guard-fake-db-tests.py --scan` lists what is left.

> **Migration in progress, here too.** `simplyblock_core/test/` is a second unit-tier directory (also collected by `tox run -e unit`, with its own `fdb` stub) and holds the densest concentration of these fakes. Treat it as legacy: don't add files there, and convert what you touch.

### `tests/integration/`

Controller-flow tests, **all of which run against a real FoundationDB**. `tests/integration/conftest.py` provisions FDB once for the whole tier from `pytest_configure` — *before* test collection — reusing `$FDB_CLUSTER_FILE` if set, otherwise starting a `testcontainers` container and binding its cluster file into `simplyblock_core.constants`. Provisioning at `pytest_configure` (rather than in a session fixture) means the real `fdb` client and a live `DBController()` are available at **collection / module-import time**, so test modules may touch the DB at import scope. A separate autouse fixture wipes the user keyspace before every test for isolation. The FDB-backed subdirs (`ftt2/`, `migration/`, `expansion_sim/`) add their own per-suite topology/bootstrap fixtures on top of that same cluster.

**Never spoof or mock the database layer in an integration test.** The whole point of the tier is to exercise real `DBController` → FoundationDB reads and writes. Concretely, in `tests/integration/` do **not**:

- `sys.modules.setdefault("fdb", MagicMock())` (or otherwise stub the `fdb` module),
- patch `DBController`, or
- assign `db.kv_store = MagicMock()` / mock `write_to_db` / `get_*` DB accessors.

Build real model objects and persist them with `write_to_db(db.kv_store)`; read them back through `DBController()`. The `ftt2/` and `migration/` conftests show the canonical pattern (real `Cluster`/`StorageNode`/`LVol` written to FDB, torn down after).

> **Migration in progress.** Several top-level files still carry the old stubbed-DB pattern (`test_cluster_duplicate_name.py`, `test_dual_fault_tolerance.py`, `test_backup.py`, …). These are the broken tests being sorted onto real FDB — do not copy them, and convert them when you touch them. As a transition guard, `integration/conftest.py`'s `pytest_configure` imports the real `fdb` before collection so a stray `setdefault("fdb", MagicMock())` becomes a no-op instead of poisoning the session.

What you *may* still mock: everything **above** the database. Storage nodes are always mocked (in-process `RPCClient`/`SNodeClient` mock servers — see the per-suite `mock_rpc_server` fixtures), and external side-effects (firewall API, `ping_host`, k8s lookups, `time.sleep`, distrib-map sends) are patched. The integration tier never starts SPDK. The line is: real DB, mocked nodes.

## Running tests

```bash
tox run -e unit             # Fast; no infra.
tox run -e integration      # Requires Docker + libfdb_c on host. EXCLUDES slow tests.
tox run -e integration-slow # Slow tier only (the migration suite, ~20min).
```

Each tier also has a `py314t-` twin (`tox run -e py314t-unit`, `py314t-integration`) running the
free-threaded 3.14 interpreter the container image ships; the un-prefixed envs use python3.9, the
floor `requires-python` promises. tox-uv fetches both, so neither has to be installed on the host.

`passenv` is deliberately minimal — it lists only `PYTEST_ADDOPTS`, so a machine-specific value
cannot leak into a run and change its result. Pass local infrastructure overrides on the command
line with `-x` instead of widening it. The common case is a non-default container socket: the FDB
testcontainer otherwise falls back to `/var/run/docker.sock` and the tier dies with
`PermissionError(13)` before collecting a test.

```bash
# rootless podman (DOCKER_HOST already exported in the shell)
tox run -e integration -x testenv:integration.passenv+=DOCKER_HOST

# or skip the container entirely by pointing at an FDB you already run
FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster \
  tox run -e integration -x testenv:integration.passenv+=FDB_CLUSTER_FILE
```

### Test timeouts

Every test has a **budget**, enforced by `pytest-timeout`. A test that blows it fails
where it stalled, with a stack dump naming the line — instead of hanging the run.

| Scope | Budget | Set in |
|---|---|---|
| Repo-wide default | **30s** | `timeout` in `pyproject.toml` |
| `tests/integration/migration/` | 120s | `MIGRATION_DEFAULT_TIMEOUT`, applied by the collection hook in that tier's `conftest.py` |
| Individual test | whatever it declares | `@pytest.mark.timeout(N)` on the test |

The budget covers fixture setup too (`timeout_func_only` is left at `false`), so a stall
in a fixture fails the same way.

**A test that needs longer must say so out loud:**

```python
@pytest.mark.timeout(60)  # StressRunner sleeps 10s of real wall clock by design
def test_long_running_stress_on_tertiary(self, ftt2_env):
```

Always write the *why* next to the marker. A new override is a claim that the test is
genuinely slow — treat it as something to justify in review, not a formality. Before
adding one, check you are not looking at a stall instead:

> **The trap that motivated these budgets.** Fixtures across this tier patch
> `time.sleep` to a no-op (`patch('simplyblock_core.storage_node_ops.time.sleep')` and
> friends). Any production loop shaped `while time.time() < deadline: …; time.sleep(n)`
> then stops sleeping but keeps its wall-clock deadline — it becomes a **hot spin**
> burning the full timeout in CPU. Two of these cost the tier ~40 minutes per run.
> Production loops paced this way must be bounded by round count as well as by deadline
> (see `_kill_spdk_until_dead` in `storage_node_ops.py` and
> `_wait_for_full_device_connectivity` in `cluster_ops.py`). If a test is unexpectedly
> slow, look for this shape before reaching for a marker.

Both integration envs run with `--durations=10`, so the slowest tests are printed on
every run and drift is visible before it trips a timeout.

### Randomness and seeds

`pytest-randomly` picks a **fresh seed on every run** and prints it in the report
header (`Using --randomly-seed=1234567`), so runs explore different stochastic paths
and any run can be replayed exactly:

```bash
PYTEST_ADDOPTS="--randomly-seed=1234567" tox run -e unit    # replay a specific run
PYTEST_ADDOPTS="--randomly-seed=last" tox run -e unit       # replay the previous local run
```

`last` reads pytest's cache directory, which CI does not persist — **re-running a CI
job picks a new seed and will not reproduce the failure.** Copy the seed out of the
failed run's header instead.

The global RNG is reseeded before each test to `seed + crc32(nodeid)`, so a test's
draws don't depend on what ran before it or on xdist sharding. Ordinary tests should
just use the `random` module; there is no fixture to request and nothing to wire up.

**`random.seed()` is banned in `tests/`** (ruff TID251) — it overrides the session
seed and breaks replay for every test that follows.

**Threads must be handed their own RNG.** A shared stream consumed from several
threads has a nondeterministic draw *order*, so seeding alone won't replay. Draw the
child seed in the spawning thread, at a fixed point:

```python
worker_rng = random.Random(random.getrandbits(64))   # in start(), not in the worker
t = threading.Thread(target=self._worker, args=(i, worker_rng))
```

`StressRunner.start` in `integration/ftt2/test_restart_concurrent_ops.py` is the
canonical example, and `MockRpcServer` (`integration/migration/mock_rpc_server.py`)
gives each node its own stream, reseeded per test in `reset_state()`, so the src/tgt
servers can't perturb each other's failure patterns.

A seed replays the *values* each thread draws, not the *interleaving* between them.
Concurrency tests must therefore assert invariants, not exact outcomes.

Don't patch stdlib `random` attributes (`patch("...utils.random.random", ...)`) —
that mutates the module process-wide, for every thread. Rebind the name in the module
under test instead (see `unit/test_soak_outage_gap.py`).

### Slow tests (the `slow` marker)

The migration suite (`tests/integration/migration/`, ~138 tests, ~20min) is tagged
`slow` and **excluded from the default `integration` run**. Tests there are tagged
automatically by `tests/integration/migration/conftest.py` — you don't mark them
individually. To mark a slow test elsewhere, add `@pytest.mark.slow` (the marker is
registered in `pyproject.toml`).

- `tox run -e integration` → runs `-m "not slow"` (fast tier; what `tox run` and the
  default CI job use).
- `tox run -e integration-slow` → runs `-m slow` (the migration suite; CI runs this in
  a separate `integration-slow` job with a longer timeout).
- Targeted runs still opt in via posargs (posargs replace the default filter):
  `tox run -e integration -- tests/integration/migration -m slow`.

The `integration` env brings up FoundationDB through `testcontainers` (image `foundationdb/foundationdb:7.3.63`, ~3–5s boot, pulled on first run). **By default, always run the integration tests through tox and rely on this testcontainers-provisioned FDB** — do not point them at another cluster.

> **Hard rule — do not touch the dev compose stack on your own.** `docker-compose-dev.yml` (and reusing its FDB instance) is **off-limits unless the user has explicitly told you, in this conversation, to use it.** That includes "just checking what's already running" — do **not** run `docker compose -f docker-compose-dev.yml ps`/`up`/`exec` or probe for a cluster file as a setup step. There is no default-path reason to invoke it; reach only for `tox run -e integration`. This is enforced for hook-capable agent runners by `.agents/hooks/guard-dev-compose.py` (wired into Claude Code via `.claude/settings.json`), which intercepts any command referencing `docker-compose-dev.yml` and requires explicit human confirmation.

**Reuse an existing FDB instance** only when the user has explicitly instructed it (e.g. a developer with `docker-compose-dev.yml` already up who wants to avoid the container boot). In that case, export a working cluster file before tox runs:

```bash
sudo docker compose -f docker-compose-dev.yml up -d fdb-server
docker compose -f docker-compose-dev.yml exec fdb-server cat /etc/foundationdb/fdb.cluster > /tmp/fdb.cluster
export FDB_CLUSTER_FILE=/tmp/fdb.cluster
tox run -e integration   # pytest_configure detects the env var and skips its own container.
```

**Targeted runs** use `{posargs}` passthrough:

```bash
tox run -e unit -- tests/unit/models/test_secrets.py -v
tox run -e integration -- tests/integration/migration/test_migration_flow.py -v
```

## Adding a test

1. Decide the tier using the rules above. When in doubt, prefer `unit/` — if the test ends up needing real models, move it.
2. Place test files under `tests/unit/` or `tests/integration/` (mirroring the source layout is fine but not required). **Never drop a `test_*.py` directly in `tests/`** — it belongs to no tier, is selected by neither tox env, and gets no tier-specific conftest. `tests/conftest.py`'s `pytest_configure` enforces this: any top-level `tests/test_*.py` aborts collection (both `unit` and `integration`) with a `UsageError` telling you to move it.
3. Reuse `from tests._mocks import make_mock_cluster` for mock `Cluster` objects rather than rebuilding the same fixture.
4. For new FDB-backed scenarios, prefer extending `ftt2/`, `migration/`, or `expansion_sim/` over inventing another conftest — they already provide topology/cluster bootstrap on top of the tier-wide keyspace wipe. Persist real models to FDB; never mock the DB layer (see the integration-tier rules above).
5. Never edit `tests/CLAUDE.md` — it is a stub that imports this file via `@AGENTS.md`.

## Secret-handling tests

New code that carries secrets (passwords, tokens, keys, connection strings — wrapped as `SecretStr`/`SecretBytes`) needs tests that verify:

1. **Plaintext never appears in `repr` / `str` / log output.** Use `caplog` or capture the rendered `repr` and assert the masked form (`**********`) is present and the raw value is not.
2. **Plaintext is delivered on the wire.** Call `unwrap_secrets_for_send` (or the client's send path) and assert the outgoing payload contains the unwrapped value.
3. **FDB round-trip preserves the value.** Construct the model, `to_dict(unwrap_secrets=True)`, `from_dict(...)`, and assert the secret is still a `SecretStr` with the right plaintext.
4. **Display-JSON survives serialization.** When adding a new secret field to a model that's already serialized to JSON in some CLI/controller path, add a regression assertion that `utils.dump_json(model.get_clean_dict())` succeeds and masks, and that `unwrap_secrets=True` recovers the plaintext. Raw `json.dumps(get_clean_dict())` will crash on a `SecretStr` — that's the failure mode this catches.

Canonical patterns:

- `tests/unit/test_secret_redaction.py` — `repr`/`str`/`pprint`/log formatter masking.
- `tests/unit/test_client_secret_logging.py` — log-then-unwrap pattern for RPC + SNode clients.
- `tests/unit/models/test_secrets.py` — FDB round-trip.
- `tests/unit/test_display_helpers.py` — `utils.dump_json` / `utils.print_table` masking + unwrap.

## Verification

After changes, follow the `tox-verify` skill (`.agents/skills/tox-verify/SKILL.md`):

1. `tox run-parallel -e lint,types`
2. `tox run -e unit -- <changed paths>` for iteration.
3. `tox run -e integration` only when the change touches FDB-backed paths.

Never mark work done without a green targeted run.
