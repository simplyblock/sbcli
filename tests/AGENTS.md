# AGENTS.md — tests

Two-tier test suite for the Simplyblock control plane. Unit tests run as pure logic with the FDB module stubbed; integration tests exercise full controller flows against a **real FoundationDB** (storage nodes stay mocked).

## Layout

```
tests/
├── conftest.py          # Clears DBController/RPC caches before each test. Does NOT stub `fdb`.
├── _mocks.py            # Shared mock factories (e.g. `make_mock_cluster`).
├── conftest_proxy.py    # `import_proxy_module()` helper that neutralizes spdk_http_proxy_server's module-level run_server side-effect; used by both proxy unit + e2e tests.
├── unit/                # Pure-logic tests; single module under test, no model state, no flows.
│   ├── conftest.py      # Stubs the native `fdb` module so unit tests run without libfdb_c / a live cluster.
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
tox run -e unit          # Fast; no infra.
tox run -e integration   # Requires Docker + libfdb_c on host.
```

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
tox run -e unit -- tests/unit/test_secrets.py -v
tox run -e integration -- tests/integration/migration/test_migration_flow.py -v
```

## Adding a test

1. Decide the tier using the rules above. When in doubt, prefer `unit/` — if the test ends up needing real models, move it.
2. Place test files directly under `tests/unit/` or `tests/integration/`. Mirroring the source layout is fine but not required.
3. Reuse `from tests._mocks import make_mock_cluster` for mock `Cluster` objects rather than rebuilding the same fixture.
4. For new FDB-backed scenarios, prefer extending `ftt2/`, `migration/`, or `expansion_sim/` over inventing another conftest — they already provide topology/cluster bootstrap on top of the tier-wide keyspace wipe. Persist real models to FDB; never mock the DB layer (see the integration-tier rules above).
5. Never edit symlink targets — `tests/CLAUDE.md` is a symlink to this file.

## Secret-handling tests

New code that carries secrets (passwords, tokens, keys, connection strings — wrapped as `SecretStr`/`SecretBytes`) needs tests that verify:

1. **Plaintext never appears in `repr` / `str` / log output.** Use `caplog` or capture the rendered `repr` and assert the masked form (`**********`) is present and the raw value is not.
2. **Plaintext is delivered on the wire.** Call `unwrap_secrets_for_send` (or the client's send path) and assert the outgoing payload contains the unwrapped value.
3. **FDB round-trip preserves the value.** Construct the model, `to_dict(unwrap_secrets=True)`, `from_dict(...)`, and assert the secret is still a `SecretStr` with the right plaintext.
4. **Display-JSON survives serialization.** When adding a new secret field to a model that's already serialized to JSON in some CLI/controller path, add a regression assertion that `utils.dump_json(model.get_clean_dict())` succeeds and masks, and that `unwrap_secrets=True` recovers the plaintext. Raw `json.dumps(get_clean_dict())` will crash on a `SecretStr` — that's the failure mode this catches.

Canonical patterns:

- `tests/unit/test_secret_redaction.py` — `repr`/`str`/`pprint`/log formatter masking.
- `tests/unit/test_client_secret_logging.py` — log-then-unwrap pattern for RPC + SNode clients.
- `tests/unit/test_basemodel_secrets.py` — FDB round-trip.
- `tests/unit/test_display_helpers.py` — `utils.dump_json` / `utils.print_table` masking + unwrap.

## Verification

After changes, follow the `tox-verify` skill (`.agents/skills/tox-verify.md`):

1. `tox run-parallel -e lint,types`
2. `tox run -e unit -- <changed paths>` for iteration.
3. `tox run -e integration` only when the change touches FDB-backed paths.

Never mark work done without a green targeted run.
