# AGENTS.md — tests

Two-tier test suite for the Simplyblock control plane. Unit tests run as pure logic with the FDB module stubbed; integration tests exercise full controller flows against a real FoundationDB (storage nodes stay mocked).

## Layout

```
tests/
├── conftest.py          # Stubs `fdb` module + clears DBController/RPC caches; applies to both tiers.
├── _mocks.py            # Shared mock factories (e.g. `make_mock_cluster`).
├── conftest_proxy.py    # `import_proxy_module()` helper that neutralizes spdk_http_proxy_server's module-level run_server side-effect; used by both proxy unit + e2e tests.
├── unit/                # Pure-logic tests; single module under test, no model state, no flows.
│   └── web/             # Unit tests for simplyblock_web (settings, v2 auth).
├── integration/         # Flow/controller tests + FDB-required subdirs.
│   ├── conftest.py      # Session-scoped testcontainers fixture that provisions FDB.
│   ├── ftt2/            # FTT=2 restart scenarios; requires real FDB.
│   ├── migration/       # Live volume migration; requires real FDB.
│   └── expansion_sim/   # Cluster-expansion simulator; requires real FDB and rebinds the stub.
└── perf/                # Performance scripts; excluded from pytest discovery via `norecursedirs`.
```

## Tiers

### `tests/unit/`

Pure-logic tests. Pick this tier when the test:

- Imports a single module under test and mocks its dependencies (`unittest.mock`).
- Doesn't build full `Cluster` / `StorageNode` / `LVol` / `Pool` state.
- Doesn't drive controller flows (failover, restart, takeover, migration, expand, …).

The repo-wide `tests/conftest.py` stubs out `fdb`, so unit tests never touch a real database. Run with `tox run -e unit` — no Docker, no infra.

### `tests/integration/`

Controller-flow tests + FDB-backed subdirs. Two flavors live here:

1. **Stubbed-FDB flow tests** at the top of `tests/integration/` — build full models, patch `RPCClient`/`SNodeClient`, run against the stubbed FDB from the repo-wide conftest.
2. **Real-FDB subdirs** (`ftt2/`, `migration/`, `expansion_sim/`) — each has its own conftest that either skips on missing `kv_store` or replaces the stub with the real `fdb` module. The session-scoped `tests/integration/conftest.py` fixture provisions FoundationDB up-front so all three flavors find a working cluster.

Storage nodes are always mocked. The integration tier never starts SPDK.

## Running tests

```bash
tox run -e unit          # Fast; no infra.
tox run -e integration   # Requires Docker + libfdb_c on host.
```

The `integration` env brings up FoundationDB through `testcontainers` (image `foundationdb/foundationdb:7.3.63`, ~3–5s boot, pulled on first run).

**Reuse an existing FDB instance** (developer with `docker-compose-dev.yml` already up) by exporting a working cluster file before tox runs:

```bash
sudo docker compose -f docker-compose-dev.yml up -d fdb-server
docker compose -f docker-compose-dev.yml exec fdb-server cat /etc/foundationdb/fdb.cluster > /tmp/fdb.cluster
export FDB_CLUSTER_FILE=/tmp/fdb.cluster
tox run -e integration   # Fixture detects the env var and skips its own container.
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
4. For new FDB-backed scenarios, prefer extending `ftt2/`, `migration/`, or `expansion_sim/` over inventing another conftest. They already handle keyspace cleanup and cluster bootstrap.
5. Never edit symlink targets — `tests/CLAUDE.md` is a symlink to this file.

## Secret-handling tests

New code that carries secrets (passwords, tokens, keys, connection strings — wrapped as `SecretStr`/`SecretBytes`) needs tests that verify:

1. **Plaintext never appears in `repr` / `str` / log output.** Use `caplog` or capture the rendered `repr` and assert the masked form (`**********`) is present and the raw value is not.
2. **Plaintext is delivered on the wire.** Call `unwrap_secrets_for_send` (or the client's send path) and assert the outgoing payload contains the unwrapped value.
3. **FDB round-trip preserves the value.** Construct the model, `to_dict(unwrap_secrets=True)`, `from_dict(...)`, and assert the secret is still a `SecretStr` with the right plaintext.

Canonical patterns:

- `tests/unit/test_secret_redaction.py` — `repr`/`str`/`pprint`/log formatter masking.
- `tests/unit/test_client_secret_logging.py` — log-then-unwrap pattern for RPC + SNode clients.
- `tests/unit/test_basemodel_secrets.py` — FDB round-trip.

## Verification

After changes, follow the `tox-verify` skill (`.agents/skills/tox-verify.md`):

1. `tox run-parallel -e lint,types`
2. `tox run -e unit -- <changed paths>` for iteration.
3. `tox run -e integration` only when the change touches FDB-backed paths.

Never mark work done without a green targeted run.
