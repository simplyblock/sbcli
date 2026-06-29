# coding=utf-8
"""FoundationDB provisioning for the integration tier.

Resolution order:
  1. If FDB_CLUSTER_FILE is already set and points at a readable file
     (developer's docker-compose-dev.yml, existing cluster, etc.),
     reuse it.
  2. Otherwise, start a foundationdb/foundationdb container via
     testcontainers, wait until it accepts writes, expose its cluster
     file on the host, and set FDB_CLUSTER_FILE for the session.
  3. If neither path works (no docker, no testcontainers), skip every
     test in tests/integration/ with a clear message.

Provisioning happens in ``pytest_configure`` — *before* test collection —
not in a fixture. This is deliberate: several source modules build a
module-level ``db_controller = DBController()`` at import time, and those
imports run during collection. ``DBController`` opens FDB (and the fdb C
client caches the handle by cluster-file path) the moment it is first
constructed. If we only bound the testcontainer's cluster file in a
session fixture — which runs *after* collection — those import-time
singletons would already have opened whatever ``constants.KVD_DB_FILE_PATH``
defaulted to, i.e. the host's ``/etc/foundationdb/fdb.cluster``. When that
stale file points at the same ``127.0.0.1:4500`` as the testcontainer but
with a *different* cluster id, the client can never settle on a coordinator
and every transaction hangs until it trips ``KVD_DB_TIMEOUT_MS`` →
``FDBError 1031`` (observed: only ``test_dual_ft_e2e`` failed, because it is
the test that drives real reads through ``cluster_ops.db_controller``).
Binding before collection makes every import-time singleton open the
testcontainer's cluster file instead.
"""
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

FDB_IMAGE = "foundationdb/foundationdb:7.3.63"
FDB_CLUSTER_CONTENTS = "docker:docker@127.0.0.1:4500"
FDB_READY_TIMEOUT_S = 60

# Provisioning state shared between pytest_configure (setup), the
# ``fdb_cluster`` fixture (skip decision), and pytest_unconfigure (teardown).
_container = None
_tmpdir = None
_skip_reason = None


def _existing_cluster_file_works() -> bool:
    path = os.environ.get("FDB_CLUSTER_FILE")
    return bool(path) and Path(path).is_file()


def _exec(container, *argv):
    """Run a command inside the container; return (rc, combined-output)."""
    return container.exec(list(argv))


def _start_fdb_container():
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(FDB_IMAGE)
        .with_env("FDB_NETWORKING_MODE", "host")
        .with_env("FDB_CLUSTER_FILE_CONTENTS", FDB_CLUSTER_CONTENTS)
        .with_kwargs(network_mode="host")
    )
    container.start()

    deadline = time.monotonic() + FDB_READY_TIMEOUT_S
    configured = False
    last_rc, last_out = -1, b""
    while time.monotonic() < deadline:
        last_rc, last_out = _exec(
            container, "fdbcli", "--exec", "status minimal", "--timeout", "3"
        )
        text = last_out.lower() if isinstance(last_out, (bytes, bytearray)) else str(last_out).encode().lower()
        if last_rc == 0 and b"available" in text and b"unavailable" not in text:
            return container
        if not configured:
            _exec(
                container,
                "fdbcli",
                "--exec",
                "configure new single ssd",
                "--timeout",
                "10",
            )
            configured = True
        time.sleep(1)

    container.stop()
    raise RuntimeError(
        f"FoundationDB did not become available in {FDB_READY_TIMEOUT_S}s "
        f"(last rc={last_rc}, last output={last_out!r})"
    )


def _bind_cluster_file(path: str) -> None:
    """Make ``path`` the active FDB cluster file for the rest of the session.

    Sets both the env var and the already-bound
    ``simplyblock_core.constants.KVD_DB_FILE_PATH`` attribute (evaluated at
    constants-import time, possibly before this runs), so every subsequent
    ``DBController()`` — including module-level singletons constructed during
    collection — opens this cluster file.
    """
    os.environ["FDB_CLUSTER_FILE"] = path

    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = path


def _provision_fdb() -> None:
    """Start (or adopt) a FoundationDB cluster and bind its cluster file.

    Runs from ``pytest_configure`` (before collection). On any unavailability
    it records ``_skip_reason`` rather than raising, so the ``fdb_cluster``
    fixture can skip the tier cleanly instead of erroring every test at setup.
    """
    global _container, _tmpdir, _skip_reason

    if _existing_cluster_file_works():
        _bind_cluster_file(os.environ["FDB_CLUSTER_FILE"])
        return

    try:
        import testcontainers.core.container  # noqa: F401
    except ImportError:
        _skip_reason = "testcontainers not installed — `pip install testcontainers`"
        return

    if shutil.which("docker") is None:
        _skip_reason = "docker not available — integration tier requires Docker"
        return

    try:
        _container = _start_fdb_container()
    except Exception as e:  # noqa: BLE001 - report as a skip, don't crash collection
        _skip_reason = f"FoundationDB testcontainer did not start: {e}"
        return

    _tmpdir = Path(tempfile.mkdtemp(prefix="sbcli-fdb-"))
    cluster_file = _tmpdir / "fdb.cluster"
    cluster_file.write_text(FDB_CLUSTER_CONTENTS)
    _bind_cluster_file(str(cluster_file))


def _teardown_fdb() -> None:
    global _container
    if _container is not None:
        _container.stop()
        _container = None
    os.environ.pop("FDB_CLUSTER_FILE", None)


def pytest_configure(config):
    """Import the real ``fdb`` and provision FDB *before* collection.

    Importing the real ``fdb`` first makes the ``sys.modules.setdefault("fdb",
    MagicMock())`` that a few test modules do at import scope a no-op.
    Provisioning + binding here (rather than in a session fixture) ensures the
    cluster file is bound before any module-level ``DBController()`` is built
    during collection — see the module docstring.
    """
    import fdb  # noqa: F401
    import fdb.tuple  # noqa: F401

    _provision_fdb()


def pytest_unconfigure(config):
    _teardown_fdb()


@pytest.fixture(scope="session", autouse=True)
def fdb_cluster():
    """Expose the bound cluster file; skip the tier if FDB is unavailable."""
    if _skip_reason:
        pytest.skip(_skip_reason)
    yield os.environ.get("FDB_CLUSTER_FILE")


@pytest.fixture(scope="module", autouse=True)
def _clean_fdb_keyspace(fdb_cluster):
    """Wipe the FoundationDB keyspace once at the start of each test module.

    The integration tier shares one cluster for the whole run. Top-level flow
    tests seed their own state but rarely tear down everything the control
    plane writes (events, stats, locks, hublvols, orphaned nodes, …), so the
    keyspace would otherwise grow and leak state between modules. Clearing
    per-module keeps the dataset bounded and gives each module a clean slate,
    without the write load of clearing before every single test. System keys
    (``\\xff`` prefix) are left untouched; no-op when FDB is unavailable.
    """
    from simplyblock_core.db_controller import DBController

    kv_store = DBController().kv_store
    if kv_store is not None:
        kv_store.clear_range(b"\x00", b"\xff")
    yield
