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
import socket
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

FDB_IMAGE = "foundationdb/foundationdb:7.3.63"
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


def _free_port() -> int:
    """Ask the OS for an unused host port.

    The container runs with ``network_mode="host"``, so there is no Docker
    port mapping to allocate one dynamically the usual testcontainers way —
    ``fdbserver`` binds directly to the host's network stack. Racy in the
    same way every "ask the kernel, then bind later" trick is (the port
    could theoretically be taken between here and container start), but the
    window is milliseconds and a collision fails fast with a clear bind
    error instead of corrupting a run.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_fdb_container():
    from docker.errors import APIError
    from testcontainers.core.container import DockerContainer

    port = _free_port()
    cluster_contents = f"docker:docker@127.0.0.1:{port}"

    container = (
        DockerContainer(FDB_IMAGE)
        .with_env("FDB_NETWORKING_MODE", "host")
        .with_env("FDB_PORT", str(port))
        .with_env("FDB_CLUSTER_FILE_CONTENTS", cluster_contents)
        .with_kwargs(network_mode="host")
    )
    container.start()

    deadline = time.monotonic() + FDB_READY_TIMEOUT_S
    configured = False
    last_rc, last_out = -1, b""
    while time.monotonic() < deadline:
        try:
            last_rc, last_out = _exec(
                container, "fdbcli", "--exec", "status minimal", "--timeout", "3"
            )
        except APIError as e:
            # The docker/podman engine can report the container as started
            # before it accepts exec sessions — "container state improper".
            # Under concurrent container creation (parallel testcontainers)
            # this loses far more often than in a single-container run, so
            # treat it as "not ready yet" and keep polling instead of
            # letting it abort startup.
            last_rc, last_out = -1, str(e).encode()
            time.sleep(1)
            continue
        text = last_out.lower() if isinstance(last_out, (bytes, bytearray)) else str(last_out).encode().lower()
        if last_rc == 0 and b"available" in text and b"unavailable" not in text:
            return container, cluster_contents
        if not configured:
            try:
                _exec(
                    container,
                    "fdbcli",
                    "--exec",
                    "configure new single ssd",
                    "--timeout",
                    "10",
                )
                configured = True
            except APIError:
                pass
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
        _container, cluster_contents = _start_fdb_container()
    except Exception as e:  # noqa: BLE001 - report as a skip, don't crash collection
        _skip_reason = f"FoundationDB testcontainer did not start: {e}"
        return

    _tmpdir = Path(tempfile.mkdtemp(prefix="sbcli-fdb-"))
    cluster_file = _tmpdir / "fdb.cluster"
    cluster_file.write_text(cluster_contents)
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

    # Abort rather than skip. A skipped tier is reported as success for all
    # ~1300 tests, so a run that provisioned nothing is indistinguishable from a
    # green one (observed: the Docker socket was unreachable, every test
    # skipped, tox exited 0). Anyone who genuinely wants the tier to no-op
    # without Docker opts in with SB_ALLOW_FDB_SKIP=1; CI never sets it.
    if _skip_reason and os.environ.get("SB_ALLOW_FDB_SKIP") != "1":
        raise pytest.UsageError(
            f"integration tier requires FoundationDB: {_skip_reason}. "
            "Set SB_ALLOW_FDB_SKIP=1 to skip the tier instead of failing."
        )


def pytest_unconfigure(config):
    _teardown_fdb()


def pytest_report_header(config):
    """State up front which cluster the tier is running against, or why it isn't.

    Without this the provisioning outcome is invisible: the tier's failure mode
    is a session-wide skip, which scrolls past as a wall of ``s`` and lets tox
    exit 0 on a run that executed nothing.
    """
    if _skip_reason:
        return f"FoundationDB: UNAVAILABLE — {_skip_reason}"
    return f"FoundationDB: {os.environ.get('FDB_CLUSTER_FILE')}"


@pytest.fixture(autouse=True)
def _no_kubernetes():
    """Seal the Kubernetes boundary for the whole tier.

    Every k8s call in the codebase funnels through
    ``utils.load_kube_config_with_fallback``, which tries in-cluster config and
    then falls back to the developer's ``~/.kube/config``. Unpatched, control
    paths that touch k8s (cluster activation calls
    ``set_storage_mcp_max_unavailable``; the event controllers call
    ``patch_cr_node_status``) issue REAL, untimed HTTPS requests to whatever
    cluster that file names.

    That made the tier's runtime a function of the developer's kubeconfig:
    ``test_dual_ft_e2e`` ran in 9.8s the day the configured API server refused
    fast, and 272s — six timeouts — once it started accepting connections
    without answering. CI has no kubeconfig, so raising here is not a new
    behaviour, it is the environment these tests were written against, pinned
    so it holds everywhere. Callers already handle an unavailable config; that
    is the branch they take on any non-k8s deployment.
    """
    from kubernetes.config.config_exception import ConfigException

    from simplyblock_core import utils

    def _refuse():
        raise ConfigException(
            "Kubernetes access is disabled in tests (tests/integration/conftest.py)")

    with patch.object(utils, "load_kube_config_with_fallback", _refuse):
        yield


@pytest.fixture(scope="session", autouse=True)
def fdb_cluster():
    """Expose the bound cluster file; fail the tier if FDB is unavailable.

    A skip here would be reported as success for all ~1300 tests — a run that
    provisioned nothing is indistinguishable from a green one (observed: the
    Docker socket was unreachable, every test skipped, tox exited 0). Anyone who
    genuinely wants the tier to no-op without Docker sets ``SB_ALLOW_FDB_SKIP=1``
    explicitly; CI never does.
    """
    if _skip_reason:
        pytest.skip(_skip_reason)
    yield os.environ.get("FDB_CLUSTER_FILE")


@pytest.fixture(autouse=True)
def _clean_fdb_keyspace(fdb_cluster):
    """Wipe the FoundationDB keyspace before every test.

    The integration tier shares one cluster for the whole run. Tests seed their
    own state but rarely tear down everything the control plane writes (events,
    stats, locks, hublvols, orphaned nodes, suspended migrations/tasks, …), so
    without a clean slate that residue leaks *between tests* and produces
    order-dependent flakiness (a later test trips over a random-UUID orphan node
    or a stale migration left by an earlier one). Wiping per-test — not merely
    per-module — is what gives each test true isolation.

    Function-scoped and autouse, so it runs before the per-test state-building
    fixtures (``ensure_cluster``, the ``topology_*`` fixtures, ``ftt2_env``,
    ``cluster_env``); those are all function-scoped too and recreate their own
    state each test, so nothing depends on cross-test persistence. A single
    range-clear per test is cheap relative to the tests themselves. System keys
    (``\\xff`` prefix) are left untouched; no-op when FDB is unavailable.
    """
    from simplyblock_core.db_controller import DBController

    kv_store = DBController().kv_store
    if kv_store is not None:
        kv_store.clear_range(b"\x00", b"\xff")
    yield
