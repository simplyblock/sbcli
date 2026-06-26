# coding=utf-8
"""FoundationDB provisioning for the integration tier.

Resolution order (evaluated at conftest module load, before any test file
imports ``simplyblock_core.constants`` — whose ``KVD_DB_FILE_PATH`` reads
``$FDB_CLUSTER_FILE`` once at import time):

  1. If FDB_CLUSTER_FILE is already set and points at a readable file
     (developer's docker-compose-dev.yml, existing cluster, etc.),
     reuse it.
  2. Otherwise, start a foundationdb/foundationdb container via
     testcontainers, wait until it accepts writes, write its cluster
     contents to a temp file, and set FDB_CLUSTER_FILE for the session.
  3. If neither path works (no docker, no testcontainers), defer the
     skip to the session fixture below so every integration test shows
     a clear "FDB unavailable" message instead of an obscure crash.

Provisioning runs at module-load time (not inside a pytest fixture) so
``simplyblock_core.constants.KVD_DB_FILE_PATH`` — captured at import — sees
the testcontainers-provisioned path. Per-test keyspace cleanup and
cluster-record bootstrap live in the sub-package conftests (ftt2/,
migration/, expansion_sim/).
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


_provisioned_container = None
_provisioning_error: str = ""


def _provision_fdb_at_module_load():
    global _provisioned_container, _provisioning_error

    if _existing_cluster_file_works():
        return

    try:
        import testcontainers.core.container  # noqa: F401
    except ImportError:
        _provisioning_error = "testcontainers not installed — `pip install testcontainers`"
        return

    if shutil.which("docker") is None:
        _provisioning_error = "docker not available — integration tier requires Docker"
        return

    try:
        container = _start_fdb_container()
    except Exception as exc:
        _provisioning_error = f"FoundationDB container failed to start: {exc}"
        return

    tmp = Path(tempfile.mkdtemp(prefix="sbcli-fdb-"))
    cluster_file = tmp / "fdb.cluster"
    cluster_file.write_text(FDB_CLUSTER_CONTENTS)
    os.environ["FDB_CLUSTER_FILE"] = str(cluster_file)
    _provisioned_container = container


_provision_fdb_at_module_load()


@pytest.fixture(scope="session", autouse=True)
def fdb_cluster():
    if not _existing_cluster_file_works():
        pytest.skip(_provisioning_error or "FoundationDB cluster file unavailable")
    try:
        yield os.environ["FDB_CLUSTER_FILE"]
    finally:
        if _provisioned_container is not None:
            _provisioned_container.stop()
            os.environ.pop("FDB_CLUSTER_FILE", None)
