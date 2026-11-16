"""A local PyKMIP reference server for KMIPClient integration tests.

Pure Python, no Docker: ``pykmip`` ships both a KMIP client and server, so the
server runs as a subprocess of the test run itself (see
``_pykmip_server_runner.py`` for why it must be a subprocess rather than a
thread), with a throwaway self-signed certificate generated per session.

This validates protocol-level correctness against a real KMIP wire
implementation. It does NOT validate that a production-grade KMIP appliance
behaves the same way for every operation this backend uses -- see
``test_cosmian_kms_client.py`` for that, and the KMIP backend's design notes
in ``simplyblock_core/kms/_kmip.py`` for the one operation
(Encrypt/Decrypt under GCM) where this reference server's own wrapper has a
gap the production code works around.
"""
import datetime
import ipaddress
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

PYKMIP_READY_TIMEOUT_S = 15


@dataclass(frozen=True)
class PyKMIPServerInfo:
    host: str
    port: int
    certificate: Path
    key: Path
    ca: Path

    @property
    def server_url(self) -> str:
        return f"{self.host}:{self.port}"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _generate_self_signed_cert(cert_path: Path, key_path: Path) -> None:
    """A throwaway cert, self-signed and reused as its own CA and as the
    client cert: PyKMIP's server requires a client certificate for every
    connection regardless of ``enable_tls_client_auth`` (it unconditionally
    sets ``ssl.CERT_REQUIRED``), and a self-signed cert trivially verifies
    against itself."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))


def _wait_until_listening(host: str, port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError as e:
            last_error = e
            time.sleep(0.2)
    raise RuntimeError(f"PyKMIP server did not start listening on {host}:{port} "
                        f"within {timeout_s}s") from last_error


@pytest.fixture(scope="session")
def pykmip_server():
    """Session-scoped: one reference server for every test in this directory.

    Tests must use distinct KEK names/paths (or clean up after themselves) --
    the server and its sqlite-backed object store are shared across the
    session, same as the FDB testcontainer is shared across tests/integration/.
    """
    with tempfile.TemporaryDirectory(prefix="pykmip-test-") as tmp:
        tmp_path = Path(tmp)
        cert_path = tmp_path / "server.crt"
        key_path = tmp_path / "server.key"
        _generate_self_signed_cert(cert_path, key_path)

        policy_dir = tmp_path / "policies"
        policy_dir.mkdir()

        port = _free_port()
        config_path = tmp_path / "server.conf"
        config_path.write_text(
            "[server]\n"
            "hostname=127.0.0.1\n"
            f"port={port}\n"
            f"certificate_path={cert_path}\n"
            f"key_path={key_path}\n"
            f"ca_path={cert_path}\n"
            # TLS1.2, not the config's "Basic" default: Basic hardcodes
            # ssl.PROTOCOL_TLSv1, which OpenSSL refuses to negotiate under a
            # modern default security policy.
            "auth_suite=TLS1.2\n"
            f"policy_path={policy_dir}\n"
            "enable_tls_client_auth=False\n"
            "logging_level=WARNING\n"
            f"database_path={tmp_path / 'pykmip.db'}\n"
        )
        log_path = tmp_path / "server.log"

        runner = Path(__file__).parent / "_pykmip_server_runner.py"
        proc = subprocess.Popen([sys.executable, str(runner), str(config_path), str(log_path)])
        try:
            _wait_until_listening("127.0.0.1", port, PYKMIP_READY_TIMEOUT_S)
            yield PyKMIPServerInfo(
                host="127.0.0.1", port=port,
                certificate=cert_path, key=key_path, ca=cert_path,
            )
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
