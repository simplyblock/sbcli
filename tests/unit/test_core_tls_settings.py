"""Server-side TLS wiring in simplyblock_core.settings.

The handshake tests run a real uvicorn server on loopback, because the thing
worth checking is not the shape of the keyword arguments but that a client
still gets the certificate it expects and still gets rejected without one.
Each is run twice: once against ``uvicorn_ssl_context_factory`` and once
against the explicit file-path arguments it replaced, so the two are held
equivalent.
"""

import datetime
import http.client
import ipaddress
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from fastapi import FastAPI

from simplyblock_core.settings import Settings


def _issue(common_name, *, issuer=None, san=None, ca=False, purpose=None):
    """Issue a certificate, self-signed unless an ``(name, key)`` issuer is given.

    The extensions below are not decorative: the free-threaded 3.14 image ships
    a stricter OpenSSL that refuses a chain whose certificates carry no key
    identifiers and no key usage.
    """
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    issuer_name, issuer_key = issuer if issuer is not None else (subject, key)
    now = datetime.datetime.now(datetime.UTC)

    key_usage = (
        x509.KeyUsage(
            digital_signature=True, content_commitment=False, key_encipherment=False,
            data_encipherment=False, key_agreement=False, key_cert_sign=True,
            crl_sign=True, encipher_only=False, decipher_only=False)
        if ca else
        x509.KeyUsage(
            digital_signature=True, content_commitment=False, key_encipherment=True,
            data_encipherment=False, key_agreement=False, key_cert_sign=False,
            crl_sign=False, encipher_only=False, decipher_only=False)
    )

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .add_extension(key_usage, critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
            critical=False)
    )
    if purpose is not None:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([purpose]), critical=False)
    if san is not None:
        builder = builder.add_extension(x509.SubjectAlternativeName(san), critical=False)

    return key, builder.sign(issuer_key, hashes.SHA256())


def _write(directory: Path, name: str, key, cert) -> tuple[Path, Path]:
    cert_path = directory / f'{name}.crt'
    key_path = directory / f'{name}.key'
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    return cert_path, key_path


@dataclass
class PKI:
    ca_certificate: Path
    server_certificate: Path
    server_key: Path
    client_certificate: Path
    client_key: Path

    @property
    def settings_kwargs(self) -> dict[str, Path]:
        return {
            'tls_certificate': self.server_certificate,
            'tls_key': self.server_key,
            'tls_certificate_authority': self.ca_certificate,
        }


@pytest.fixture(scope='module')
def pki(tmp_path_factory) -> PKI:
    """A CA, a server certificate valid for 127.0.0.1, and a client certificate."""
    directory = tmp_path_factory.mktemp('pki')

    ca_key, ca_cert = _issue('test-ca', ca=True)
    ca_certificate, _ = _write(directory, 'ca', ca_key, ca_cert)
    issuer = (ca_cert.subject, ca_key)

    server_certificate, server_key = _write(directory, 'server', *_issue(
        '127.0.0.1',
        issuer=issuer,
        san=[x509.IPAddress(ipaddress.ip_address('127.0.0.1'))],
        purpose=ExtendedKeyUsageOID.SERVER_AUTH,
    ))
    client_certificate, client_key = _write(
        directory, 'client',
        *_issue('test-client', issuer=issuer, purpose=ExtendedKeyUsageOID.CLIENT_AUTH))

    return PKI(
        ca_certificate=ca_certificate,
        server_certificate=server_certificate,
        server_key=server_key,
        client_certificate=client_certificate,
        client_key=client_key,
    )


def _legacy_ssl_kwargs(settings: Settings) -> dict:
    """The arguments both entry points passed before ``uvicorn_ssl_context_factory``."""
    return {
        'ssl_certfile': settings.tls_certificate if settings.tls_serve else None,
        'ssl_keyfile': settings.tls_key if settings.tls_serve else None,
        'ssl_ca_certs': (
            str(settings.tls_certificate_authority)
            if settings.tls_client_auth != ssl.CERT_NONE else None
        ),
        'ssl_cert_reqs': settings.tls_client_auth,
    }


def _factory_ssl_kwargs(settings: Settings) -> dict:
    return {'ssl_context_factory': settings.uvicorn_ssl_context_factory()}


SSL_KWARGS_BUILDERS = {
    'ssl_context_factory': _factory_ssl_kwargs,
    'legacy_file_paths': _legacy_ssl_kwargs,
}


@pytest.fixture(params=SSL_KWARGS_BUILDERS, ids=list(SSL_KWARGS_BUILDERS))
def serve(request):
    """Run a uvicorn server configured like the entry points, and yield its port."""
    build_ssl_kwargs = SSL_KWARGS_BUILDERS[request.param]
    running = []

    def start(settings: Settings) -> int:
        app = FastAPI()

        @app.get('/')
        def root() -> dict:
            return {}

        server = uvicorn.Server(uvicorn.Config(
            app=app,
            host='127.0.0.1',
            port=0,
            log_level='warning',
            access_log=False,
            **build_ssl_kwargs(settings),
        ))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        running.append((server, thread))
        deadline = time.monotonic() + 10
        while not server.started:
            if time.monotonic() > deadline:
                raise TimeoutError('uvicorn did not start')
            time.sleep(0.01)

        return server.servers[0].sockets[0].getsockname()[1]

    yield start

    # Joined, not just signalled: a server left running would keep an event
    # loop and its sockets alive in a background thread for the rest of the
    # session, concurrent with every later test.
    for server, thread in running:
        server.should_exit = True
    for server, thread in running:
        thread.join(timeout=10)
        assert not thread.is_alive(), 'uvicorn did not shut down'


#: How a refused handshake surfaces. Which one it is depends on how far the
#: peer got before giving up -- under TLS 1.3 the server's rejection of a
#: missing client certificate arrives after the client considers the handshake
#: done, so the failure can surface on the write or the read. None of these
#: is raised by a connection that was never accepted.
HANDSHAKE_REJECTED = (
    ssl.SSLError,
    ConnectionResetError,
    BrokenPipeError,
    http.client.RemoteDisconnected,
)


def _client_context(pki: PKI, *, certificate: bool) -> ssl.SSLContext:
    context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH, cafile=str(pki.ca_certificate))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    if certificate:
        context.load_cert_chain(pki.client_certificate, pki.client_key)
    return context


def _get(port: int, context: ssl.SSLContext | None = None) -> int:
    connection = (
        http.client.HTTPSConnection('127.0.0.1', port, context=context, timeout=10)
        if context is not None
        else http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    )
    try:
        connection.request('GET', '/')
        return connection.getresponse().status
    finally:
        connection.close()


class TestUvicornSslContextFactory:
    def test_none_without_tls(self):
        assert Settings(tls_serve=False).uvicorn_ssl_context_factory() is None

    def test_factory_returns_the_settings_context(self, pki):
        settings = Settings(
            tls_serve=True, tls_client_auth='required', **pki.settings_kwargs)

        context = settings.uvicorn_ssl_context_factory()(None, None)

        assert isinstance(context, ssl.SSLContext)
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.get_ca_certs() != []

    def test_client_auth_disabled_loads_no_ca(self, pki):
        settings = Settings(
            tls_serve=True, tls_client_auth='disabled', **pki.settings_kwargs)

        context = settings.uvicorn_ssl_context_factory()(None, None)

        assert context.verify_mode == ssl.CERT_NONE
        assert context.get_ca_certs() == []


class TestServing:
    def test_plain_http_without_tls_serve(self, serve, pki):
        port = serve(Settings(tls_serve=False, **pki.settings_kwargs))

        assert _get(port) == 200

    def test_serves_the_configured_certificate(self, serve, pki):
        port = serve(Settings(
            tls_serve=True, tls_client_auth='disabled', **pki.settings_kwargs))

        with socket.create_connection(('127.0.0.1', port), timeout=10) as raw:
            with _client_context(pki, certificate=False).wrap_socket(
                    raw, server_hostname='127.0.0.1') as tls:
                served = tls.getpeercert()

        assert dict(served['subject'][0])['commonName'] == '127.0.0.1'
        assert _get(port, _client_context(pki, certificate=False)) == 200

    def test_plaintext_rejected_when_serving_tls(self, serve, pki):
        port = serve(Settings(
            tls_serve=True, tls_client_auth='disabled', **pki.settings_kwargs))

        with pytest.raises(HANDSHAKE_REJECTED):
            _get(port)

    def test_client_certificate_accepted_when_required(self, serve, pki):
        port = serve(Settings(
            tls_serve=True, tls_client_auth='required', **pki.settings_kwargs))

        assert _get(port, _client_context(pki, certificate=True)) == 200

    def test_client_without_certificate_rejected_when_required(self, serve, pki):
        port = serve(Settings(
            tls_serve=True, tls_client_auth='required', **pki.settings_kwargs))

        with pytest.raises(HANDSHAKE_REJECTED):
            _get(port, _client_context(pki, certificate=False))

    def test_client_certificate_ignored_when_disabled(self, serve, pki):
        port = serve(Settings(
            tls_serve=True, tls_client_auth='disabled', **pki.settings_kwargs))

        assert _get(port, _client_context(pki, certificate=True)) == 200
