"""KMIPClient against Cosmian KMS: validates Encrypt/Decrypt the way a real
appliance implements them, not PyKMIP's own reference server.

``test_kmip_client.py`` proves the protocol composition works against a real
KMIP wire implementation, but it is still PyKMIP's own reference server --
not representative of how a production-grade appliance behaves (see the CBC
note in ``simplyblock_core/kms/_kmip.py`` for a gap found exactly that way).
Cosmian KMS (https://github.com/Cosmian/kms) is a full KMIP 1.0-2.1
implementation with confirmed Encrypt/Decrypt support for symmetric keys --
the closest thing to the eventual production target this suite can run
without committing to a specific vendor.

Opt-in via environment variables, not an auto-provisioned testcontainer: the
KMIP socket listener needs its own TLS + client-certificate configuration in
``kms.toml`` (``[socket_server]``, port 5696 by default -- see
https://docs.cosmian.com/key_management_system/configuration/server_configuration_file.html),
which has to be set up once for whatever certificates the test run uses. That
is a one-time setup step for whoever runs this locally or wires it into CI,
not something to guess at and bake into a fixture.

To run:
    docker run -p 5696:5696 -v $(pwd)/cosmian-config:/config \\
        ghcr.io/cosmian/kms:latest --config-file /config/kms.toml
    export COSMIAN_KMIP_HOST=127.0.0.1
    export COSMIAN_KMIP_PORT=5696
    export COSMIAN_KMIP_CLIENT_CERT=/path/to/client.crt
    export COSMIAN_KMIP_CLIENT_KEY=/path/to/client.key
    export COSMIAN_KMIP_CA=/path/to/ca.crt
    tox run -e integration -- tests/integration/kms/test_cosmian_kms_client.py
"""
import os
import uuid

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.kms._kmip import KMIPClient

_ENV_VARS = (
    "COSMIAN_KMIP_HOST", "COSMIAN_KMIP_PORT",
    "COSMIAN_KMIP_CLIENT_CERT", "COSMIAN_KMIP_CLIENT_KEY", "COSMIAN_KMIP_CA",
)


@pytest.fixture()
def cosmian_client():
    missing = [name for name in _ENV_VARS if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Cosmian KMS not configured: set {', '.join(missing)} "
                     "to run this test (see module docstring)")
    if DBController().kv_store is None:
        pytest.skip("FoundationDB is not available")

    server_url = f"{os.environ['COSMIAN_KMIP_HOST']}:{os.environ['COSMIAN_KMIP_PORT']}"
    client = KMIPClient(
        server_url,
        os.environ["COSMIAN_KMIP_CA"],
        os.environ["COSMIAN_KMIP_CLIENT_CERT"],
        os.environ["COSMIAN_KMIP_CLIENT_KEY"],
    )
    with client:
        yield client


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def test_round_trip_against_cosmian(cosmian_client):
    kek_name = _unique("kek")
    path = _unique("cluster/test/lvol")
    cosmian_client.create_key_encryption_key(kek_name)
    try:
        cosmian_client.create_data_encryption_keys(path, kek_name)
        key1, key2 = cosmian_client.get_data_encryption_keys(path, kek_name)
        assert key1.get_secret_value() != key2.get_secret_value()
    finally:
        cosmian_client.delete_data_encryption_keys(path)
        cosmian_client.delete_key_encryption_key(kek_name)


def test_rekey_against_cosmian(cosmian_client):
    src_kek, dst_kek = _unique("kek-src"), _unique("kek-dst")
    src_path, dst_path = _unique("cluster/test/lvol"), _unique("cluster/test/backup")
    cosmian_client.create_key_encryption_key(src_kek)
    cosmian_client.create_key_encryption_key(dst_kek)
    try:
        cosmian_client.create_data_encryption_keys(src_path, src_kek)
        original = cosmian_client.get_data_encryption_keys(src_path, src_kek)

        cosmian_client.rekey_data_encryption_keys(src_path, src_kek, dst_path, dst_kek)

        assert cosmian_client.get_data_encryption_keys(dst_path, dst_kek) == original
    finally:
        cosmian_client.delete_data_encryption_keys(src_path)
        cosmian_client.delete_data_encryption_keys(dst_path)
        cosmian_client.delete_key_encryption_key(src_kek)
        cosmian_client.delete_key_encryption_key(dst_kek)
