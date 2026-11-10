"""Typed, validated KMS configuration for a cluster.

A leaf module: pure pydantic, no dependency on ``models.cluster`` or anything
else in this package, so ``models/cluster.py`` can import :data:`KMSSettings`
without a cycle (``kms/__init__.py`` otherwise reaches back to
``models.cluster`` through ``LocalKMS``/``create_kms_connection`` — see the
``TYPE_CHECKING`` guards there and in ``_fdb.py``).

``Cluster.kms_settings`` stores the ``dict`` form of whichever of these
``model_dump()`` produces; :data:`KMSSettings` is a real pydantic discriminated
union over backends; ``provider`` is the discriminator, so a malformed config
fails with pydantic's own clear discriminator error rather than a hand-rolled
"exactly one of" check.
"""
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic.networks import AnyUrl, UrlConstraints


class VaultKMSSettings(BaseModel):
    """HashiCorp Vault (or an OpenBao-compatible server): a Transit mount
    wraps/unwraps DEKs, a KV mount stores the wrapped ciphertext."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["vault"] = "vault"
    base_url: Annotated[AnyUrl, UrlConstraints(allowed_schemes=["https"])]
    transit_mount: str = "simplyblock/transit"
    kv_mount: str = "simplyblock/kv"
    cert_role: str = "simplyblock-webappapi"


class KMIPKMSSettings(BaseModel):
    """A KMIP-speaking key management appliance."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["kmip"] = "kmip"
    #: ``host:port``. KMIP has no URL scheme convention to validate against
    #: (unlike Vault's HTTPS API), so this is kept as the plain address the
    #: wire protocol itself uses.
    server_url: Annotated[str, Field(min_length=1)]


KMSSettings = Annotated[
    Union[VaultKMSSettings, KMIPKMSSettings],
    Field(discriminator="provider"),
]
