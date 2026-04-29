import ssl
from pathlib import Path
from typing import Annotated, Any, Literal, Optional

from pydantic import BeforeValidator, Field, PlainSerializer, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_VERIFY_MODE_TO_STR = {
    ssl.CERT_NONE: "disabled",
    ssl.CERT_OPTIONAL: "optional",
    ssl.CERT_REQUIRED: "required",
}
_STR_TO_VERIFY_MODE = {v: k for k, v in _VERIFY_MODE_TO_STR.items()}


def _parse_verify_mode(x: Any) -> ssl.VerifyMode:
    if isinstance(x, str):
        if x not in _STR_TO_VERIFY_MODE:
            raise ValueError("Invalid input")
        return _STR_TO_VERIFY_MODE[x]

    return x


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    tls_serve: Annotated[
        bool,
        Field(
            description="Run servers in TLS mode. Requires certificate and key to be present."
        ),
    ] = False
    tls_client_auth: Annotated[
        ssl.VerifyMode,
        Field(
            description="Client authentication mode. May be 'disabled', 'optional', or 'required'"
        ),
        BeforeValidator(_parse_verify_mode),
        PlainSerializer(_VERIFY_MODE_TO_STR.__getitem__, return_type=str),
    ] = ssl.CERT_NONE
    tls_connect: Annotated[
        Literal["disabled", "anonymous", "authenticated"],
        Field(description="Connect to internal services via TLS."),
    ] = "disabled"
    tls_provider: Annotated[
        Optional[Literal["openshift", "cert-manager"]],
        Field(description="Provider for TLS certificates in the cluster."),
    ] = None
    tls_certificate: Path = Path("/etc/simplyblock/tls/tls.crt")
    tls_key: Path = Path("/etc/simplyblock/tls/tls.key")
    tls_certificate_authority: Path = Path("/etc/simplyblock/tls/ca.crt")

    @model_validator(mode="after")
    def validate_tls_files(self):
        if not self.tls_serve and self.tls_connect == "disabled":
            return self

        if self.tls_serve and (
            missing := [
                name
                for name in ["tls_certificate", "tls_key"]
                if not getattr(self, name).is_file()
            ]
        ):
            raise ValueError(
                "SB_TLS_SERVE=true requires TLS files to exist: " + ", ".join(missing)
            )

        if (
            self.tls_connect != "disabled"
            and not self.tls_certificate_authority.is_file()
        ):
            raise ValueError(
                "SB_TLS_CONNECT != 'disabled' requires certificate authority to exist"
            )

        return self

    @model_validator(mode="after")
    def validate_tls_provider(self):
        if self.tls_connect != "disabled" and self.tls_provider is None:
            raise ValueError(
                "TLS provider needs to be configured for TLS connections to be used"
            )
        return self

    def make_server_ssl_context(self):
        """Return an SSLContext requiring client certificates, or None if TLS is not configured."""
        if self.tls_client_auth == "disabled":
            return None

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.tls_certificate, self.tls_key)
        ctx.verify_mode = self.tls_client_auth
        ctx.load_verify_locations(self.tls_certificate_authority)
        return ctx
