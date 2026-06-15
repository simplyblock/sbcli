from typing import Annotated, Any

from pydantic import AfterValidator, BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, EnvSettingsSource, SettingsConfigDict


def _parse_str_list(v: Any) -> list[str]:
    if isinstance(v, str):
        return [item.strip() for item in v.split(',') if item.strip()]
    return v


def _check_api_versions(v: set[int]) -> set[int]:
    if unknown := v - {1, 2}:
        raise ValueError(
            f"SB_API_VERSIONS contains unknown versions: {sorted(unknown)}. Valid versions are: 1, 2."
        )
    return v


def _parse_int_list(v: Any) -> list[int]:
    """Parse a comma-separated string or a bare integer into a list of ints.

    Handles the case where ``_CommaSupportedEnvSource`` already applied
    ``json.loads`` and returned a Python int (e.g. ``SB_API_VERSIONS=2``
    becomes the integer ``2`` after JSON parsing).
    """
    if isinstance(v, str):
        return [int(item.strip()) for item in v.split(',') if item.strip()]
    if isinstance(v, int):
        return [v]
    return v


class _CommaSupportedEnvSource(EnvSettingsSource):
    """Env source that falls back to the raw string for list fields when the
    value is not valid JSON, allowing comma-separated env-var values."""

    def decode_complex_value(self, field_name: str, field: Any, value: Any) -> Any:
        try:
            return super().decode_complex_value(field_name, field, value)
        except Exception:
            return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    api_versions: Annotated[
        set[int],
        Field(
            description=(
                "API versions to enable. "
                "Comma-separated list of version numbers (e.g. '1,2'). "
                "Valid versions are 1 and 2. Defaults to all versions."
            )
        ),
        BeforeValidator(_parse_int_list),
        AfterValidator(_check_api_versions),
    ] = {1, 2}
    enable_cluster_secret_auth: bool = True
    k8s_admin_service_accounts: Annotated[
        list[str],
        Field(
            description=(
                "Kubernetes service accounts granted full admin access to the API. "
                "Comma-separated list of fully-qualified names "
                "(e.g. 'system:serviceaccount:default:my-operator'). "
            )
        ),
        BeforeValidator(_parse_str_list),
    ] = []

    @model_validator(mode="after")
    def validate_cluster_secret_auth(self) -> "Settings":
        if not self.enable_cluster_secret_auth and 1 in self.api_versions:
            raise ValueError(
                "SB_ENABLE_CLUSTER_SECRET_AUTH=false requires API v1 to be disabled."
            )
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        return (
            init_settings,
            _CommaSupportedEnvSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )
