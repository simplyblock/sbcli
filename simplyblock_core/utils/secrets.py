from typing import Any

from pydantic import SecretBytes, SecretStr

MASK = "**********"

# RPC param names that carry secrets as plain ``str`` rather than ``SecretStr``
# (the v1 API hands controllers raw JSON, so the type system can't catch these).
# A ``SecretStr``/``SecretBytes`` value masks itself via its own ``repr``, so
# only the plain-``str`` case needs name-based coverage here.
_SENSITIVE_RPC_PARAM_NAMES = {
    "key", "key2", "psk", "dhchap_key", "dhchap_ctrlr_key", "secret_access_key",
}


def redact_rpc_params(params: Any) -> Any:
    """Return a copy of an RPC ``params`` mapping with known secret-bearing
    values masked by parameter name, for safe inclusion in a log message.

    Leaves ``SecretStr``/``SecretBytes`` values untouched -- their own
    ``repr`` already masks them.
    """
    if not isinstance(params, dict):
        return params
    return {
        key: MASK if key in _SENSITIVE_RPC_PARAM_NAMES and not isinstance(value, (SecretStr, SecretBytes))
        else value
        for key, value in params.items()
    }


def unwrap_secrets_for_send(obj: Any) -> Any:
    """Return a copy of ``obj`` with every ``SecretStr``/``SecretBytes`` replaced
    by its plaintext value.

    Used at the wire-send site of clients (just before ``requests.post(json=...)``)
    so the dict carrying the wrapper can be logged safely one line earlier — the
    wrapper's repr masks the value, while this function produces a plain
    JSON-serializable structure for the HTTP body.
    """
    if isinstance(obj, (SecretStr, SecretBytes)):
        return obj.get_secret_value()
    if isinstance(obj, dict):
        return {k: unwrap_secrets_for_send(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [unwrap_secrets_for_send(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(unwrap_secrets_for_send(v) for v in obj)
    return obj


def unwrap_secret(value: SecretStr | str | None) -> str | None:
    """Tolerant scalar unwrap for transitional call sites that still expect ``str``.

    Removed once the surrounding code is type-correct on ``SecretStr``.
    """
    if value is None:
        return None
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    return value
