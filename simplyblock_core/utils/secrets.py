from typing import Any, Optional, Union

from pydantic import SecretBytes, SecretStr


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


def unwrap_secret(value: Union[SecretStr, str, None]) -> Optional[str]:
    """Tolerant scalar unwrap for transitional call sites that still expect ``str``.

    Removed once the surrounding code is type-correct on ``SecretStr``.
    """
    if value is None:
        return None
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    return value
