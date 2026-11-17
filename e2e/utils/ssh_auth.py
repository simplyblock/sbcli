"""The one SSH credential chain, for code that does not go through SshUtils.

Most of the suite connects through utils.ssh_utils, which runs this order
itself. The standalone scripts -- logs/cleanup.py, the two MinIO uploaders and
utils/get_lba_diff_report.py -- open their own paramiko connections, and each
used to load exactly one key and fail if it was refused.

The order matches what the pipelines build into SB_SSH, and it exists because
the lab is mid-migration:

    1. KEY_PATH / KEY_NAME      the CI key the run installed from a secret
    2. simplyblock-us-east-2.pem  still authorised on some nodes, for now
    3. ~/.ssh/id_ed25519, id_rsa  a developer running this by hand
    4. SSH_PASSWORD             last, because it is the one being withdrawn

Only after all of those are refused is a connection a failure.
"""
from __future__ import annotations

import os
from pathlib import Path

import paramiko


def key_paths() -> list[str]:
    """Candidate private keys, best first, existing files only."""
    out: list[str] = []
    explicit = os.environ.get("KEY_PATH")
    if explicit and os.path.isfile(explicit):
        out.append(explicit)

    home = os.path.join(Path.home(), ".ssh")
    name = os.environ.get("KEY_NAME")
    if name:
        out.append(os.path.join(home, name))
    out.append(os.path.join(home, "simplyblock-us-east-2.pem"))
    out.append(os.path.join(home, "id_ed25519"))
    out.append(os.path.join(home, "id_rsa"))

    seen, uniq = set(), []
    for p in out:
        if p in seen or not os.path.isfile(p):
            continue
        seen.add(p)
        uniq.append(p)
    return uniq


def load_keys() -> list[paramiko.PKey]:
    """Every candidate key that parses, in order.

    A key that does not parse is skipped rather than raised on: the next one
    may well work, and a passphrase-protected id_rsa sitting in a developer's
    ~/.ssh should not stop the CI key being tried.
    """
    keys: list[paramiko.PKey] = []
    for p in key_paths():
        for loader in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
            try:
                keys.append(loader.from_private_key_file(p))
                break
            except Exception:
                continue
    return keys


def password() -> str | None:
    return os.environ.get("SSH_PASSWORD") or None


def connect(client: paramiko.SSHClient, host: str, username: str,
            sock=None, timeout: int = 30, port: int = 22) -> str:
    """Connect *client* to *host*, trying every credential in turn.

    Returns a short description of what worked, so a caller can log that a
    connection only succeeded on the password -- a run that passes on the
    credential being withdrawn should say so rather than look healthy.

    Raises the last error when nothing authenticates, with every candidate
    named, because "authentication failed" on its own does not say whether the
    key was missing, refused, or never tried.
    """
    attempted: list[str] = []
    last: Exception | None = None

    for key in load_keys():
        try:
            client.connect(hostname=host, port=port, username=username,
                           pkey=key, sock=sock, timeout=timeout,
                           allow_agent=False, look_for_keys=False)
            return f"key {key.get_fingerprint().hex()[:16]}"
        except Exception as exc:                          # noqa: BLE001
            attempted.append(f"key {key.get_fingerprint().hex()[:16]}")
            last = exc

    pw = password()
    if pw:
        try:
            client.connect(hostname=host, port=port, username=username,
                           password=pw, sock=sock, timeout=timeout,
                           allow_agent=False, look_for_keys=False)
            return "password"
        except Exception as exc:                          # noqa: BLE001
            attempted.append("password")
            last = exc

    tried = ", ".join(attempted) if attempted else "nothing (no key, no SSH_PASSWORD)"
    raise RuntimeError(
        f"could not authenticate to {username}@{host}: tried {tried}. "
        f"Candidates were {key_paths() or '[none on disk]'}. "
        f"Last error: {last!r}")
