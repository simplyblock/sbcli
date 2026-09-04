"""Shared mock factories for unit tests."""

from unittest.mock import MagicMock


def make_mock_cluster(cluster_id="cluster-1", **attrs):
    """Build a MagicMock Cluster with safe defaults for unit tests.

    ``hashicorp_vault_settings`` is set to ``None`` so callers of
    ``create_kms_connection`` take the LocalKMS branch instead of trying
    to read TLS material from disk via the HCP branch (a MagicMock's
    auto-created attribute would otherwise be truthy).
    """
    cluster = MagicMock()
    cluster.get_id.return_value = cluster_id
    cluster.hashicorp_vault_settings = None
    for name, value in attrs.items():
        setattr(cluster, name, value)
    return cluster


def assert_hublvol_wired(mock_connect, primary, *, role, lvs_node,
                         failover_node=None):
    """Assert a peer was wired to ``primary``'s hublvol once, for ``role``.

    Both recreate paths (``recreate_lvstore`` and
    ``recreate_lvstore_on_non_leader``) call ``connect_to_hublvol`` TWICE per
    peer: once with ``attach_only=True`` before the client port is blocked —
    the controller attach is inert until the in-window connect registers the
    redirect, so hoisting it shrinks the port-block outage span — and once for
    real inside the window. Asserting ``assert_called_once_with`` therefore
    fails on the count, and pinning the exact kwargs breaks whenever the
    in-window call gains one (it now passes ``coordinator_lock``).

    So assert the meaning instead: every call agrees on the role, exactly one
    is the real connect, and that one routes to the right primary / LVS.
    """
    calls = mock_connect.call_args_list
    assert calls, "connect_to_hublvol was never called"

    roles = {c.kwargs.get("role") for c in calls}
    assert roles == {role}, f"expected role {role!r} on every call, got {roles}"

    in_window = [c for c in calls if not c.kwargs.get("attach_only")]
    assert len(in_window) == 1, (
        f"expected exactly one in-window connect, got {len(in_window)} "
        f"(of {len(calls)} calls)")

    call = in_window[0]
    assert call.args[0] is primary
    assert call.kwargs.get("lvs_node") is lvs_node
    assert call.kwargs.get("failover_node") is failover_node


_UNIQUE_IPS = {}


def unique_ip(host_key):
    """Return a distinct, stable IP address for ``host_key``.

    Production treats ``mgmt_ip`` as the physical-host identity: affected
    nodes are deduped by it, failure domains are keyed on it. Two test nodes
    that accidentally share one therefore collapse into a single host and
    silently undercount.

    The idiom this replaces, ``f"10.0.0.{abs(hash(uuid)) % 254 + 1}"``, did
    exactly that. ``hash()`` on ``str`` is salted per process, so the octet
    varies with ``PYTHONHASHSEED`` (which tox randomizes on every run), and
    four nodes over 254 octets collide at ~2.3%.

    Addresses come from ``10.200/16`` so they never alias the ``10.0.0.x``
    literals tests pass explicitly to place two nodes on one host.
    """
    ip = _UNIQUE_IPS.get(host_key)
    if ip is None:
        index = len(_UNIQUE_IPS)
        ip = f"10.200.{index // 254}.{index % 254 + 1}"
        _UNIQUE_IPS[host_key] = ip
    return ip
