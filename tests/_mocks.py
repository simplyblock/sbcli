# coding=utf-8
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
