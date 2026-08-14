# coding=utf-8
"""Tier-local pytest config for the edge-clusters e2e campaign.

These tests drive real AWS instances: a single case can span a fio run, an
instance reboot and a full node rebuild. The repo-wide per-test budget
(``timeout = 30`` in pyproject.toml) is sized for unit/integration tests and
would kill every case here at 30s, so the tier sets its own budget the same
way the migration tier does — centrally, so new cases inherit it.

Individual cases that need more (the two-node double-reboot, the soak-style
connection-fault case) carry their own ``@pytest.mark.timeout``.
"""
import pathlib

import pytest

_TIER_DIR = str(pathlib.Path(__file__).parent)

#: Generous: the longest ordinary case is a two-node reboot cycle (fio 1500s
#: runtime + reboot + rebuild + fail-back wait). 3h leaves head-room for a
#: slow region without letting a genuinely wedged case hang a whole campaign.
EDGE_E2E_DEFAULT_TIMEOUT = 3 * 60 * 60


def pytest_collection_modifyitems(items):
    for item in items:
        if str(item.fspath).startswith(_TIER_DIR):
            item.add_marker(pytest.mark.edge_e2e)
            if item.get_closest_marker("timeout") is None:
                # method="thread": the campaign driver runs on Windows, where
                # pytest-timeout's default signal method dies collecting with
                # "module 'signal' has no attribute 'SIGALRM'" — it aborted
                # every test stage of the first run that reached them
                # (2026-08-14) before a single test executed.
                item.add_marker(pytest.mark.timeout(EDGE_E2E_DEFAULT_TIMEOUT,
                                                    method="thread"))


def pytest_report_header(config):
    return ("edge_e2e: campaign tier — requires a provisioned environment "
            "(edge_e2e/provision.py + deploy.py); see edge_e2e/README.md")
