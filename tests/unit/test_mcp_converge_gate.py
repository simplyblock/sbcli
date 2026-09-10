"""Regression tests for the MCP converge gate that holds SPDK start until the
CPU-topology reboot has actually landed on the node.

The gate previously read `status.configuration.name` as the pool's target.
MCO only advances that field once the WHOLE pool has finished rolling, so for
the duration of a rollout it names the PREVIOUS render — a node that had
already converged on the NEW render compared unequal and waited for every other
node in the pool, blowing the 240x10s budget on pools of any size.
"""
from types import SimpleNamespace

import pytest

from simplyblock_web.api.internal.storage_node.kubernetes import (
    _cfg_hash,
    _mcp_target_config,
    _node_carries_config,
)

OLD = "rendered-storage-298ce7-319694d68de5accd302208a85f98850d"
NEW = "rendered-storage-298ce7-52452853afdfea89ad50739be7063d93"


def _node(current, state="Done", unschedulable=False):
    return SimpleNamespace(
        metadata=SimpleNamespace(annotations={
            "machineconfiguration.openshift.io/currentConfig": current,
            "machineconfiguration.openshift.io/state": state,
        }),
        spec=SimpleNamespace(unschedulable=unschedulable),
    )


def _mcp(spec_config, status_config):
    return {
        "spec": {"configuration": {"name": spec_config}},
        "status": {"configuration": {"name": status_config}},
    }


class TestMcpTargetConfig:
    def test_reads_spec_not_status(self):
        """The regression: mid-rollout the two fields diverge and spec is right."""
        mcp = _mcp(spec_config=NEW, status_config=OLD)
        assert _mcp_target_config(mcp) == NEW

    def test_settled_pool_has_both_equal(self):
        assert _mcp_target_config(_mcp(NEW, NEW)) == NEW

    @pytest.mark.parametrize("mcp", [
        {},
        {"spec": {}},
        {"spec": {"configuration": {}}},
        {"spec": {"configuration": None}},
        {"spec": None},
    ])
    def test_missing_fields_yield_none(self, mcp):
        assert _mcp_target_config(mcp) is None


class TestNodeCarriesConfig:
    def test_converged_node_passes_during_pool_rollout(self):
        """worker-4's exact state: Done on the NEW render while the pool still rolls."""
        mcp = _mcp(spec_config=NEW, status_config=OLD)
        assert _node_carries_config(_node(NEW), _mcp_target_config(mcp)) is True

    def test_node_still_on_old_render_waits(self):
        mcp = _mcp(spec_config=NEW, status_config=OLD)
        assert _node_carries_config(_node(OLD), _mcp_target_config(mcp)) is False

    def test_stale_status_target_would_have_inverted_the_check(self):
        """Guards the bug directly: the old code compared against status."""
        mcp = _mcp(spec_config=NEW, status_config=OLD)
        stale_target = mcp["status"]["configuration"]["name"]
        # A converged node fails against the stale target ...
        assert _node_carries_config(_node(NEW), stale_target) is False
        # ... and an un-converged one passes. Exactly backwards.
        assert _node_carries_config(_node(OLD), stale_target) is True

    def test_working_node_waits(self):
        assert _node_carries_config(_node(NEW, state="Working"), NEW) is False

    def test_cordoned_node_waits(self):
        assert _node_carries_config(_node(NEW, unschedulable=True), NEW) is False

    def test_matches_on_hash_across_pool_rename(self):
        """Same content under a different pool name must pass without a reboot."""
        other_pool = "rendered-storage-abcdef-52452853afdfea89ad50739be7063d93"
        assert _node_carries_config(_node(other_pool), NEW) is True

    def test_no_target_waits(self):
        assert _node_carries_config(_node(NEW), None) is False

    def test_no_current_config_waits(self):
        assert _node_carries_config(_node(None), NEW) is False


class TestCfgHash:
    def test_extracts_trailing_hash(self):
        assert _cfg_hash(NEW) == "52452853afdfea89ad50739be7063d93"

    def test_none_passthrough(self):
        assert _cfg_hash(None) is None
