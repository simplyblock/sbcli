"""Restart must not re-derive the node's core-role allocation.

alceml_cpu_cores/distrib_cpu_cores/poller_cpu_cores and every derived mask
are l-core INDICES (0..req_cpu_count-1), decided once at add time by
whichever allocation policy was in effect then (calculate_core_allocations,
via add_node/recalculate_cores_distribution's first-ever call). Restart
used to call recalculate_cores_distribution again on every restart where
the OS-reported core count still matched -- which re-ran that policy from
scratch, so upgrading the node agent to a build with a changed allocation
policy (e.g. distrib/poller now taking priority over alceml) silently
re-pinned an already-provisioned node's roles the next time it merely
restarted, not as a deliberate re-provisioning action.

What legitimately can go stale across a restart is which *physical* core
sits at each index -- the OS/k8s CPU manager can hand back a different
specific set (same count) than before. That's all restart may still
refresh: the index@physical_core pairing in l_cores, via
reassign_l_cores_for_restart() -- which keeps every role's index set (its
size, and any sharing with another role at the same index) exactly as
decided at add time, only choosing which fresh physical core fills each
index, preferring to keep distrib/poller/alceml's own cores mutual
hyperthread siblings -- and generate_l_cores(), nothing else.
"""
import inspect

from simplyblock_core import storage_node_ops, utils


class TestRestartDoesNotRederiveCoreRoles:

    def test_restart_never_calls_recalculate_cores_distribution(self):
        """add_node is the one legitimate place this runs -- restart must
        not call it again. Checks for an actual call, not just the name --
        the explanatory comment above the fix mentions it by name too."""
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "recalculate_cores_distribution(" not in src

    def test_restart_still_refreshes_l_cores_from_the_fresh_core_list(self):
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "read_allowed_list" in src
        assert "snode.l_cores = utils.generate_l_cores(" in src

    def test_restart_places_physical_cores_via_the_sibling_aware_helper(self):
        """Not a plain sort -- distrib/poller/alceml's saved index sets are
        handed to reassign_l_cores_for_restart so it can prefer keeping
        each role's own cores real hyperthread siblings."""
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "utils.reassign_l_cores_for_restart(" in src

    def test_restart_skips_reassignment_on_an_unchanged_cpuset(self):
        """A no-op restart (identical cpuset) must not churn which physical
        core each role lands on for no operational reason."""
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "prior_physical_cores" in src

    def test_restart_warns_rather_than_silently_stales_on_a_core_count_mismatch(self):
        """A genuine mismatch (host lost cores) must be visible, not
        swallowed -- a stale l_cores left in place is exactly the kind of
        thing that should show up in the logs, not just happen quietly."""
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "leaving l_cores as-is" in src


class TestGenerateLCores:
    """The shared helper restart/add_node/sn configure all key off, so a
    fix to one path can't drift from the others the way number_of_distribs
    already had across generate_configs/regenerate_config/calculate_hp_only."""

    def test_pairs_index_with_physical_core_in_order(self):
        assert utils.generate_l_cores([5, 9, 14]) == "0@5,1@9,2@14"

    def test_empty_list_is_empty_string(self):
        assert utils.generate_l_cores([]) == ""

    def test_used_by_every_l_cores_call_site(self):
        """Regression guard for the drift class of bug this refactor keeps
        running into: the inline '{i}@{core}' idiom must appear exactly
        once in the whole module -- inside generate_l_cores itself -- not
        reintroduced ad hoc at some other call site."""
        src = inspect.getsource(utils)
        assert src.count('{i}@{core}"') == 1, (
            "the l-cores pairing idiom should live in exactly one place "
            "(generate_l_cores); found it duplicated again")
