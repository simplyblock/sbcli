"""A non-leader peer must never be left with a single hublvol path.

The tertiary's second path is its redirect to the secondary, and it is the only
thing that keeps a redirect alive when the PRIMARY dies: NVMe multipath fails
the controller over to it. With one path, losing the primary destroys the
controller outright -- the hub bdev is removed and reopening returns ENODEV --
and the peer, with nothing to redirect through, promotes itself on the next
write (``spdk_lvs_trigger_leadership_switch``). Two leaders, writer conflict,
and the conflict handler then blocks the peer's own client port.

Both 2026-09 incidents reduce to this one missing path:

  * AWS 2026-09-04 17:11:23 (fc32e143, LVS_10). The tertiary's only hublvol
    path died with the container-killed primary and never reconnected
    ("spdk_nvme_ctrlr_reconnect_poll_async: *ERROR*"). 35 s later the restart
    moved leadership; four nodes that were never in the outage self-aborted and
    the cluster suspended.
  * k8s 2026-09-05 08:57 (1fb83b67, LVS_13). "Receive remove event from
    callback", then "hub bdev LVS_13/hublvoln1 cannot be opened, error=-19" on
    BOTH survivors. Both promoted, both blocked their ports, the client lost
    every path and fio took EIO. No restart and no control-plane action was
    involved at all.

The path is established by a deferred, post-unblock, best-effort pass in the
restart flow. Nothing re-checked it afterwards, so a skipped or failed
deferral -- or a later disconnect from a network hiccup -- left the peer
single-pathed indefinitely.
"""
import inspect

from simplyblock_core.controllers import health_controller


def _check_src():
    return inspect.getsource(health_controller._check_sec_node_hublvol)


class TestFailoverPathRepairIsReachable:
    def test_it_does_not_require_auto_fix_alone(self):
        """auto_fix is only escalated to once the coarse existence check has
        FAILED -- and a controller holding 1 of 2 paths PASSES that check. So
        gated on auto_fix this branch is unreachable in the very state it
        exists to repair."""
        src = _check_src()
        assert "elif passed and is_sec2 and (auto_fix or repair_paths)" in src

    def test_repair_paths_is_a_separate_cadence(self):
        sig = inspect.signature(health_controller._check_sec_node_hublvol)
        assert "repair_paths" in sig.parameters
        assert "auto_fix" in sig.parameters

    def test_it_triggers_when_fewer_than_two_paths(self):
        src = _check_src()
        assert "len(ctrlrs) < 2" in src

    def test_it_adds_the_failover_path(self):
        src = _check_src()
        assert "add_hublvol_failover_path(" in src

    def test_it_stands_down_while_a_restart_owns_the_lvs(self):
        """The restart flow is the exclusive author of hublvol attaches during
        its phases; a concurrent repair produced the attach-during-destroy race
        before."""
        src = _check_src()
        i = src.index("len(ctrlrs) < 2")
        assert "_restart_owns_lvs(primary_node)" in src[i:i + 200]


class TestOrderingWithinTheCheck:
    def test_full_reconnect_is_still_tried_first_when_the_controller_is_gone(self):
        """Missing controller entirely is a different repair from a missing
        second path, and must still come first."""
        src = _check_src()
        assert src.index("if not passed and auto_fix") < src.index(
            "elif passed and is_sec2 and (auto_fix or repair_paths)")


class TestRepairIsNotGatedOnPrimaryHealth:
    """The tertiary's second path targets the SECONDARY, not the primary.

    Requiring primary_node.lvstore_status == "ready" closed this repair at
    exactly the moment the path becomes load-bearing: the primary dying is
    what makes the tertiary depend on its redirect to the secondary.
    """

    def test_the_lvstore_ready_gate_is_gone_from_this_branch(self):
        src = _check_src()
        i = src.index("elif passed and is_sec2 and (auto_fix or repair_paths)")
        window = src[i:i + 900]
        assert 'primary_node.lvstore_status == "ready":' not in window

    def test_the_failover_target_status_is_still_checked(self):
        """Relaxing the primary gate must not mean attaching to a dead sec1."""
        src = _check_src()
        i = src.index("elif passed and is_sec2 and (auto_fix or repair_paths)")
        window = src[i:i + 5200]
        assert "sec1.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]" in window


class TestStaleFenceRemediation:
    def test_monitor_has_a_remediation_arm(self):
        from simplyblock_core.services import storage_node_monitor
        assert hasattr(storage_node_monitor, "_remediate_stale_port_blocks")

    def test_it_is_gated_on_hublvol_health(self):
        """Unblocking a peer with no redirect reopens the loop: it promotes on
        the next write and fences itself again, with client IO let back in."""
        import inspect as _i
        from simplyblock_core.services import storage_node_monitor
        src = _i.getsource(storage_node_monitor._remediate_stale_port_blocks)
        assert "_check_sec_node_hublvol(" in src
        assert "_check_node_hublvol(" in src
        assert "NOT unblocking" in src

    def test_it_stands_down_for_restart_owned_lvs(self):
        import inspect as _i
        from simplyblock_core.services import storage_node_monitor
        src = _i.getsource(storage_node_monitor._remediate_stale_port_blocks)
        assert "_restart_owns_lvs(owner)" in src

    def test_it_requires_the_node_to_be_online(self):
        import inspect as _i
        from simplyblock_core.services import storage_node_monitor
        src = _i.getsource(storage_node_monitor._remediate_stale_port_blocks)
        assert "snode.status != StorageNode.STATUS_ONLINE" in src

    def test_the_threshold_is_under_the_client_ctrl_loss_tmo(self):
        """ctrl_loss_tmo is 30 x 2s = 60s; after that the kernel deletes the
        controller and the namespace fails IO instead of requeuing."""
        from simplyblock_core.services import storage_node_monitor
        assert 0 < storage_node_monitor.STALE_PORT_BLOCK_SEC < 60
