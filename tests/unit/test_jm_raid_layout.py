"""The JM RAID geometry must be reproduced, never re-planned under a new logic.

The JM's journal storage is a RAID with ``superblock=False`` -- the geometry is
NOT recorded on disk. RAID 0+1 stripes each leg 4 KiB at a time across its
drives; the legacy N-way RAID1 kept a full linear copy per drive. Reading
legacy-written journal bytes back through RAID 0+1 (or vice versa) returns them
scrambled, so the alceml PBA header / journal / distrib superblock fail to
parse.

Production incident 2026-09-08: a cluster whose journals were built N-way was
upgraded to a build that plans RAID 0+1, its JMs were rebuilt RAID 0+1, and
every LVS superblock read back "unsupported version" -> activation failed. No
data was written (a fresh RAID1 create with both legs present neither resyncs
nor writes), so it was recoverable by reproducing the original geometry.
"""
import inspect

from simplyblock_core import jm_raid


class TestPlannerHonoursLayout:
    def test_raid01_is_the_default(self):
        p = jm_raid.plan_topology(["a", "b", "c", "d"])
        assert p["level"] == jm_raid.RAID_0PLUS1
        assert p["legs"] == [["a", "b"], ["c", "d"]]

    def test_legacy_is_one_nway_mirror_over_all_members(self):
        p = jm_raid.plan_topology(["a", "b", "c", "d"],
                                  layout=jm_raid.LAYOUT_LEGACY)
        assert p["level"] == jm_raid.RAID_1_NWAY
        assert p["members"] == ["a", "b", "c", "d"]
        assert "legs" not in p or not p.get("legs")

    def test_legacy_does_not_stripe(self):
        """The whole point: no raid0 leg splitting under the legacy layout."""
        p = jm_raid.plan_topology(["a", "b", "c", "d", "e", "f", "g", "h"],
                                  layout=jm_raid.LAYOUT_LEGACY)
        assert p["members"] == list("abcdefgh")

    def test_single_device_is_no_raid_regardless_of_layout(self):
        for layout in (jm_raid.LAYOUT_LEGACY, jm_raid.LAYOUT_RAID01):
            p = jm_raid.plan_topology(["only"], layout=layout)
            assert p["level"] == jm_raid.RAID_NONE
            assert p["base"] == "only"

    def test_two_devices_raid01_is_a_2way_mirror(self):
        p = jm_raid.plan_topology(["a", "b"])
        assert p["level"] == jm_raid.RAID_0PLUS1
        assert p["legs"] == [["a"], ["b"]]

    def test_two_devices_legacy_is_a_2way_nway_mirror(self):
        p = jm_raid.plan_topology(["a", "b"], layout=jm_raid.LAYOUT_LEGACY)
        assert p["level"] == jm_raid.RAID_1_NWAY
        assert p["members"] == ["a", "b"]

    def test_zero_members_still_raises(self):
        import pytest
        with pytest.raises(ValueError):
            jm_raid.plan_topology([], layout=jm_raid.LAYOUT_LEGACY)

    def test_the_two_layout_constants_are_distinct(self):
        assert jm_raid.LAYOUT_LEGACY != jm_raid.LAYOUT_RAID01
        assert jm_raid.RAID_1_NWAY != jm_raid.RAID_0PLUS1


class TestClusterCarriesTheLayout:
    def test_cluster_has_the_field_defaulting_empty(self):
        from simplyblock_core.models.cluster import Cluster
        c = Cluster()
        assert c.jm_raid_layout == "", "empty = not pinned yet"

    def test_fresh_clusters_are_pinned_raid01(self):
        """create_cluster / _add_cluster_impl set the new-cluster geometry."""
        import inspect as _i
        from simplyblock_core import cluster_ops
        for fn in (cluster_ops.create_cluster, cluster_ops._add_cluster_impl):
            src = _i.getsource(fn)
            assert "jm_raid_layout = jm_raid.LAYOUT_RAID01" in src, fn.__name__


class TestBuildResolvesGeometry:
    def _src(self):
        from simplyblock_core import storage_node_ops
        return inspect.getsource(storage_node_ops._create_jm_stack_on_raid)

    def test_cluster_flag_is_authoritative(self):
        src = self._src()
        i = src.index('layout = (getattr(cluster, "jm_raid_layout"')
        # the per-device fallback only runs when the flag is empty
        j = src.index("if not layout:")
        assert i < j

    def test_a_recorded_raid_without_legs_is_treated_as_legacy(self):
        """The old build path recorded a raid_bdev but never any legs."""
        src = self._src()
        assert "prior_raid and not prior_legs" in src
        assert "jm_raid.LAYOUT_LEGACY" in src

    def test_it_plans_with_the_resolved_layout_not_the_default(self):
        src = self._src()
        assert "plan_topology(jm_nvme_bdevs, layout=layout)" in src

    def test_legacy_level_builds_one_raid1_over_all_members(self):
        src = self._src()
        i = src.index("jm_raid.RAID_1_NWAY")
        window = src[i:i + 400]
        assert 'bdev_raid_create(raid_bdev, plan["members"], "1")' in window

    def test_legacy_keeps_the_record_legacy_shaped(self):
        """leg_bdevs/leg_members must stay empty so the device is not
        mis-detected as raid01 on a later rebuild."""
        src = self._src()
        # in the RAID_1_NWAY branch there is no append to leg_bdevs
        i = src.index("jm_raid.RAID_1_NWAY")
        j = src.index("else:", i)
        assert "leg_bdevs.append" not in src[i:j]


class TestUpgradePinsFromCleanRecords:
    def test_upgrade_pins_before_the_rolling_restart(self):
        from simplyblock_core import cluster_ops
        src = inspect.getsource(cluster_ops.update_cluster)
        i = src.index("run_pre_update(cluster)")
        j = src.index('jm_raid_layout", v')
        assert i < j, "pin must come after the pre-update hook"
        # and before any node restart is issued from this function
        restart_markers = [m for m in ("try_set_node_restarting",
                                       "restart_storage_node",
                                       "_rolling") if m in src]
        for m in restart_markers:
            assert j < src.index(m), f"pin must precede {m}"

    def test_detection_reads_the_jmdevice_leg_records(self):
        from simplyblock_core import cluster_ops
        src = inspect.getsource(cluster_ops.update_cluster)
        assert "jm_leg_bdevs" in src
        assert "jm_raid.LAYOUT_RAID01" in src
        assert "jm_raid.LAYOUT_LEGACY" in src

    def test_only_pins_when_unset(self):
        from simplyblock_core import cluster_ops
        src = inspect.getsource(cluster_ops.update_cluster)
        i = src.index("_detected_layout")
        assert 'jm_raid_layout", "") or "").strip()' in src[max(0, i - 200):i]
