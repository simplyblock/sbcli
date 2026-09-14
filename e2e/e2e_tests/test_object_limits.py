"""TC-LIMITS-001 — Hard object limits: enforcement on the control plane and
no performance degradation on SPDK at the limits.

Limits under test (simplyblock_core/constants.py):
  * MAX_LVOL_SIZE            = 70 TiB   volume create / resize / clone --resize
  * MAX_SNAPSHOTS_PER_LVOL   = 100      active snapshots of one volume (one chain)
  * MAX_CLONES_PER_SNAPSHOT  = 500      active clones of one snapshot

Flow
  1. pool + one volume, connect, mount, fio baseline (json output -> IOPS / clat).
  2. take MAX_SNAPSHOTS_PER_LVOL snapshots of it; the next one must be
     rejected with "Snapshot limit reached".
  3. fio on the volume again: it now sits on top of a 100-deep blob chain and
     reads of untouched clusters walk that chain. IOPS / latency must stay
     within tolerance of the baseline.
  4. create MAX_CLONES_PER_SNAPSHOT clones from the newest snapshot; the next
     one must be rejected with "Clone limit reached". fio on one clone (reads
     resolve through the snapshot) and once more on the source volume.
  5. size cap: a create and a resize one TiB ABOVE the limit must both be
     rejected with "exceeds the maximum". The oversize value is derived from
     constants.MAX_LVOL_SIZE rather than written out: it was hardcoded to
     51TiB against a 50 TiB limit, and when the limit moved to 70 TiB that
     turned the whole step into an assertion that a perfectly legal volume
     is refused -- a test that fails on correct behaviour.

Tolerance: after-vs-baseline IOPS >= (1 - perf_tolerance) * baseline and mean
completion latency <= (1 + perf_tolerance) * baseline, default 30%. The
counts and tolerance are overridable through kwargs for shorter smoke runs
(snapshot_count / clone_count / perf_tolerance), the defaults ARE the limits.
"""
import json
import time

import requests

from simplyblock_core import constants

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger

FIO_RUNTIME = 120
FIO_JSON_DIR = "/tmp/fio_object_limits"


class TestObjectLimits(TestClusterBase):
    """Enforce the size / snapshot / clone limits and prove SPDK holds up at them."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "object_limits"
        self.logger = setup_logger(__name__)
        self.snapshot_count = int(kwargs.get("snapshot_count", 100))
        self.clone_count = int(kwargs.get("clone_count", 500))
        self.perf_tolerance = float(kwargs.get("perf_tolerance", 0.30))
        # One TiB past whatever the product limit currently is. Derived, not
        # written out: see the module docstring.
        self.oversize = kwargs.get(
            "oversize", f"{constants.MAX_LVOL_SIZE // (1024 ** 4) + 1}TiB")
        self.created_clones = []
        self.created_snapshots = []

    # ------------------------------------------------------------------ fio

    def _fio_node(self, lvol_name):
        reg = self._volume_registry.get(lvol_name, {})
        node = reg.get("node")
        if node:
            return node
        cm = self.client_machines
        return cm[0] if isinstance(cm, (list, tuple)) else cm

    def _fio_json(self, lvol_name, mount, tag):
        """Run fio (randrw 4K, time based) with json output and return
        (read_iops, write_iops, read_clat_mean_us, write_clat_mean_us).

        On k8s the base fio helper runs fio as a Job from a ConfigMap and does
        not forward output_format/output_file, so the perf comparison is a
        docker-mode check; on k8s the limits are still enforced and verified,
        the perf assertions are skipped (returns None)."""
        if self.k8s_test:
            self.logger.warning(f"[fio:{tag}] perf comparison skipped on k8s (no json output path)")
            return None
        node = self._fio_node(lvol_name)
        out = f"{FIO_JSON_DIR}/{lvol_name}_{tag}.json"
        self.ssh_obj.exec_command(node, f"mkdir -p {FIO_JSON_DIR} && rm -f {out}")
        handle = self._run_fio_dual(
            lvol_name, mount_path=mount, name=f"lim_{tag}", runtime=FIO_RUNTIME,
            rw="randrw", size="1G", bs="4K", iodepth=8, numjobs=2, nrfiles=4,
            output_format="json", output_file=out)
        self._wait_fio_dual([handle], timeout=FIO_RUNTIME + 600)
        raw = self.ssh_obj.read_file(node, out)
        data = json.loads(raw[raw.index("{"):])
        job = data["jobs"][0]
        r, w = job["read"], job["write"]
        res = (float(r["iops"]), float(w["iops"]),
               float(r.get("clat_ns", {}).get("mean", 0)) / 1000.0,
               float(w.get("clat_ns", {}).get("mean", 0)) / 1000.0)
        self.logger.info(f"[fio:{tag}] {lvol_name}: read {res[0]:.0f} IOPS / {res[2]:.0f} us, "
                         f"write {res[1]:.0f} IOPS / {res[3]:.0f} us")
        return res

    def _assert_no_degradation(self, label, base, after):
        if base is None or after is None:
            self.logger.warning(f"[{label}] perf comparison skipped")
            return
        b_r, b_w, b_rl, b_wl = base
        a_r, a_w, a_rl, a_wl = after
        tol = self.perf_tolerance
        problems = []
        if b_r and a_r < (1 - tol) * b_r:
            problems.append(f"read IOPS {a_r:.0f} < {(1-tol):.0%} of baseline {b_r:.0f}")
        if b_w and a_w < (1 - tol) * b_w:
            problems.append(f"write IOPS {a_w:.0f} < {(1-tol):.0%} of baseline {b_w:.0f}")
        if b_rl and a_rl > (1 + tol) * b_rl:
            problems.append(f"read clat {a_rl:.0f}us > {(1+tol):.0%} of baseline {b_rl:.0f}us")
        if b_wl and a_wl > (1 + tol) * b_wl:
            problems.append(f"write clat {a_wl:.0f}us > {(1+tol):.0%} of baseline {b_wl:.0f}us")
        assert not problems, f"[{label}] performance degraded beyond {tol:.0%}: " + "; ".join(problems)
        self.logger.info(f"[{label}] within {tol:.0%} of baseline")

    # ------------------------------------------------------------ rejection

    @staticmethod
    def _expect_rejected(what, fn, needle):
        try:
            fn()
        except requests.exceptions.HTTPError as e:
            assert needle in str(e), f"{what}: rejected, but not by the limit: {e}"
            return str(e)
        raise AssertionError(f"{what}: was accepted, expected rejection containing {needle!r}")

    # ------------------------------------------------------------------ run

    def run(self):
        self.logger.info("=== TC-LIMITS-001: object limits (enforcement + SPDK performance) ===")
        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol = f"{self.lvol_name}_lim"
        self.sbcli_utils.add_lvol(lvol_name=lvol, pool_name=self.pool_name, size="20G")
        lvol_id = self.sbcli_utils.get_lvol_id(lvol)
        assert lvol_id, "volume not created"
        _device, mount = self._connect_and_mount_dual(lvol, format_disk=True)

        # 1. baseline
        base = self._fio_json(lvol, mount, "baseline")

        # 2. snapshots up to the limit, then one more
        t0 = time.time()
        for i in range(self.snapshot_count):
            name = f"{lvol}_s{i:03d}"
            self.sbcli_utils.add_snapshot(lvol_id=lvol_id, snapshot_name=name, retry=3)
            self.created_snapshots.append(name)
        self.logger.info(f"created {self.snapshot_count} snapshots in {time.time()-t0:.0f}s")
        if self.snapshot_count >= 100:
            msg = self._expect_rejected(
                "snapshot #101",
                lambda: self.sbcli_utils.add_snapshot(lvol_id=lvol_id, snapshot_name=f"{lvol}_s_over", retry=1),
                "Snapshot limit reached")
            self.logger.info(f"snapshot cap enforced: {msg}")

        # 3. fio on the volume sitting on the deep chain
        after_chain = self._fio_json(lvol, mount, "after_snapshots")
        self._assert_no_degradation("100-deep snapshot chain", base, after_chain)

        # 4. clones up to the limit, then one more
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name=self.created_snapshots[-1])
        assert snap_id, "newest snapshot id not found"
        t0 = time.time()
        for i in range(self.clone_count):
            name = f"{lvol}_c{i:03d}"
            self.sbcli_utils.add_clone(snapshot_id=snap_id, clone_name=name, retry=3)
            self.created_clones.append(name)
            if (i + 1) % 50 == 0:
                self.logger.info(f"  {i+1}/{self.clone_count} clones ({time.time()-t0:.0f}s)")
        self.logger.info(f"created {self.clone_count} clones in {time.time()-t0:.0f}s")
        if self.clone_count >= 500:
            msg = self._expect_rejected(
                "clone #501",
                lambda: self.sbcli_utils.add_clone(snapshot_id=snap_id, clone_name=f"{lvol}_c_over", retry=1),
                "Clone limit reached")
            self.logger.info(f"clone cap enforced: {msg}")

        sample = self.created_clones[-1]
        _cdev, cmount = self._connect_and_mount_dual(sample, format_disk=False)
        clone_perf = self._fio_json(sample, cmount, "clone_sample")
        self._assert_no_degradation("clone reading through snapshot with 500 siblings", base, clone_perf)
        after_clones = self._fio_json(lvol, mount, "after_clones")
        self._assert_no_degradation("source volume with 500 clones", base, after_clones)

        # 5. size cap
        self._expect_rejected(
            f"create {self.oversize}",
            lambda: self.sbcli_utils.add_lvol(lvol_name=f"{lvol}_big", pool_name=self.pool_name,
                                              size=self.oversize, retry=1),
            "exceeds the maximum")
        self._expect_rejected(
            f"resize to {self.oversize}",
            lambda: self.sbcli_utils.resize_lvol(lvol_id, self.oversize),
            "exceeds the maximum")
        self.logger.info("size cap enforced on create and resize")

        self.logger.info("TC-LIMITS-001 PASSED")

    # -------------------------------------------------------------- cleanup

    def teardown(self, *a, **kw):
        try:
            for name in (self.created_clones[-1:] if self.created_clones else []):
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning(f"cleanup clone mount {name}: {e}")
            for name in self.created_clones:
                try:
                    self.sbcli_utils.delete_lvol(name, skip_error=True)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning(f"delete clone {name}: {e}")
            try:
                self.delete_snapshots()
            except Exception as e:  # noqa: BLE001
                self.logger.warning(f"delete snapshots: {e}")
        finally:
            super().teardown(*a, **kw)
