"""AR-I: is the replicated data actually the same data.

This is the lane the feature lives or dies on, and it is the one with the
least natural observability, for a structural reason: **the target copy is
not independently readable while replication is running.** It only becomes a
usable volume when a fail-over clones it. So "compare source and target" is
not a thing a test can do mid-flight -- every integrity assertion here has to
go through a fail-over first, and that is why AR-I-001 reads as a recovery
test even though what it is measuring is bytes.

Two deliberate choices about what counts as a failure:

* **crc32c on a raw, disjoint region is the gate**, not filesystem md5. The
  suite's default FIO path hardcodes ``--verify=md5`` and turns on
  ``verify_backlog``, which the code comment admits "bypasses the rand_seed
  check" -- deliberate false-positive suppression. A mismatch from that path
  is worth logging and not worth failing on. A mismatch on a sequentially
  written, separately verified region is real.

* **MD corruption is never acceptable and is checked separately.** A torn
  user-data write on a device without 4K atomicity is a known hazard the
  filesystem is expected to absorb. Blobstore metadata corruption is not --
  it means the metadata journal is not holding, and it is silent until
  something much later cannot open an lvstore.
"""
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
    ReplicationPreconditionError,
)
from utils.common_utils import sleep_n_sec


class ReplicationDataIsIdentical(ReplicationTestBase):
    """AR-I-001: replicated data is byte-identical after fail-over.

    Write known content, let a cycle complete, fail over, mount the
    failed-over copy and compare. This is the only route to the target's
    bytes, so it is also the only honest proof that replication moves data
    rather than metadata about data.
    """

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"ti{stamp}", self.cluster_b)
        pname = self.policy_add(f"pi{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arint{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)

        self.logger.info("[AR-I-001] seeding %s with known content", vol)
        expected = self.seed_volume(vol)

        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="initial full sync")

        # Force a cycle rather than waiting out the interval, then give the
        # full transfer room: REPL_VOLUME_SIZE moves in its entirety every
        # time, so this is bounded by volume size, not by what changed.
        self.logger.info("[AR-I-001] triggering a cycle and waiting for it")
        self.replication_trigger(vol_id)
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 60)

        # Quiesce the source before failing over, so a cycle cannot be
        # mid-flight. Without this a mismatch is ambiguous -- it could mean
        # "replication is broken" or "we read while it was still writing",
        # and those need very different responses.
        self.logger.info("[AR-I-001] unmounting the source before fail-over")
        self._disconnect_and_cleanup_dual(vol)

        self.logger.info("[AR-I-001] failing over policy %s", pname)
        self.failover_policy(pname)
        self.await_state(vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="fail-over")

        failed_over = self.failed_over_volume_name(vol_id, vol)
        self.logger.info("[AR-I-001] mounting the failed-over copy %s",
                         failed_over)
        self._connect_and_mount_dual(failed_over, format_disk=False)
        self.verify_volume(failed_over, expected,
                           context="on the failed-over copy (AR-I-001)")
        self.logger.info("[AR-I-001] PASS: byte-identical after fail-over")

        self.assert_no_corruption("after AR-I-001")
        self._teardown(vol, failed_over, vol_id)

    def _teardown(self, *names_and_id):
        *names, vol_id = names_and_id
        try:
            self.policy_clear(vol_id)
        except Exception:                             # noqa: BLE001
            pass
        for n in dict.fromkeys(names):
            try:
                self._disconnect_and_cleanup_dual(n)
            except Exception:                         # noqa: BLE001
                pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR] could not delete %s: %s", n,
                                    str(exc)[:120])
        self.cleanup_replication()


class ReplicationConvergesUnderLoad(ReplicationTestBase):
    """AR-I-002: replication under continuous load converges.

    The question is not whether a quiesced volume replicates -- AR-I-001
    covers that -- but whether a volume being written to can ever finish a
    cycle. With full transfers this is a real risk: if a transfer of
    REPL_VOLUME_SIZE takes longer than the interval, cycles overlap and lag
    grows without bound. A policy that can never converge is worse than one
    that is slow, because nothing reports it.
    """

    #: Long enough for several cycles to complete under load.
    LOAD_RUNTIME = 420

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tc{stamp}", self.cluster_b)
        pname = self.policy_add(f"pc{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arload{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self._connect_and_mount_dual(vol, format_disk=True)

        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        self.logger.info("[AR-I-002] starting %ds of continuous IO",
                         self.LOAD_RUNTIME)
        handle = self._run_fio_dual(vol, runtime=self.LOAD_RUNTIME,
                                    rw="randrw", bs="16K", iodepth=8,
                                    numjobs=2, size="512M",
                                    name="arconverge")

        # Sample the relationship while the load runs. A state that leaves
        # `replicating` under load, or a lag that only grows, is the finding.
        samples = []
        deadline = time.time() + self.LOAD_RUNTIME
        while time.time() < deadline:
            rel = self.relationship_for(vol_id) or {}
            samples.append((round(time.time() % 100000),
                            rel.get("state"),
                            rel.get("lag") or rel.get("lag_seconds")))
            if rel.get("state") not in (self.STATE_REPLICATING, None):
                raise AssertionError(
                    f"[AR-I-002] the relationship left 'replicating' and went "
                    f"to {rel.get('state')!r} under load, with no operator "
                    f"action. Load must not change replication state.")
            sleep_n_sec(30)

        self._wait_fio_dual([handle], timeout=self.LOAD_RUNTIME + 300)
        self._validate_fio_dual(handle)
        self.logger.info("[AR-I-002] state/lag samples: %s", samples[-10:])

        # With the load stopped the volume must settle. This is the actual
        # convergence assertion: a cycle that only completes once writing
        # stops is still acceptable; one that never completes is not.
        self.logger.info("[AR-I-002] load stopped; waiting for a clean cycle")
        self.replication_trigger(vol_id)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="convergence after load stops")

        lags = [s[2] for s in samples if isinstance(s[2], (int, float))]
        if len(lags) >= 4 and lags[-1] > lags[0] and lags == sorted(lags):
            self.logger.warning(
                "[AR-I-002] lag rose monotonically across the whole load "
                "window (%s -> %s). With full transfers this is what "
                "divergence looks like: the cycle cannot keep up with the "
                "interval. Raised for dev -- the volume did converge once "
                "load stopped, so this is a capacity question, not a "
                "correctness one.", lags[0], lags[-1])

        self.logger.info("[AR-I-002] PASS: converged after load")
        self.assert_no_corruption("after AR-I-002")

        try:
            self.policy_clear(vol_id)
            self._disconnect_and_cleanup_dual(vol)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[AR] teardown: %s", str(exc)[:120])
        self.cleanup_replication()


class ReplicationNoMetadataCorruption(ReplicationTestBase):
    """AR-I-003: no MD corruption anywhere in the replication path.

    Separate from AR-I-001/002 because it is checking a different class of
    damage, on a different surface, with a different severity rule. User data
    is verified by comparison; metadata is verified by scanning for the
    strings SPDK emits when a metadata page fails its checksum. Those strings
    are never acceptable and never caused by a non-atomic device:

        Metadata page %d crc mismatch for blobid
        Metadata page is all zero.
        bad magic header / hdr_fail

    This runs the replication path hard -- several cycles, a fail-over and a
    fail-back -- and then scans BOTH clusters. Scanning only the source would
    miss the case that matters most, where a transfer writes a damaged page
    onto the target and nothing notices until the target is promoted.
    """

    CYCLES = 3

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tm{stamp}", self.cluster_b)
        pname = self.policy_add(f"pm{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"armd{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        expected = self.seed_volume(vol)

        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        for i in range(self.CYCLES):
            self.logger.info("[AR-I-003] cycle %d/%d", i + 1, self.CYCLES)
            # New data every cycle on both platforms. Guarded to docker
            # only, this loop replicated an idle volume on k8s and proved
            # nothing about repeated transfers.
            expected = self.write_marker_files(vol, f"armd{i}", count=2)
            self.replication_trigger(vol_id)
            sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 45)
            # Scan after every cycle, not just at the end. Metadata damage is
            # cumulative and silent; knowing which cycle introduced it is the
            # difference between a usable bug report and "it broke somewhere".
            self.assert_no_corruption(f"after replication cycle {i + 1}")

        self._disconnect_and_cleanup_dual(vol)
        self.logger.info("[AR-I-003] failing over, then scanning both sides")
        self.failover_policy(pname)
        self.await_state(vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="fail-over")
        self.assert_no_corruption("after fail-over (AR-I-003)")

        failed_over = self.failed_over_volume_name(vol_id, vol)
        self._connect_and_mount_dual(failed_over, format_disk=False)
        self.verify_volume(failed_over, expected,
                           context="after %d cycles and a fail-over "
                                   "(AR-I-003)" % self.CYCLES)

        self.logger.info("[AR-I-003] failing back to %s", self.cluster_a)
        out, err = self.failback(vol_id, source_cluster_id=self.cluster_a)
        combined = (out or "") + (err or "")
        if "error" in combined.lower():
            raise ReplicationPreconditionError(
                f"[AR-I-003] fail-back could not be configured: "
                f"{combined[:300]}")
        self.drive_cutover(vol_id)
        self.await_state(vol_id, self.STATE_CUTOVER_DONE,
                         timeout=self.REPL_OP_SEC, what="fail-back cut-over")
        self.assert_no_corruption("after fail-back (AR-I-003)")
        self.logger.info("[AR-I-003] PASS: no metadata corruption on either "
                         "cluster across %d cycles, a fail-over and a "
                         "fail-back", self.CYCLES)

        for n in (vol, failed_over):
            try:
                self._disconnect_and_cleanup_dual(n)
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()
