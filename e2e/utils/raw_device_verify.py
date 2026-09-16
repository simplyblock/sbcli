"""Raw-device data integrity verification, for non-NVMe (lblk) clusters.

Why this exists rather than the suite's usual md5 bracket
---------------------------------------------------------
NVMe guarantees a 4K atomic write. A generic Linux block device may guarantee
512b, or nothing. Simplyblock's answer to that is a metadata journal with
per-entry CRCs (torn-write *detection*, not atomicity) -- but the journal
protects **metadata only**. User data is not journalled, so user-data integrity
on these devices is exactly what needs testing and exactly what nothing tests.

The suite's normal path cannot be the gate here. ``SshUtils.run_fio_test``
hardcodes ``--verify=md5`` and auto-enables ``verify_backlog=4096`` for mixed
workloads -- deliberate false-positive suppression, per the comment at its call
site. Worse, it writes through a filesystem, and a filesystem journal can mask
or transform a torn device write. Databases, which are the actual use case for
these devices, do raw IO and their own journalling.

So this module verifies the raw block device, using the shape the feature team
themselves used in ``scripts/single_node_partition_soak.py``:

    stamp  : sequential crc32c-verified write over a fixed region
    churn  : random mixed IO over a DISJOINT region
    verify : replay the stamp job with --verify_only

Three properties make this free of the false positives md5-over-a-filesystem
produces, without needing ``serialize_overlap``:

* **crc32c on the raw device** -- no filesystem in the way.
* **Sequential, non-overlapping writes** -- fio cannot tear its own data, so a
  mismatch is the storage layer's, not the test's.
* **``--verify_only`` replays the identical job** -- deterministic. A plain
  ``--rw=read`` would not reproduce the pattern layout and would verify nothing,
  which is a genuinely easy mistake to make here.

A mismatch reported by this module is a real data-integrity defect. That is the
whole point of it being separate from the md5 lanes, which stay warning-level.
"""

import re

# Shared by the stamp and verify passes: fio must replay the *identical* job for
# --verify_only to mean anything.
FIO_COMMON = ("--direct=1 --ioengine=libaio --group_reporting "
              "--time_based=0 --randrepeat=0 --thread")

DEFAULT_VERIFY_REGION = "4G"
DEFAULT_CHURN_OFFSET = "5G"
DEFAULT_CHURN_SIZE = "4G"
DEFAULT_CHURN_RUNTIME = 120

#: fio prints these when verification fails. Distinct from the md5 markers in
#: CommonUtils: anything here is a hard failure, never a warning.
CORRUPTION_MARKERS = (
    "verify failed",
    "verify: bad",
    "bad magic header",
    "checksum",
    "crc32c: verify failed",
    "hdr_fail",
)


def _verify_job(name, region_size, bs="256k", iodepth=8, offset=None):
    job = (f"--name={name} {FIO_COMMON} --rw=write --bs={bs} "
           f"--iodepth={iodepth} --size={region_size} "
           f"--verify=crc32c --verify_state_save=0")
    if offset:
        job += f" --offset={offset}"
    return job


class RawDeviceVerifier:
    """Stamp / churn / verify a raw block device over an SSH connection.

    Deliberately not a TestClusterBase mixin: the k8s and docker paths reach
    their client nodes differently, and both already hold an SshUtils.
    """

    def __init__(self, ssh_obj, logger):
        self.ssh_obj = ssh_obj
        self.logger = logger

    # ── internals ────────────────────────────────────────────────────────
    def _run(self, node, cmd, timeout=3600):
        out, err = self.ssh_obj.exec_command(node, cmd, timeout=timeout,
                                             max_retries=1)
        return (out or ""), (err or "")

    def _assert_device(self, node, device):
        if not self.ssh_obj.is_block_device(node, device):
            raise RuntimeError(
                f"[raw-verify] {device} is not a block device on {node}; "
                "refusing to run -- writing to a regular file here would "
                "silently verify nothing about the storage layer")

    @staticmethod
    def _find_corruption(text):
        low = (text or "").lower()
        return [m for m in CORRUPTION_MARKERS if m in low]

    # ── phases ───────────────────────────────────────────────────────────
    def stamp(self, node, device, region_size=DEFAULT_VERIFY_REGION,
              name="stamp", bs="256k"):
        """Lay down the crc32c-verified region. Writes only, no verify pass."""
        self._assert_device(node, device)
        cmd = (f"sudo fio {_verify_job(name, region_size, bs=bs)} "
               f"--filename={device} --do_verify=0")
        self.logger.info("[raw-verify] stamping %s on %s (%s)",
                         device, node, region_size)
        out, err = self._run(node, cmd)
        hits = self._find_corruption(out + err)
        if hits:
            # A write pass should never report verification problems.
            raise RuntimeError(
                f"[raw-verify] stamp pass on {device} reported {hits}; "
                f"the device was already returning bad data before any "
                f"verification ran")
        return out

    def churn(self, node, device, offset=DEFAULT_CHURN_OFFSET,
              size=DEFAULT_CHURN_SIZE, runtime=DEFAULT_CHURN_RUNTIME,
              bs="16k", iodepth=16, rwmixread=70):
        """Random mixed IO over a region DISJOINT from the stamped one.

        Disjoint on purpose: overlapping the stamped region would let the churn
        legitimately rewrite verified blocks, and every subsequent mismatch
        would be the test's own doing.
        """
        self._assert_device(node, device)
        cmd = (f"sudo fio --name=churn {FIO_COMMON} --filename={device} "
               f"--rw=randrw --rwmixread={rwmixread} --bs={bs} "
               f"--iodepth={iodepth} --offset={offset} --size={size} "
               f"--time_based=1 --runtime={runtime}")
        self.logger.info("[raw-verify] churning %s on %s (offset=%s size=%s "
                         "runtime=%ss)", device, node, offset, size, runtime)
        out, err = self._run(node, cmd, timeout=runtime + 600)
        return out + err

    def verify(self, node, device, region_size=DEFAULT_VERIFY_REGION,
               name="stamp", bs="256k", context=""):
        """Replay the stamp job with --verify_only. Raises on any mismatch.

        This is the gate. Everything else in the lblk integrity lanes is
        supporting evidence.
        """
        self._assert_device(node, device)
        cmd = (f"sudo fio {_verify_job(name, region_size, bs=bs)} "
               f"--filename={device} --verify_only --verify_fatal=1")
        where = f" ({context})" if context else ""
        self.logger.info("[raw-verify] verifying %s on %s%s",
                         device, node, where)
        out, err = self._run(node, cmd)
        blob = out + err

        hits = self._find_corruption(blob)
        if hits:
            raise RuntimeError(
                f"[raw-verify] DATA CORRUPTION on {device} at {node}{where}: "
                f"markers {hits}. This is a raw-device crc32c mismatch with no "
                f"filesystem involved and no overlapping IO, so it is a real "
                f"integrity defect.\n{self._excerpt(blob)}")

        # fio can fail without printing a verify marker (IO error, device gone).
        if re.search(r"\berr=\s*[1-9]", blob) or "fio: io_u error" in blob:
            raise RuntimeError(
                f"[raw-verify] IO error verifying {device} at {node}{where}\n"
                f"{self._excerpt(blob)}")

        self.logger.info("[raw-verify] %s verified clean%s", device, where)
        return blob

    # ── convenience ──────────────────────────────────────────────────────
    def stamp_churn_verify(self, node, device, churn_runtime=DEFAULT_CHURN_RUNTIME,
                           region_size=DEFAULT_VERIFY_REGION, context=""):
        """The full cycle, for a steady-state integrity check."""
        self.stamp(node, device, region_size=region_size)
        self.churn(node, device, runtime=churn_runtime)
        return self.verify(node, device, region_size=region_size,
                           context=context or "steady state")

    @staticmethod
    def _excerpt(blob, lines=25):
        """The tail of the fio output, for the failure message."""
        rows = [ln for ln in (blob or "").splitlines() if ln.strip()]
        return "\n".join(rows[-lines:])
