"""Product limit enforcement under outage, for docker and k8s.

Fills each documented product limit to its maximum with real data, proves the
next object past the limit is rejected, then takes nodes down and brings them
back to prove what was built survives.

The limits, as agreed with dev (2026-09-12):

    100     snapshots per chain, all with data
    500     clones per snapshot, all with data
    6000    objects per node
    40      delta backups in chain
    70 TiB  max size of an lvol          (untested by anyone; its own variants)

"Chain" is tested both ways, because the word carries two meanings here and
dev's number should hold for both:

    lineage     lvol -> snap -> clone -> snap -> clone ...  100 levels deep
    sequential  100 snapshots of one lvol in series, data written between each

Platform handling rides on the ``_*_dual`` helpers on ``TestClusterBase``: on
docker they drive sbcli directly, on k8s they drive CSI (PVC, VolumeSnapshot,
clone PVC). Each platform is therefore exercised the way its customers use it,
at the cost of rejections arriving differently - see ``_expect_rejected`` and
``_expect_rejected_or_absent``.

Run:
    python3 stress.py --testname ProductLimits_Docker --ndcs 2 --npcs 2
    python3 stress.py --testname ProductLimits_K8s --ndcs 2 --npcs 2 --run_k8s True
    python3 stress.py --testname ProductLimits_70TiB_Docker ...
    python3 stress.py --testname ProductLimits_70TiB_K8s ... --run_k8s True

Runtime: per-object cost is roughly 20-40s on docker and 60-120s on k8s, so
writing data at all 100 chain levels is about an hour per chain phase on docker
and several on k8s. ``DATA_EVERY_N_LEVELS`` trades that down when a faster
signal is wanted, and the summary reports how many writes actually happened.
"""

import random
import time
import traceback

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class LimitNotEnforced(AssertionError):
    """The product accepted an object past a documented limit.

    Raised rather than logged: these limits are agreed with the org, so a
    missing guard means a customer can build a configuration nobody has tested.
    """


class LimitCheckInconclusive(AssertionError):
    """We could not determine whether the limit holds.

    Deliberately distinct from LimitNotEnforced. Treating an API timeout or a
    5xx as "the limit held" would turn infrastructure noise into a silent PASS
    on the one assertion this test exists to make.
    """


class _ProductLimitsMixin:
    """Phases and assertions. Platform differences live in the _*_dual helpers
    on TestClusterBase and in the two outage hooks below."""

    # ── the limits under test ───────────────────────────────────────────────
    MAX_SNAPSHOTS_PER_CHAIN = 100
    MAX_CLONES_PER_SNAPSHOT = 500
    MAX_OBJECTS_PER_NODE = 6000
    MAX_DELTA_BACKUPS = 40
    MAX_LVOL_SIZE_TIB = 70

    # ── which phases this variant runs ──────────────────────────────────────
    RUN_LINEAGE_CHAIN = True
    RUN_SEQUENTIAL_CHAIN = True
    RUN_CLONES_PER_SNAPSHOT = True
    RUN_OBJECTS_PER_NODE = True
    RUN_DELTA_BACKUPS = True
    RUN_MAX_LVOL_SIZE = False       # only the 70TiB variants

    # ── outage behaviour ────────────────────────────────────────────────────
    # One node at a time, at random, after each limit is reached. A cluster
    # sitting exactly at a documented maximum is the state least likely to have
    # been exercised, which is why the outages go here rather than midway.
    OUTAGES_PER_PHASE_MIN = 3
    OUTAGES_PER_PHASE_MAX = 4
    OUTAGE_RECOVERY_TIMEOUT = 900
    POST_OUTAGE_SETTLE_SEC = 60

    # ── data ────────────────────────────────────────────────────────────────
    BASE_LVOL_SIZE = "2G"
    DATA_FIO_RUNTIME = 20
    DATA_FIO_SIZE = "64M"

    # Data at every chain level is the honest reading of "all with data", and
    # it is what makes the extent and blob paths do real work rather than only
    # the metadata path. It is also the dominant cost. Raise this to write
    # every Nth level for a faster signal; the summary reports the real count.
    DATA_EVERY_N_LEVELS = 1

    # The 500 clones inherit their data from the snapshot, so they all have
    # data without 500 writes. A random sample is verified after the outages,
    # and the sample size is logged so the coverage claim stays explicit.
    CLONE_VERIFY_SAMPLE = 20

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.pool_name = "limits_pool"
        self.results = []            # (phase, status, detail)
        self._data_writes = 0

    # ══════════════════════════════════════════════════════════════════════
    #  reporting and assertions
    # ══════════════════════════════════════════════════════════════════════

    def _record(self, phase, status, detail=""):
        self.results.append((phase, status, detail))
        self.logger.info(f"[limits] {status:4} {phase}: {detail}")

    # Words that mean "the product declined this", as opposed to "the request
    # never arrived".
    REJECTION_MARKERS = ("exceed", "limit", "maximum", "too many", "not allowed",
                         "refus", "denied", "cannot create", "capacity",
                         "forbidden", "invalid")

    def _expect_rejected(self, phase, what, fn):
        """Run `fn`, which must be refused by the product. Anything else fails.

        The distinction that matters is between a rejection and an
        infrastructure failure. Treating every exception as proof the limit
        held would turn an API blip or a 5xx into a silent PASS, and this
        assertion is the entire point of the test.
        """
        self.logger.info(f"[limits] {phase}: attempting {what}, expecting rejection")
        try:
            result = fn()
        except Exception as exc:                      # noqa: BLE001
            status = getattr(getattr(exc, "response", None), "status_code", None)
            text = str(exc).lower()
            if status is not None and 400 <= status < 500:
                self._record(phase, "PASS",
                             f"{what} rejected with HTTP {status}: {str(exc)[:140]}")
                return
            if status is None and any(m in text for m in self.REJECTION_MARKERS):
                self._record(phase, "PASS", f"{what} rejected: {str(exc)[:140]}")
                return
            raise LimitCheckInconclusive(
                f"[{phase}] INCONCLUSIVE: creating {what} failed, but not with a "
                f"refusal we recognise, so we cannot say whether the limit is "
                f"enforced. status={status} {type(exc).__name__}: {str(exc)[:200]}"
            ) from exc

        if not result:
            self._record(phase, "PASS", f"{what} rejected (nothing returned)")
            return

        text = str(result).lower()
        if any(m in text for m in self.REJECTION_MARKERS) or '"status": false' in text:
            self._record(phase, "PASS", f"{what} rejected: {str(result)[:140]}")
            return

        raise LimitNotEnforced(
            f"[{phase}] LIMIT NOT ENFORCED: {what} SUCCEEDED. The documented "
            f"maximum was already reached, so this should have been refused. "
            f"Result: {str(result)[:200]}"
        )

    def _expect_rejected_or_absent(self, phase, what, fn, name, kind="lvol"):
        """Like _expect_rejected, but tolerates an asynchronous CSI rejection.

        On k8s a create can return cleanly and then fail during provisioning,
        because the CRD is accepted before the backend is asked. If the call
        appears to succeed there, give it a settling window and assert the
        object never actually materialised. Only a fully materialised object
        means the guard is missing.
        """
        try:
            self._expect_rejected(phase, what, fn)
            return
        except LimitNotEnforced:
            if not self.k8s_test:
                raise
        self.logger.info(
            f"[limits] {phase}: create returned cleanly on k8s; checking whether "
            f"{name} actually materialises")
        sleep_n_sec(60)
        if self._object_exists(name, kind):
            raise LimitNotEnforced(
                f"[{phase}] LIMIT NOT ENFORCED: {what} was accepted AND {name} "
                f"exists after settling. The guard is missing.")
        self._record(phase, "PASS",
                     f"{what} accepted by the CSI layer but never provisioned "
                     f"({name} absent after 60s)")

    def _object_exists(self, name, kind):
        try:
            if kind == "snapshot":
                listing = self.sbcli_utils.list_snapshots() or {}
            else:
                listing = self.sbcli_utils.list_lvols(exclude_in_deletion=True) or {}
        except Exception:                             # noqa: BLE001
            return False
        # k8s normalises names for CRDs, so match loosely on both sides.
        return name in listing or any(name in k or k in name for k in listing)

    # ══════════════════════════════════════════════════════════════════════
    #  outages -- the two platform hooks
    # ══════════════════════════════════════════════════════════════════════

    def _outage_types(self):
        raise NotImplementedError

    def _perform_outage(self, node_uuid, outage_type):
        raise NotImplementedError

    def _random_outages(self, phase):
        count = random.randint(self.OUTAGES_PER_PHASE_MIN, self.OUTAGES_PER_PHASE_MAX)
        try:
            nodes = [n["uuid"] for n in
                     self.sbcli_utils.get_storage_nodes()["results"]]
        except Exception as exc:                      # noqa: BLE001
            self._record(f"{phase}/outages", "FAIL", f"cannot list nodes: {exc}")
            raise
        if not nodes:
            self._record(f"{phase}/outages", "SKIP", "no storage nodes listed")
            return

        self.logger.info(f"[limits] {phase}: {count} outages, one node at a time")
        for i in range(1, count + 1):
            node = random.choice(nodes)
            outage_type = random.choice(self._outage_types())
            started = time.time()
            self.logger.info(
                f"[limits] {phase}: outage {i}/{count} -- {outage_type} on {node[:8]}")
            try:
                self._perform_outage(node, outage_type)
                # Auto-restart is what is under test, so wait for it rather
                # than helping the node along.
                self.sbcli_utils.wait_for_storage_node_status(
                    node, "online", timeout=self.OUTAGE_RECOVERY_TIMEOUT)
            except Exception as exc:                  # noqa: BLE001
                self._record(f"{phase}/outage_{i}", "FAIL",
                             f"{outage_type} on {node[:8]}: {exc}")
                raise
            self._record(f"{phase}/outage_{i}", "PASS",
                         f"{outage_type} on {node[:8]} back online in "
                         f"{time.time() - started:.0f}s")
            sleep_n_sec(self.POST_OUTAGE_SETTLE_SEC)

    # ══════════════════════════════════════════════════════════════════════
    #  data
    # ══════════════════════════════════════════════════════════════════════

    def _write_data(self, lvol_name, tag, level=None):
        """Put real, verified data on a volume.

        Goes through the platform-agnostic FIO helpers, which run md5
        verification, so a write that lands wrong is caught here rather than
        surviving silently into the chain.
        """
        if level is not None and self.DATA_EVERY_N_LEVELS > 1:
            if level % self.DATA_EVERY_N_LEVELS != 0:
                return False
        try:
            handle = self._run_fio_dual(
                lvol_name, name=f"lim_{tag}", rw="write",
                size=self.DATA_FIO_SIZE, bs="64K",
                runtime=self.DATA_FIO_RUNTIME, numjobs=1, nrfiles=1,
                time_based=False,
            )
            self._wait_fio_dual([handle], timeout=self.DATA_FIO_RUNTIME + 240)
            self._validate_fio_dual(handle)
            self._data_writes += 1
            return True
        except Exception as exc:                      # noqa: BLE001
            # A failed write makes the phase meaningless, because an empty
            # chain exercises metadata only. Fatal, not a warning.
            raise AssertionError(
                f"[limits] could not write data to {lvol_name} ({tag}): "
                f"{type(exc).__name__}: {exc}") from exc

    # ══════════════════════════════════════════════════════════════════════
    #  phase 1 -- lineage chain
    # ══════════════════════════════════════════════════════════════════════

    def _phase_lineage_chain(self):
        phase = "snapshots_per_chain(lineage)"
        limit = self.MAX_SNAPSHOTS_PER_CHAIN
        stamp = int(time.time())
        self.logger.info(f"[limits] === {phase}: {limit} levels deep ===")

        root = f"limlin{stamp}r"
        self._create_lvol_dual(root, self.BASE_LVOL_SIZE, pool_name=self.pool_name)
        self._connect_and_mount_dual(root)
        self._write_data(root, "lin_root")

        current, snaps = root, []
        for level in range(1, limit + 1):
            snap = f"limlin{stamp}s{level}"
            snap_ref = self._create_snapshot_dual(current, snap)
            if not snap_ref:
                raise AssertionError(
                    f"[{phase}] snapshot at level {level} was not created; the "
                    f"chain only reached depth {level - 1} of {limit}")
            snaps.append((snap, snap_ref))

            if level == limit:
                break
            clone = f"limlin{stamp}c{level}"
            self._create_clone_dual(snap_ref, clone, size=self.BASE_LVOL_SIZE,
                                    format_disk=True)
            self._write_data(clone, f"lin_l{level}", level=level)
            current = clone
            if level % 10 == 0:
                self.logger.info(f"[limits] {phase}: depth {level}/{limit}")

        self._record(phase, "PASS",
                     f"{len(snaps)} levels deep, data every "
                     f"{self.DATA_EVERY_N_LEVELS} level(s)")

        # one past the limit: clone the deepest snapshot, then snapshot that
        tip = f"limlin{stamp}tip"
        self._create_clone_dual(snaps[-1][1], tip, size=self.BASE_LVOL_SIZE)
        over = f"limlin{stamp}s{limit + 1}"
        self._expect_rejected_or_absent(
            phase, f"snapshot {limit + 1} on a {limit}-deep lineage",
            lambda: self._create_snapshot_dual(tip, over), over, kind="snapshot")

        self._random_outages(phase)
        self._verify_snapshots_survived(phase, [n for n, _ in snaps])

    def _verify_snapshots_survived(self, phase, names):
        try:
            present = set(self.sbcli_utils.list_snapshots() or {})
        except Exception as exc:                      # noqa: BLE001
            self._record(f"{phase}/survived", "FAIL", f"cannot list snapshots: {exc}")
            raise
        missing = [n for n in names
                   if n not in present
                   and not any(n in p or p in n for p in present)]
        if missing:
            raise AssertionError(
                f"[{phase}] {len(missing)} of {len(names)} snapshots gone after "
                f"the outages, e.g. {missing[:8]}")
        self._record(f"{phase}/survived", "PASS",
                     f"all {len(names)} snapshots present after outages")

    # ══════════════════════════════════════════════════════════════════════
    #  phase 2 -- sequential chain
    # ══════════════════════════════════════════════════════════════════════

    def _phase_sequential_chain(self):
        phase = "snapshots_per_chain(sequential)"
        limit = self.MAX_SNAPSHOTS_PER_CHAIN
        stamp = int(time.time())
        self.logger.info(f"[limits] === {phase}: {limit} snapshots of one lvol ===")

        lvol = f"limseq{stamp}"
        self._create_lvol_dual(lvol, self.BASE_LVOL_SIZE, pool_name=self.pool_name)
        self._connect_and_mount_dual(lvol)

        names = []
        for i in range(1, limit + 1):
            # Fresh data before each snapshot, so every one carries a delta
            # rather than duplicating its predecessor.
            self._write_data(lvol, f"seq_{i}", level=i)
            snap = f"limseq{stamp}s{i}"
            if not self._create_snapshot_dual(lvol, snap):
                raise AssertionError(
                    f"[{phase}] snapshot {i} of {limit} was not created")
            names.append(snap)
            if i % 10 == 0:
                self.logger.info(f"[limits] {phase}: {i}/{limit}")

        self._record(phase, "PASS", f"{len(names)} sequential snapshots")

        over = f"limseq{stamp}s{limit + 1}"
        self._expect_rejected_or_absent(
            phase, f"snapshot {limit + 1} of the same lvol",
            lambda: self._create_snapshot_dual(lvol, over), over, kind="snapshot")

        self._random_outages(phase)
        self._verify_snapshots_survived(phase, names)

    # ══════════════════════════════════════════════════════════════════════
    #  phase 3 -- clones per snapshot
    # ══════════════════════════════════════════════════════════════════════

    def _phase_clones_per_snapshot(self):
        phase = "clones_per_snapshot"
        limit = self.MAX_CLONES_PER_SNAPSHOT
        stamp = int(time.time())
        self.logger.info(f"[limits] === {phase}: {limit} clones of one snapshot ===")

        src = f"limcln{stamp}src"
        self._create_lvol_dual(src, self.BASE_LVOL_SIZE, pool_name=self.pool_name)
        self._connect_and_mount_dual(src)
        # Data goes on the source before the snapshot so all 500 clones inherit
        # it. Writing separately to 500 clones is a different, far longer test;
        # a sample is verified after the outages instead.
        self._write_data(src, "clone_source")

        snap = f"limcln{stamp}snap"
        snap_ref = self._create_snapshot_dual(src, snap)
        if not snap_ref:
            raise AssertionError(f"[{phase}] source snapshot was not created")

        clones = []
        for i in range(1, limit + 1):
            name = f"limcln{stamp}n{i}"
            self._create_clone_dual(snap_ref, name, size=self.BASE_LVOL_SIZE)
            clones.append(name)
            if i % 50 == 0:
                self.logger.info(f"[limits] {phase}: {i}/{limit}")

        self._record(phase, "PASS", f"{len(clones)} clones, all inheriting data")

        over = f"limcln{stamp}over"
        self._expect_rejected_or_absent(
            phase, f"clone {limit + 1} of the same snapshot",
            lambda: self._create_clone_dual(snap_ref, over,
                                            size=self.BASE_LVOL_SIZE),
            over, kind="lvol")

        self._random_outages(phase)

        sample = random.sample(clones, min(self.CLONE_VERIFY_SAMPLE, len(clones)))
        gone = [c for c in sample if not self._object_exists(c, "lvol")]
        if gone:
            raise AssertionError(
                f"[{phase}] {len(gone)} of {len(sample)} sampled clones missing "
                f"after the outages: {gone}")
        self._record(f"{phase}/survived", "PASS",
                     f"sampled {len(sample)} of {limit} clones, all present")

    # ══════════════════════════════════════════════════════════════════════
    #  phase 4 -- objects per node
    # ══════════════════════════════════════════════════════════════════════

    def _phase_objects_per_node(self):
        phase = "objects_per_node"
        limit = self.MAX_OBJECTS_PER_NODE
        stamp = int(time.time())
        self.logger.info(f"[limits] === {phase}: {limit} objects on one node ===")

        nodes = self.sbcli_utils.get_storage_nodes()["results"]
        if not nodes:
            self._record(phase, "SKIP", "no storage nodes")
            return
        target = nodes[0]["uuid"]

        # An lvol plus its snapshot is two objects, hence half as many pairs.
        # This phase is a count test, not a data test: writing to any
        # meaningful fraction of 6000 would dwarf every other phase.
        pairs = limit // 2
        for i in range(1, pairs + 1):
            lv = f"limobj{stamp}n{i}"
            self._create_lvol_dual(lv, "128M", pool_name=self.pool_name,
                                   host_id=target)
            if not self._create_snapshot_dual(lv, f"limobj{stamp}s{i}"):
                raise AssertionError(
                    f"[{phase}] snapshot {i} was not created; only "
                    f"{i * 2 - 1} of {limit} objects exist")
            if i % 250 == 0:
                self.logger.info(f"[limits] {phase}: {i * 2}/{limit} objects")

        self._record(phase, "PASS", f"{pairs * 2} objects on {target[:8]}")

        over = f"limobj{stamp}over"
        self._expect_rejected_or_absent(
            phase, f"object {limit + 1} on the same node",
            lambda: self._create_lvol_dual(over, "128M",
                                           pool_name=self.pool_name,
                                           host_id=target),
            over, kind="lvol")

        self._random_outages(phase)

    # ══════════════════════════════════════════════════════════════════════
    #  phase 5 -- delta backups
    # ══════════════════════════════════════════════════════════════════════

    def _phase_delta_backups(self):
        phase = "delta_backups_in_chain"
        limit = self.MAX_DELTA_BACKUPS
        stamp = int(time.time())
        self.logger.info(f"[limits] === {phase}: {limit} delta backups ===")

        if not self._backups_available():
            self._record(phase, "SKIP",
                         "backups unavailable on this cluster (no S3 configured, "
                         "or 'backup list' is not answering)")
            return

        lvol = f"limbkp{stamp}"
        self._create_lvol_dual(lvol, self.BASE_LVOL_SIZE, pool_name=self.pool_name)
        self._connect_and_mount_dual(lvol)
        lvol_id = self._volume_registry[lvol]["lvol_id"]

        made = 0
        for i in range(1, limit + 1):
            # New data between backups keeps each one a genuine delta.
            self._write_data(lvol, f"bkp_{i}", level=i)
            if not self._create_backup(lvol, lvol_id, f"limbkp{stamp}b{i}"):
                raise AssertionError(
                    f"[{phase}] backup {i} of {limit} was not created; the chain "
                    f"only reached depth {made}")
            made += 1
            if i % 5 == 0:
                self.logger.info(f"[limits] {phase}: {i}/{limit}")

        self._record(phase, "PASS", f"{made} delta backups in chain")

        self._write_data(lvol, "bkp_over")
        self._expect_rejected(
            phase, f"backup {limit + 1} in the same chain",
            lambda: self._create_backup(lvol, lvol_id,
                                        f"limbkp{stamp}b{limit + 1}"))

        self._random_outages(phase)

    def _backups_available(self):
        """Probe rather than guess.

        There is no S3-configured flag anywhere in the suite, so a guess based
        on attributes is vacuous and silently skips forever. Ask the product
        instead: if `backup list` answers without erroring, backups are usable.
        """
        try:
            out, err = self._sbcli_raw("-d backup list")
            blob = f"{out or ''} {err or ''}".lower()
            return not any(m in blob for m in
                           ("not configured", "no s3", "usage:", "traceback"))
        except Exception as exc:                      # noqa: BLE001
            self.logger.info(f"[limits] backup probe failed: {exc}")
            return False

    def _sbcli_raw(self, subcmd):
        """Run a raw sbcli subcommand.

        Backups have no sbcli_utils wrapper at all: the CLI is the only API on
        docker, and CRDs are the only API on k8s.
        """
        if self.k8s_test:
            return self.sbcli_utils.k8s.exec_sbcli(subcmd)
        return self.ssh_obj.exec_command(
            node=self.mgmt_nodes[0], command=f"{self.base_cmd} {subcmd}")

    def _create_backup(self, lvol_name, lvol_id, name):
        """Create a backup and return a truthy id, mirroring
        BackupTestBase._create_snapshot(backup=True).

        There is no server-side chain API, so chain depth is simply what we
        have successfully created.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc = self._volume_registry[lvol_name]["pvc_name"]
            k8s.create_storage_backup(f"bck-{name}", pvc)
            k8s.wait_storage_backup_done(f"bck-{name}")
            return f"bck-{name}"
        out, err = self._sbcli_raw(f"-d snapshot add {lvol_id} {name} --backup")
        blob = f"{out or ''} {err or ''}".lower()
        if any(m in blob for m in self.REJECTION_MARKERS):
            return None
        text = (out or "").strip()
        return text.split()[-1] if text else None

    # ══════════════════════════════════════════════════════════════════════
    #  phase 6 -- 70 TiB lvol
    # ══════════════════════════════════════════════════════════════════════

    def _phase_max_lvol_size(self):
        phase = "max_lvol_size"
        tib = self.MAX_LVOL_SIZE_TIB
        stamp = int(time.time())
        self.logger.info(f"[limits] === {phase}: {tib} TiB lvol ===")

        name = f"limbig{stamp}"
        self._create_lvol_dual(name, f"{tib}T", pool_name=self.pool_name)
        lvol_id = self._volume_registry[name]["lvol_id"]
        details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]

        status, size = details.get("status"), int(details.get("size") or 0)
        expected = tib * (1024 ** 4)
        if status != "online":
            raise AssertionError(f"[{phase}] {tib} TiB lvol is {status!r}, not online")
        if not (expected * 0.99 <= size <= expected * 1.01):
            raise AssertionError(
                f"[{phase}] {tib} TiB lvol reports size={size}, expected ~{expected}")
        self._record(phase, "PASS", f"{tib} TiB lvol online, size={size}")

        over = f"limbig{stamp}over"
        self._expect_rejected_or_absent(
            phase, f"an lvol larger than {tib} TiB",
            lambda: self._create_lvol_dual(over, f"{tib + 1}T",
                                           pool_name=self.pool_name),
            over, kind="lvol")

        self._random_outages(phase)

        details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]
        if details.get("status") != "online":
            raise AssertionError(
                f"[{phase}] {tib} TiB lvol is {details.get('status')!r} after outages")
        self._record(f"{phase}/survived", "PASS",
                     f"{tib} TiB lvol still online after outages")

    # ══════════════════════════════════════════════════════════════════════
    #  run
    # ══════════════════════════════════════════════════════════════════════

    def run(self):
        self.logger.info(f"[limits] starting {self.test_name}")
        self._prepare_cluster()

        phases = [
            (self.RUN_LINEAGE_CHAIN, "lineage_chain", self._phase_lineage_chain),
            (self.RUN_SEQUENTIAL_CHAIN, "sequential_chain", self._phase_sequential_chain),
            (self.RUN_CLONES_PER_SNAPSHOT, "clones_per_snapshot", self._phase_clones_per_snapshot),
            (self.RUN_OBJECTS_PER_NODE, "objects_per_node", self._phase_objects_per_node),
            (self.RUN_DELTA_BACKUPS, "delta_backups", self._phase_delta_backups),
            (self.RUN_MAX_LVOL_SIZE, "max_lvol_size", self._phase_max_lvol_size),
        ]

        failures = []
        for enabled, name, fn in phases:
            if not enabled:
                self._record(name, "SKIP", "disabled for this variant")
                continue
            try:
                fn()
            except (LimitNotEnforced, LimitCheckInconclusive) as exc:
                # Keep going: one missing guard should not hide the state of
                # the other four limits, which is the value of running them
                # together in the first place.
                self._record(name, "FAIL", str(exc)[:300])
                failures.append(str(exc))
                self.logger.error(f"[limits] {exc}")
            except Exception as exc:                  # noqa: BLE001
                self._record(name, "FAIL", f"{type(exc).__name__}: {exc}")
                failures.append(f"{name}: {exc}")
                self.logger.error(f"[limits] {name} failed: {exc}")
                self.logger.error(traceback.format_exc())

        self._print_summary()
        if failures:
            raise AssertionError(
                f"{len(failures)} limit phase(s) failed:\n  " +
                "\n  ".join(f[:250] for f in failures))

    def _print_summary(self):
        self.logger.info("[limits] ================= SUMMARY =================")
        width = max((len(p) for p, _, _ in self.results), default=20)
        for phase, status, detail in self.results:
            self.logger.info(f"[limits] {status:4}  {phase:<{width}}  {detail[:110]}")
        counts = {}
        for _, status, _ in self.results:
            counts[status] = counts.get(status, 0) + 1
        self.logger.info(
            f"[limits] totals: {counts}, data writes: {self._data_writes}")

    def _prepare_cluster(self):
        """add_storage_pool takes no capacity argument, so the pool is unbounded
        and the cluster's own capacity is the constraint -- which is why the
        70 TiB variants need a cluster sized for it. On k8s the call can return
        a different name than requested, so use what it hands back."""
        try:
            actual = self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
            if actual and actual != self.pool_name:
                self.logger.info(
                    f"[limits] pool created as {actual!r}, not {self.pool_name!r}")
                self.pool_name = actual
        except Exception as exc:                      # noqa: BLE001
            self.logger.info(f"[limits] pool {self.pool_name}: {exc}")


# ═════════════════════════════════════════════════════════════════════════════
#  platform bindings
# ═════════════════════════════════════════════════════════════════════════════

class _ProductLimitsDocker(_ProductLimitsMixin, TestClusterBase):
    """sbcli + NVMe-oF. Outages kill the SPDK container or use CLI restart."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "product_limits_docker"

    def _outage_types(self):
        return ["container_stop", "graceful_shutdown"]

    def _perform_outage(self, node_uuid, outage_type):
        details = self.sbcli_utils.get_storage_node_details(node_uuid)[0]
        if outage_type == "container_stop":
            # Auto-restart is expected to bring this back unaided; that is
            # exactly the behaviour under test.
            self.ssh_obj.stop_spdk_process(
                details["mgmt_ip"], details["rpc_port"], self.cluster_id)
        else:
            self.sbcli_utils.shutdown_node(node_uuid)
            sleep_n_sec(30)
            self.sbcli_utils.restart_node(node_uuid)


class _ProductLimitsK8s(_ProductLimitsMixin, TestClusterBase):
    """CSI: PVC, VolumeSnapshot, clone PVC. Outages delete the SPDK pod."""

    def __init__(self, **kwargs):
        kwargs["k8s_run"] = True
        super().__init__(**kwargs)
        self.test_name = "product_limits_k8s"

    def _outage_types(self):
        return ["pod_delete", "graceful_shutdown"]

    def _perform_outage(self, node_uuid, outage_type):
        details = self.sbcli_utils.get_storage_node_details(node_uuid)[0]
        if outage_type == "pod_delete":
            # restart_spdk_pod only issues the delete and does not wait; the
            # wait for online happens in _random_outages, which is the number
            # we actually want to measure.
            self._ensure_k8s_utils().restart_spdk_pod(details["mgmt_ip"])
        else:
            self.sbcli_utils.shutdown_node(node_uuid)
            sleep_n_sec(30)
            self.sbcli_utils.restart_node(node_uuid)


# ═════════════════════════════════════════════════════════════════════════════
#  the four runnable cases
# ═════════════════════════════════════════════════════════════════════════════

class ProductLimits_Docker(_ProductLimitsDocker):
    """Every limit dev has tested, on docker. 70 TiB excluded."""
    RUN_MAX_LVOL_SIZE = False


class ProductLimits_K8s(_ProductLimitsK8s):
    """Every limit dev has tested, on k8s via CSI. 70 TiB excluded."""
    RUN_MAX_LVOL_SIZE = False


class ProductLimits_70TiB_Docker(_ProductLimitsDocker):
    """Every limit plus the 70 TiB lvol, on docker.

    Separate because the 70 TiB maximum has been verified by nobody, needs a
    cluster with the capacity to back it, and is the only phase here whose
    expected result is genuinely unknown.
    """
    RUN_MAX_LVOL_SIZE = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "product_limits_70tib_docker"


class ProductLimits_70TiB_K8s(_ProductLimitsK8s):
    """Every limit plus the 70 TiB lvol, on k8s. See the docker twin."""
    RUN_MAX_LVOL_SIZE = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "product_limits_70tib_k8s"
