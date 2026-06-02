"""
Large-scale lvol stress test (100 subsystems, 3200 lvols).

Creates 100 subsystems with 32 namespaces each (3200 total lvols), starts
lightweight FIO (iodepth=1, numjobs=1) on every volume, then holds steady
state for 30 minutes verifying system stability under high volume count.

Three modes:
  Docker  (LargeScaleLvolDocker)  — sbcli API + NVMe connect + SSH FIO
  K8s     (LargeScaleLvolK8s)     — PVC + FIO K8s Jobs OR Client SSH FIO

Invocation:
  # Docker
  python3 stress.py --testname LargeScaleLvolDocker --ndcs 2 --npcs 2

  # K8s without client (FIO Jobs)
  python3 stress.py --testname LargeScaleLvolK8s --ndcs 2 --npcs 2 --run_k8s True

  # K8s with client (SSH FIO)
  CLIENT_IP="10.0.0.5" python3 stress.py --testname LargeScaleLvolK8s --ndcs 2 --npcs 2 --run_k8s True
"""

from __future__ import annotations

import os
import random
import re
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger_config import setup_logger
from utils.common_utils import sleep_n_sec

logger = setup_logger(__name__)


def _rand_seq(length: int = 8) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


# ─────────────────────────────────────────────────────────────────────────────
#  Shared mixin: config, orchestration, steady-state, summary
# ─────────────────────────────────────────────────────────────────────────────

class _LargeScaleMixin:
    """Shared orchestration for 100-subsystem / 3200-lvol scale test."""

    # ── Scale ────────────────────────────────────────────────────────────────
    NUM_SUBSYSTEMS = 100
    NAMESPACES_PER_SUBSYSTEM = 32       # 100 × 32 = 3200 total lvols
    LVOL_SIZE = "1G"                    # Docker API
    PVC_SIZE = "1Gi"                    # K8s PVC
    FIO_SIZE = "800M"                   # < 1G to fit within filesystem overhead

    # ── FIO — intentionally lightweight to avoid overload ────────────────────
    FIO_IODEPTH = 1
    FIO_NUMJOBS = 1
    FIO_RUNTIME = 3600                  # 1 hour
    FIO_WAIT_TIMEOUT = 7200             # max 2 hours to wait for FIO completion

    # ── Timing ───────────────────────────────────────────────────────────────
    STEADY_STATE_DURATION = 1800        # 30 minutes
    HEALTH_CHECK_INTERVAL = 60          # seconds between health logs

    # ── Parallelism ──────────────────────────────────────────────────────────
    MAX_WORKERS = 20
    BATCH_SIZE = 50

    # ── Internal state ───────────────────────────────────────────────────────
    _phase_durations: dict
    _fio_failures: int
    _total_created: int

    def _init_mixin_state(self):
        self._phase_durations = {}
        self._fio_failures = 0
        self._total_created = 0
        self._fio_started = 0

    # ── Orchestration ────────────────────────────────────────────────────────

    def _run_large_scale_test(self):
        total = self.NUM_SUBSYSTEMS * self.NAMESPACES_PER_SUBSYSTEM
        self._init_mixin_state()
        self.logger.info(
            f"=== Starting {self.__class__.__name__}: "
            f"{self.NUM_SUBSYSTEMS} subsystems × "
            f"{self.NAMESPACES_PER_SUBSYSTEM} ns = {total} lvols ==="
        )
        try:
            t0 = time.time()
            self._phase_create_subsystems()
            self._phase_durations["create"] = round(time.time() - t0, 1)

            t0 = time.time()
            self._phase_start_fio()
            self._phase_durations["start_fio"] = round(time.time() - t0, 1)

            t0 = time.time()
            self._phase_steady_state()
            self._phase_durations["steady_state"] = round(time.time() - t0, 1)

            t0 = time.time()
            self._phase_validate()
            self._phase_durations["validate"] = round(time.time() - t0, 1)
        finally:
            t0 = time.time()
            self._phase_cleanup()
            self._phase_durations["cleanup"] = round(time.time() - t0, 1)
            self._print_large_scale_summary()
            self._write_monitoring_json()
            self._generate_charts()

        if self._fio_failures > 0:
            raise RuntimeError(
                f"Large-scale test had {self._fio_failures} FIO failures"
            )

    # ── Steady state (shared) ────────────────────────────────────────────────

    def _phase_steady_state(self):
        self.logger.info(
            f"=== Steady state: monitoring for "
            f"{self.STEADY_STATE_DURATION}s ==="
        )
        elapsed = 0
        while elapsed < self.STEADY_STATE_DURATION:
            sleep_n_sec(min(self.HEALTH_CHECK_INTERVAL,
                            self.STEADY_STATE_DURATION - elapsed))
            elapsed += self.HEALTH_CHECK_INTERVAL
            self._log_health_status(elapsed)
        self.logger.info("=== Steady state complete ===")

    def _log_health_status(self, elapsed: int):
        """Override in subclass for mode-specific health checks."""
        self.logger.info(
            f"[health] {elapsed}/{self.STEADY_STATE_DURATION}s elapsed"
        )

    # ── Validate (shared) ────────────────────────────────────────────────────

    def _phase_validate(self):
        """Override in subclass for mode-specific validation."""
        self.logger.info("=== Validation phase ===")

    # ── Summary (shared) ─────────────────────────────────────────────────────

    def _print_large_scale_summary(self):
        total_expected = self.NUM_SUBSYSTEMS * self.NAMESPACES_PER_SUBSYSTEM
        self.logger.info("=" * 65)
        self.logger.info("  LARGE-SCALE LVOL STRESS TEST — SUMMARY")
        self.logger.info("=" * 65)
        self.logger.info(f"  Subsystems:      {self.NUM_SUBSYSTEMS}")
        self.logger.info(f"  NS/subsystem:    {self.NAMESPACES_PER_SUBSYSTEM}")
        self.logger.info(f"  Expected lvols:  {total_expected}")
        self.logger.info(f"  Created lvols:   {self._total_created}")
        self.logger.info(f"  FIO started:     {self._fio_started}")
        self.logger.info(f"  FIO failures:    {self._fio_failures}")
        self.logger.info(f"  FIO config:      iodepth={self.FIO_IODEPTH}  "
                         f"numjobs={self.FIO_NUMJOBS}")
        self.logger.info(f"  Steady state:    {self.STEADY_STATE_DURATION}s")
        for phase, dur in self._phase_durations.items():
            self.logger.info(f"  Phase {phase:15s}: {dur}s")
        self.logger.info("=" * 65)

    def _write_monitoring_json(self):
        """Write standardised timing JSON for monitoring suite aggregation."""
        import json as _json
        from datetime import datetime, timezone
        from pathlib import Path

        phases = []
        for name, dur in self._phase_durations.items():
            phases.append({
                "name": name,
                "duration_sec": dur,
                "status": "ok",
            })

        total_dur = sum(self._phase_durations.values())
        report = {
            "test_class": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed" if self._fio_failures == 0 else "failed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "num_subsystems": self.NUM_SUBSYSTEMS,
                "namespaces_per_subsystem": self.NAMESPACES_PER_SUBSYSTEM,
                "fio_iodepth": self.FIO_IODEPTH,
                "fio_numjobs": self.FIO_NUMJOBS,
                "steady_state_duration": self.STEADY_STATE_DURATION,
            },
            "phases": phases,
            "summary": {
                "total_duration_sec": round(total_dur, 2),
                "key_metric": round(
                    self._phase_durations.get("create", total_dur), 2
                ),
                "key_metric_label": "create_duration_sec",
                "total_created": self._total_created,
                "fio_started": self._fio_started,
                "fio_failures": self._fio_failures,
            },
        }

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "large_scale_timing.json"
        with open(out_path, "w") as f:
            _json.dump(report, f, indent=2)
        self.logger.info(f"Monitoring JSON written to {out_path}")

    def _generate_charts(self):
        """Generate phase duration and scale charts."""
        from pathlib import Path

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            self.logger.warning("matplotlib not available — skipping charts")
            return

        class_name = self.__class__.__name__
        total = self.NUM_SUBSYSTEMS * self.NAMESPACES_PER_SUBSYSTEM

        # Chart 1: Phase duration bar chart
        try:
            phases = list(self._phase_durations.keys())
            durations = list(self._phase_durations.values())
            colors = ["#3498db", "#2ecc71", "#f39c12", "#9b59b6", "#e74c3c"]
            colors = colors[:len(phases)] + ["#95a5a6"] * max(0, len(phases) - len(colors))

            fig, ax = plt.subplots(figsize=(10, 5))
            bars = ax.bar(range(len(phases)), durations, color=colors, alpha=0.8)
            ax.set_xticks(range(len(phases)))
            ax.set_xticklabels(phases, rotation=30, ha="right", fontsize=9)
            ax.set_ylabel("Duration (seconds)")
            ax.set_title(f"{class_name} — Phase Durations ({total} lvols)")
            for b, v in zip(bars, durations):
                ax.text(b.get_x() + b.get_width() / 2, b.get_height() + max(durations) * 0.02,
                        f"{v:.0f}s", ha="center", va="bottom", fontsize=9)
            ax.grid(True, axis="y", alpha=0.3)
            plt.tight_layout()
            path = out_dir / "phase_durations.png"
            fig.savefig(str(path), dpi=150)
            plt.close(fig)
            self.logger.info(f"Chart saved: {path}")
        except Exception as exc:
            self.logger.warning(f"Phase duration chart failed: {exc}")

        # Chart 2: Creation rate (lvols/sec)
        try:
            create_dur = self._phase_durations.get("create", 0)
            if create_dur > 0:
                fig, ax = plt.subplots(figsize=(8, 4))
                rate = self._total_created / create_dur
                ax.barh(["Created lvols", "Create rate"],
                        [self._total_created, rate],
                        color=["#3498db", "#2ecc71"], alpha=0.8)
                ax.set_xlabel("Count / Rate")
                ax.set_title(f"{class_name} — Creation Summary")
                ax.text(self._total_created, 0,
                        f"  {self._total_created}/{total}", va="center", fontsize=9)
                ax.text(rate, 1,
                        f"  {rate:.1f} lvols/sec", va="center", fontsize=9)
                plt.tight_layout()
                path = out_dir / "creation_rate.png"
                fig.savefig(str(path), dpi=150)
                plt.close(fig)
                self.logger.info(f"Chart saved: {path}")
        except Exception as exc:
            self.logger.warning(f"Creation rate chart failed: {exc}")

        # Chart 3: FIO summary pie
        try:
            if self._fio_started > 0:
                fig, ax = plt.subplots(figsize=(6, 5))
                success = self._fio_started - self._fio_failures
                values = [success, self._fio_failures]
                labels = [f"OK ({success})", f"Failed ({self._fio_failures})"]
                colors = ["#2ecc71", "#e74c3c"] if self._fio_failures > 0 else ["#2ecc71", "#ecf0f1"]
                ax.pie(values, labels=labels, colors=colors, autopct="%1.0f%%",
                       startangle=90)
                ax.set_title(f"{class_name} — FIO Status ({self._fio_started} total)")
                plt.tight_layout()
                path = out_dir / "fio_summary.png"
                fig.savefig(str(path), dpi=150)
                plt.close(fig)
                self.logger.info(f"Chart saved: {path}")
        except Exception as exc:
            self.logger.warning(f"FIO summary chart failed: {exc}")

    # ── Abstract methods (subclasses implement) ──────────────────────────────

    def _phase_create_subsystems(self):
        raise NotImplementedError

    def _phase_start_fio(self):
        raise NotImplementedError

    def _phase_cleanup(self):
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────────────────────
#  Docker variant — sbcli API + NVMe connect + SSH FIO
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.lvol_ha_stress_fio import TestLvolHACluster  # noqa: E402
from utils.ssh_utils import get_parent_device  # noqa: E402


class LargeScaleLvolDocker(_LargeScaleMixin, TestLvolHACluster):
    """100 subsystems × 32 namespaces with FIO via SSH (Docker / bare-metal)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "large_scale_lvol_docker"
        self.fio_threads: list[threading.Thread] = []

        # parent_name → {id, client, ctrl_dev, nqn, devices: [dev_path]}
        self._parent_registry: dict[str, dict] = {}
        # child_name → {id, parent_name}
        self._child_registry: dict[str, dict] = {}
        # device_path → {name, client, mount, log, parent_name}
        self._device_registry: dict[str, dict] = {}

    # ── NVMe namespace helpers (from continuous_failover_ha_namespace.py) ────

    def _list_nvme_ns_devices(self, node: str, ctrl_dev: str) -> list[str]:
        """Return sorted list of namespace devices, e.g. ['/dev/nvme5n1', ...]."""
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"ls -1 {ctrl}n* 2>/dev/null | sort -V || true\""
        out, _ = self.ssh_obj.exec_command(node=node, command=cmd,
                                           supress_logs=True)
        return [x.strip() for x in (out or "").splitlines() if x.strip()]

    def _rescan_nvme_namespaces(self, node: str, ctrl_dev: str):
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"nvme ns-rescan {ctrl} 2>/dev/null || true\""
        self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=True)

    def _wait_for_new_namespace_device(self, node: str, ctrl_dev: str,
                                       before_set: set, timeout: int = 120,
                                       interval: int = 3):
        """Poll until a NEW namespace device appears that wasn't in before_set.

        Returns (new_device_path, updated_set) or (None, current_set).
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            self._rescan_nvme_namespaces(node, ctrl_dev)
            sleep_n_sec(interval)
            cur = set(self._list_nvme_ns_devices(node, ctrl_dev))
            diff = sorted(cur - before_set)
            if diff:
                return diff[-1], cur
            self.logger.info(
                f"[ns-wait] {ctrl_dev} on {node}: "
                f"no new device yet ({len(cur)} visible)"
            )
        return None, set(self._list_nvme_ns_devices(node, ctrl_dev))

    def _wait_until_namespace_device_gone(self, node: str, ctrl_dev: str,
                                          device: str, timeout: int = 120,
                                          interval: int = 3) -> bool:
        """Poll until *device* is no longer visible on the controller."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            self._rescan_nvme_namespaces(node, ctrl_dev)
            sleep_n_sec(interval)
            cur = set(self._list_nvme_ns_devices(node, ctrl_dev))
            if device not in cur:
                return True
        return False

    # ── run() ────────────────────────────────────────────────────────────────

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
        self._run_large_scale_test()

    # ── Phase 1: Create subsystems ───────────────────────────────────────────

    def _phase_create_subsystems(self):
        self.logger.info("=== Phase: Create Subsystems (Docker) ===")

        # Sub-phase 1: Create 100 parent lvols in parallel
        self.logger.info(
            f"[create] Sub-phase 1: Creating {self.NUM_SUBSYSTEMS} parents"
        )
        parent_items = []
        for i in range(self.NUM_SUBSYSTEMS):
            name = f"lss-par-{_rand_seq(6)}-{i:03d}"
            parent_items.append({"name": name, "idx": i})

        self._batch_exec(parent_items, self._create_parent, "create_parents")

        parent_count = len(self._parent_registry)
        self.logger.info(f"[create] {parent_count} parents created")
        if parent_count == 0:
            raise RuntimeError("No parents created — cannot continue")

        # Sub-phase 2: NVMe connect all parents + format/mount parent device
        self.logger.info(
            f"[create] Sub-phase 2: NVMe connecting {parent_count} parents"
        )
        parent_names = list(self._parent_registry.keys())
        self._batch_exec(
            parent_names, self._connect_parent, "connect_parents"
        )

        connected = sum(
            1 for p in self._parent_registry.values() if p.get("ctrl_dev")
        )
        self.logger.info(f"[create] {connected} parents connected")

        # Sub-phase 3: Create namespace children per parent
        # (sequential within a parent, parallel across parents)
        total_children = (self.NAMESPACES_PER_SUBSYSTEM - 1) * connected
        self.logger.info(
            f"[create] Sub-phase 3: Creating {total_children} namespace "
            f"children ({self.NAMESPACES_PER_SUBSYSTEM - 1} per parent)"
        )
        connected_parents = [
            pname for pname, pinfo in self._parent_registry.items()
            if pinfo.get("ctrl_dev")
        ]
        # Each parent creates 31 children sequentially (~130s each worst case)
        self._batch_exec(
            connected_parents,
            self._create_children_for_parent,
            "create_children",
            per_item_timeout=5400,  # 90 min per parent
        )

        child_count = len(self._child_registry)
        self._total_created = len(self._device_registry)
        self.logger.info(
            f"[create] {child_count} children created, "
            f"{self._total_created} total devices formatted + mounted"
        )

    def _create_parent(self, params: dict):
        name = params["name"]
        try:
            self.sbcli_utils.add_lvol(
                lvol_name=name,
                pool_name=self.pool_name,
                size=self.LVOL_SIZE,
                distr_ndcs=self.ndcs,
                distr_npcs=self.npcs,
                distr_bs=self.bs,
                distr_chunk_bs=self.chunk_bs,
                max_namespace_per_subsys=self.NAMESPACES_PER_SUBSYSTEM,
                retry=3,
            )
            sleep_n_sec(2)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=name)
            if not lvol_id:
                self.logger.error(f"[create_parent] {name}: ID not found")
                return
            self._parent_registry[name] = {
                "id": lvol_id,
                "client": None,
                "ctrl_dev": None,
                "nqn": None,
                "devices": [],
            }
            self.logger.info(f"[create_parent] {name} -> {lvol_id}")
        except Exception as e:
            self.logger.error(f"[create_parent] {name} failed: {e}")

    def _connect_parent(self, parent_name: str):
        """NVMe-connect parent, detect device, format + mount the parent
        namespace (nsid=1)."""
        pinfo = self._parent_registry.get(parent_name)
        if not pinfo:
            return
        try:
            connect_ls = self.sbcli_utils.get_lvol_connect_str(
                lvol_name=parent_name
            )
            if not connect_ls:
                self.logger.error(
                    f"[connect] {parent_name}: no connect strings"
                )
                return

            # Round-robin across client nodes
            client = self.fio_node[
                list(self._parent_registry.keys()).index(parent_name)
                % len(self.fio_node)
            ]
            pinfo["client"] = client

            initial_devices = self.ssh_obj.get_devices(node=client)

            for cmd in connect_ls:
                self.ssh_obj.exec_command(node=client, command=cmd)
                # Extract NQN for later disconnect
                nqn_match = re.search(r"-n\s+(nqn\S+)", cmd)
                if nqn_match:
                    pinfo["nqn"] = nqn_match.group(1)

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client)

            parent_dev = None
            for dev in final_devices:
                if dev not in initial_devices:
                    parent_dev = f"/dev/{dev.strip()}"
                    break

            if not parent_dev:
                self.logger.error(
                    f"[connect] {parent_name}: no new device after connect"
                )
                return

            ctrl_dev = get_parent_device(parent_dev)
            pinfo["ctrl_dev"] = ctrl_dev
            pinfo["devices"] = [parent_dev]

            # Format + mount the parent device (nsid=1)
            mount_name = f"lss-{parent_name[-3:]}-ns01"
            mount_point = f"{self.mount_path}/{mount_name}"
            log_file = f"{self.log_path}/{mount_name}.log"
            self.ssh_obj.format_disk(
                node=client, device=parent_dev, fs_type="ext4"
            )
            self.ssh_obj.mount_path(
                node=client, device=parent_dev, mount_path=mount_point
            )
            self._device_registry[parent_dev] = {
                "name": mount_name,
                "client": client,
                "mount": mount_point,
                "log": log_file,
                "parent_name": parent_name,
                "ctrl_dev": ctrl_dev,
                "ns_idx": 1,
            }
            self.logger.info(
                f"[connect] {parent_name}: {parent_dev} ns01 "
                f"(ctrl={ctrl_dev}) on {client} -> {mount_point}"
            )
        except Exception as e:
            self.logger.error(f"[connect] {parent_name} failed: {e}")

    def _create_children_for_parent(self, parent_name: str):
        """Create all namespace children for one parent sequentially.

        For each child:
          1. add_lvol(namespace=parent_id)
          2. Verify the new namespace device appears on the client
             (rescan if it doesn't show up automatically)
          3. Format + mount the new device
        """
        pinfo = self._parent_registry.get(parent_name)
        if not pinfo or not pinfo.get("ctrl_dev"):
            return
        parent_id = pinfo["id"]
        client = pinfo["client"]
        ctrl_dev = pinfo["ctrl_dev"]

        # Snapshot of current namespace devices before creating children
        before_set = set(self._list_nvme_ns_devices(client, ctrl_dev))
        created = 0

        for ns_idx in range(2, self.NAMESPACES_PER_SUBSYSTEM + 1):
            cname = (
                f"lss-ch-{parent_name[-3:]}-ns{ns_idx:02d}-{_rand_seq(4)}"
            )
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=cname,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                    namespace=parent_id,
                    retry=3,
                )
                sleep_n_sec(2)
                child_id = self.sbcli_utils.get_lvol_id(lvol_name=cname)
                if not child_id:
                    self.logger.error(
                        f"[create_child] {cname}: ID not found"
                    )
                    continue

                # Wait for the new namespace device to appear on client
                new_dev, new_set = self._wait_for_new_namespace_device(
                    node=client,
                    ctrl_dev=ctrl_dev,
                    before_set=before_set,
                    timeout=120,
                    interval=3,
                )
                if not new_dev:
                    self.logger.error(
                        f"[create_child] {cname}: namespace device did not "
                        f"appear on {client} (ctrl={ctrl_dev})"
                    )
                    continue
                before_set = new_set

                # Format + mount the new namespace device
                mount_name = (
                    f"lss-{parent_name[-3:]}-ns{ns_idx:02d}"
                )
                mount_point = f"{self.mount_path}/{mount_name}"
                log_file = f"{self.log_path}/{mount_name}.log"
                self.ssh_obj.format_disk(
                    node=client, device=new_dev, fs_type="ext4"
                )
                self.ssh_obj.mount_path(
                    node=client, device=new_dev, mount_path=mount_point
                )

                self._child_registry[cname] = {
                    "id": child_id,
                    "parent_name": parent_name,
                    "device": new_dev,
                    "ns_idx": ns_idx,
                }
                self._device_registry[new_dev] = {
                    "name": mount_name,
                    "client": client,
                    "mount": mount_point,
                    "log": log_file,
                    "parent_name": parent_name,
                    "ctrl_dev": ctrl_dev,
                    "ns_idx": ns_idx,
                }
                created += 1
                self.logger.info(
                    f"[create_child] {cname} -> {child_id} "
                    f"ns{ns_idx:02d} device={new_dev} on {client}"
                )
            except Exception as e:
                self.logger.error(
                    f"[create_child] {cname} failed: {e}"
                )

        self.logger.info(
            f"[create_children] {parent_name}: "
            f"{created}/{self.NAMESPACES_PER_SUBSYSTEM - 1} children created"
        )

    # ── Phase 2: Start FIO ──────────────────────────────────────────────────

    def _phase_start_fio(self):
        self.logger.info(
            f"=== Phase: Start FIO ({len(self._device_registry)} devices) ==="
        )
        items = list(self._device_registry.items())
        self._batch_exec(items, self._start_single_fio, "start_fio")
        self._fio_started = len(self.fio_threads)
        self.logger.info(f"[fio] {self._fio_started} FIO threads started")

    def _start_single_fio(self, item: tuple):
        device, dinfo = item
        name = dinfo["name"]
        client = dinfo["client"]
        mount_point = dinfo["mount"]
        log_file = dinfo["log"]
        bs = f"{2 ** random.randint(2, 7)}K"
        randseed = random.randint(1, 2**63)
        iolog_file = log_file.replace(".log", "_iolog.log")
        fio_log_file = log_file.replace(".log", "_fio")

        try:
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, mount_point, log_file),
                kwargs={
                    "size": self.FIO_SIZE,
                    "name": f"{name}_fio",
                    "rw": "randrw",
                    "bs": bs,
                    "nrfiles": 4,
                    "iodepth": self.FIO_IODEPTH,
                    "numjobs": self.FIO_NUMJOBS,
                    "time_based": True,
                    "runtime": self.FIO_RUNTIME,
                    "randseed": randseed,
                    "iolog_file": iolog_file,
                    "fio_log_file": fio_log_file,
                    "log_avg_msec": 1000,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
        except Exception as e:
            self.logger.error(f"[fio] {name} on {client} failed to start: {e}")

    # ── Health status ────────────────────────────────────────────────────────

    def _log_health_status(self, elapsed: int):
        alive = sum(1 for t in self.fio_threads if t.is_alive())
        total = len(self.fio_threads)
        dead = total - alive
        self.logger.info(
            f"[health] {elapsed}/{self.STEADY_STATE_DURATION}s — "
            f"FIO threads: {alive}/{total} alive, {dead} dead"
        )
        if dead > 0:
            self._fio_failures = dead

    # ── Validate ─────────────────────────────────────────────────────────────

    def _phase_validate(self):
        self.logger.info("=== Phase: Validate FIO (Docker) ===")
        alive = sum(1 for t in self.fio_threads if t.is_alive())
        dead = len(self.fio_threads) - alive
        self.logger.info(
            f"[validate] FIO threads: {alive} alive, {dead} dead"
        )
        if dead > 0:
            self._fio_failures = dead
            self.logger.error(
                f"[validate] {dead} FIO threads died during test"
            )

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Phase: Cleanup (Docker) ===")

        # 1. Wait for FIO threads to complete (up to FIO_WAIT_TIMEOUT)
        alive = sum(1 for t in self.fio_threads if t.is_alive())
        if alive > 0:
            self.logger.info(
                f"[cleanup] Waiting for {alive} FIO threads to finish "
                f"(timeout={self.FIO_WAIT_TIMEOUT}s)"
            )
            deadline = time.time() + self.FIO_WAIT_TIMEOUT
            for t in self.fio_threads:
                remaining = max(0, deadline - time.time())
                if remaining <= 0:
                    break
                t.join(timeout=remaining)
            alive = sum(1 for t in self.fio_threads if t.is_alive())
            if alive > 0:
                self.logger.warning(
                    f"[cleanup] {alive} FIO threads still running "
                    f"after {self.FIO_WAIT_TIMEOUT}s — killing"
                )
                clients_used = set(
                    d["client"] for d in self._device_registry.values()
                )
                for client in clients_used:
                    try:
                        self.ssh_obj.exec_command(
                            node=client,
                            command="bash -lc "
                                    "'pkill -9 -f fio 2>/dev/null || true'",
                        )
                    except Exception:
                        pass
                sleep_n_sec(5)
            else:
                self.logger.info("[cleanup] All FIO threads completed")

        # 2. Unmount all filesystems
        for device, dinfo in self._device_registry.items():
            try:
                self.ssh_obj.exec_command(
                    node=dinfo["client"],
                    command=f"bash -lc 'umount {dinfo['mount']} 2>/dev/null; "
                            f"rm -rf {dinfo['mount']} 2>/dev/null || true'",
                )
            except Exception:
                pass

        # 3. Delete children individually with device-gone verification
        #    Group by parent so we can parallelize across parents
        children_by_parent: dict[str, list] = {}
        for cname, cinfo in self._child_registry.items():
            pname = cinfo["parent_name"]
            children_by_parent.setdefault(pname, []).append(
                (cname, cinfo)
            )

        parent_names_for_cleanup = list(children_by_parent.keys())
        if parent_names_for_cleanup:
            self.logger.info(
                f"[cleanup] Deleting {len(self._child_registry)} children "
                f"across {len(parent_names_for_cleanup)} parents"
            )
            self._batch_exec(
                parent_names_for_cleanup,
                lambda pn: self._delete_children_for_parent(
                    pn, children_by_parent.get(pn, [])
                ),
                "delete_children",
                per_item_timeout=5400,  # 90 min per parent
            )

        # 4. Delete parents + disconnect NVMe controllers
        self.logger.info(
            f"[cleanup] Deleting {len(self._parent_registry)} parents"
        )
        for pname, pinfo in self._parent_registry.items():
            try:
                self.sbcli_utils.delete_lvol(
                    pname, max_attempt=120, skip_error=True
                )
                self.logger.info(f"[cleanup] Deleted parent {pname}")
            except Exception as e:
                self.logger.warning(
                    f"[cleanup] Parent {pname} delete failed: {e}"
                )

            # Disconnect NVMe controller (all namespaces gone)
            if pinfo.get("nqn") and pinfo.get("client"):
                try:
                    self.ssh_obj.exec_command(
                        node=pinfo["client"],
                        command=f"bash -lc 'nvme disconnect -n "
                                f"{pinfo['nqn']} 2>/dev/null || true'",
                    )
                except Exception:
                    pass
        sleep_n_sec(5)

        # 5. Safety net: bulk-delete anything remaining + pool
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception:
            pass

        self.logger.info("[cleanup] Docker cleanup complete")

    def _delete_children_for_parent(self, parent_name: str,
                                    children: list[tuple]):
        """Delete all namespace children of one parent sequentially,
        verifying each device is gone on the client after deletion."""
        pinfo = self._parent_registry.get(parent_name, {})
        client = pinfo.get("client")
        ctrl_dev = pinfo.get("ctrl_dev")

        for cname, cinfo in reversed(children):
            device = cinfo.get("device")
            try:
                # delete_lvol already polls until lvol is gone
                self.sbcli_utils.delete_lvol(
                    cname, max_attempt=120, skip_error=True
                )
                self.logger.info(f"[cleanup] Deleted child {cname}")
            except Exception as e:
                self.logger.warning(
                    f"[cleanup] Child {cname} delete failed: {e}"
                )
                continue

            # Verify namespace device is gone on client
            if client and ctrl_dev and device:
                self._rescan_nvme_namespaces(client, ctrl_dev)
                ok = self._wait_until_namespace_device_gone(
                    node=client, ctrl_dev=ctrl_dev,
                    device=device, timeout=60, interval=3,
                )
                if ok:
                    self.logger.info(
                        f"[cleanup] Verified {device} gone on {client}"
                    )
                else:
                    self.logger.warning(
                        f"[cleanup] {device} still present on "
                        f"{client} after deleting {cname}"
                    )

    # ── Batch parallel helper ────────────────────────────────────────────────

    def _batch_exec(self, items, task_fn, op_name: str,
                    per_item_timeout: int = 600):
        """Execute task_fn(item) for each item using ThreadPoolExecutor."""
        total = len(items)
        success = 0
        failures = 0

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for batch_start in range(0, total, self.BATCH_SIZE):
                batch = items[batch_start:batch_start + self.BATCH_SIZE]
                futures = {}
                for item in batch:
                    f = executor.submit(task_fn, item)
                    futures[f] = item

                for f in as_completed(futures):
                    try:
                        f.result(timeout=per_item_timeout)
                        success += 1
                    except Exception as exc:
                        failures += 1
                        self.logger.error(
                            f"[{op_name}] Failed: {exc}"
                        )

                done = batch_start + len(batch)
                self.logger.info(
                    f"[{op_name}] progress: {done}/{total} "
                    f"(ok={success} fail={failures})"
                )

        return success, failures


# ─────────────────────────────────────────────────────────────────────────────
#  K8s variant — PVC + FIO K8s Jobs or Client SSH FIO
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest  # noqa: E402


class LargeScaleLvolK8s(_LargeScaleMixin, K8sNativeFailoverTest):
    """100 subsystems × 32 namespaces via K8s PVC + FIO Jobs / SSH."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "large_scale_lvol_k8s"
        # Override base class FIO config for lightweight load
        self.fio_num_jobs = self.FIO_NUMJOBS
        self.FIO_RUNTIME = 7200

    # ── run() ────────────────────────────────────────────────────────────────

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        cluster_id = self.cluster_id or os.environ.get("CLUSTER_ID", "")
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
            max_namespace_per_subsys=self.NAMESPACES_PER_SUBSYSTEM,
        )

        self._run_large_scale_test()

    # ── Phase 1: Create subsystems ───────────────────────────────────────────

    def _phase_create_subsystems(self):
        total_pvcs = self.NUM_SUBSYSTEMS * self.NAMESPACES_PER_SUBSYSTEM
        self.logger.info(
            f"=== Phase: Create {total_pvcs} PVCs (K8s) ==="
        )

        pvc_items = []
        for i in range(total_pvcs):
            pvc_name = f"lss-pvc-{_rand_seq(6)}-{i:04d}"
            pvc_items.append({"name": pvc_name, "idx": i})

        if self.use_client_fio:
            self._create_pvcs_client_mode(pvc_items)
        else:
            self._create_pvcs_job_mode(pvc_items)

        self._total_created = len(self.pvc_details)
        self.logger.info(f"[create] {self._total_created} PVCs created")

    def _create_pvcs_job_mode(self, items: list[dict]):
        """Create PVCs in parallel (K8s Job FIO mode)."""
        self._batch_exec_k8s(items, self._create_single_pvc, "create_pvcs")

    def _create_pvcs_client_mode(self, items: list[dict]):
        """Create PVCs + NVMe connect on clients."""
        self._batch_exec_k8s(
            items, self._create_single_pvc_client, "create_pvcs_client"
        )

    def _create_single_pvc(self, params: dict):
        name = params["name"]
        try:
            self.k8s_utils.create_pvc(
                name=name,
                size=self.PVC_SIZE,
                storage_class=self.STORAGE_CLASS_NAME,
            )
            if not self.k8s_utils.wait_pvc_bound(name, timeout=300):
                self.logger.error(f"[create_pvc] {name}: not Bound in 300s")
                return
            self.pvc_details[name] = {
                "job_name": None,
                "configmap_name": None,
                "snapshots": [],
            }
            self.logger.info(f"[create_pvc] {name} Bound")
        except Exception as e:
            self.logger.error(f"[create_pvc] {name} failed: {e}")

    def _create_single_pvc_client(self, params: dict):
        """Create a single PVC, NVMe-connect on a client, and verify the
        namespace device appears.  CSI auto-groups PVCs into subsystems
        based on the StorageClass max_namespace_per_subsys setting.

        After NVMe connect, the device may appear as:
        - A new controller + namespace (first PVC in a subsystem)
        - A new namespace on an existing controller (shared subsystem)
        Either way we verify a new block device is present.
        """
        name = params["name"]
        try:
            self.k8s_utils.create_pvc(
                name=name,
                size=self.PVC_SIZE,
                storage_class=self.STORAGE_CLASS_NAME,
            )
            if not self.k8s_utils.wait_pvc_bound(name, timeout=300):
                self.logger.error(f"[create_pvc] {name}: not Bound in 300s")
                return

            # Get lvol info for NVMe connect
            lvol_id = self.k8s_utils.get_pvc_volume_handle(name)
            if not lvol_id:
                self.logger.error(
                    f"[create_pvc] {name}: no volume handle"
                )
                return

            lvol_name = None
            lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            if lvol_details:
                lvol_name = lvol_details[0].get("lvol_name", name)
            else:
                lvol_name = name

            connect_ls = self.sbcli_utils.get_lvol_connect_str(
                lvol_name=lvol_name
            )

            client = self.fio_node[params["idx"] % len(self.fio_node)]

            # Snapshot devices before connect
            initial_devices = set(self.ssh_obj.get_devices(node=client))

            # Extract NQN from connect strings for namespace tracking
            nqn = None
            for cmd in connect_ls:
                self.ssh_obj.exec_command(node=client, command=cmd)
                nqn_match = re.search(r"-n\s+(nqn\S+)", cmd)
                if nqn_match:
                    nqn = nqn_match.group(1)

            sleep_n_sec(3)

            # Check for new device — could be new controller or new namespace
            final_devices = set(self.ssh_obj.get_devices(node=client))
            new_devs = sorted(final_devices - initial_devices)

            new_dev = None
            if new_devs:
                new_dev = f"/dev/{new_devs[-1].strip()}"
            else:
                # Device didn't appear automatically — try NVMe rescan
                # Find controller for this NQN and rescan namespaces
                self.logger.info(
                    f"[create_pvc] {name}: no new device, rescanning"
                )
                # Rescan all controllers on this client
                rescan_cmd = (
                    "bash -lc 'for c in /dev/nvme*; do "
                    "[ -c \"$c\" ] && nvme ns-rescan $c 2>/dev/null; "
                    "done || true'"
                )
                self.ssh_obj.exec_command(
                    node=client, command=rescan_cmd
                )
                sleep_n_sec(5)
                final_devices = set(self.ssh_obj.get_devices(node=client))
                new_devs = sorted(final_devices - initial_devices)
                if new_devs:
                    new_dev = f"/dev/{new_devs[-1].strip()}"

            if not new_dev:
                self.logger.error(
                    f"[create_pvc] {name}: no device after NVMe "
                    f"connect + rescan on {client}"
                )
                return

            ctrl_dev = get_parent_device(new_dev)
            mount_point = f"{self.mount_path}/{name}"
            log_file = f"{self.log_path}/{name}.log"

            self.ssh_obj.format_disk(
                node=client, device=new_dev, fs_type="ext4"
            )
            self.ssh_obj.mount_path(
                node=client, device=new_dev, mount_path=mount_point
            )

            self.pvc_details[name] = {
                "job_name": None,
                "configmap_name": None,
                "snapshots": [],
            }
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id,
                "Name": lvol_name,
                "Mount": mount_point,
                "Device": new_dev,
                "FS": "ext4",
                "Log": log_file,
                "Client": client,
                "pvc_name": name,
                "ctrl_dev": ctrl_dev,
                "nqn": nqn,
            }
            self.logger.info(
                f"[create_pvc] {name} -> {new_dev} "
                f"(ctrl={ctrl_dev}) on {client}"
            )
        except Exception as e:
            self.logger.error(f"[create_pvc] {name} failed: {e}")

    # ── Phase 2: Start FIO ──────────────────────────────────────────────────

    def _phase_start_fio(self):
        if self.use_client_fio:
            self._start_fio_client_mode()
        else:
            self._start_fio_job_mode()

    def _start_fio_job_mode(self):
        """Create FIO K8s Jobs for each PVC."""
        total = len(self.pvc_details)
        self.logger.info(f"=== Phase: Start FIO Jobs ({total} PVCs) ===")

        items = list(self.pvc_details.items())
        self._batch_exec_k8s(items, self._create_fio_job, "start_fio_jobs")
        self._fio_started = sum(
            1 for p in self.pvc_details.values() if p.get("job_name")
        )
        self.logger.info(f"[fio] {self._fio_started} FIO jobs created")

    def _create_fio_job(self, item: tuple):
        pvc_name, pvc_info = item
        job_name = f"fio-{pvc_name}"
        cm_name = f"fio-cm-{pvc_name}"
        bs = f"{2 ** random.randint(2, 7)}k"
        run_id = _rand_seq(6)
        randseed = random.randint(1, 2**63)

        fio_config = (
            f"[global]\n"
            f"name={pvc_name}-fio\n"
            f"filename_format=/spdkvol/fio-{run_id}.$jobnum\n"
            f"rw=randrw\n"
            f"rwmixread=50\n"
            f"bs={bs}\n"
            f"iodepth={self.FIO_IODEPTH}\n"
            f"direct=1\n"
            f"ioengine=libaio\n"
            f"size={self.FIO_SIZE}\n"
            f"numjobs={self.FIO_NUMJOBS}\n"
            f"time_based\n"
            f"runtime={self.FIO_RUNTIME}\n"
            f"group_reporting\n"
            f"verify=md5\n"
            f"verify_dump=1\n"
            f"verify_fatal=1\n"
            f"verify_backlog=4096\n"
            f"verify_backlog_batch=32\n"
            f"randseed={randseed}\n"
            f"max_latency=20s\n"
            f"log_avg_msec=1000\n"
            f"\n"
            f"[job1]\n"
        )

        try:
            self.k8s_utils.create_fio_job(
                job_name=job_name,
                pvc_name=pvc_name,
                configmap_name=cm_name,
                fio_config=fio_config,
            )
            pvc_info["job_name"] = job_name
            pvc_info["configmap_name"] = cm_name
            self.logger.info(f"[fio] Created job {job_name}")
        except Exception as e:
            self.logger.error(f"[fio] Job {job_name} failed: {e}")

    def _start_fio_client_mode(self):
        """Start FIO via SSH on client nodes."""
        total = len(self.lvol_mount_details)
        self.logger.info(
            f"=== Phase: Start FIO SSH ({total} volumes) ==="
        )
        for lvol_name, details in self.lvol_mount_details.items():
            client = details["Client"]
            mount_point = details["Mount"]
            log_file = details["Log"]
            bs = f"{2 ** random.randint(2, 7)}K"
            randseed = random.randint(1, 2**63)
            iolog_file = log_file.replace(".log", "_iolog.log")
            fio_log_file = log_file.replace(".log", "_fio")

            try:
                fio_thread = threading.Thread(
                    target=self.ssh_obj.run_fio_test,
                    args=(client, None, mount_point, log_file),
                    kwargs={
                        "size": self.FIO_SIZE,
                        "name": f"{lvol_name}_fio",
                        "rw": "randrw",
                        "bs": bs,
                        "nrfiles": 4,
                        "iodepth": self.FIO_IODEPTH,
                        "numjobs": self.FIO_NUMJOBS,
                        "time_based": True,
                        "runtime": self.FIO_RUNTIME,
                        "randseed": randseed,
                        "iolog_file": iolog_file,
                        "fio_log_file": fio_log_file,
                        "log_avg_msec": 1000,
                    },
                )
                fio_thread.start()
                self.fio_threads.append(fio_thread)
            except Exception as e:
                self.logger.error(
                    f"[fio] {lvol_name} on {client} failed: {e}"
                )
        self._fio_started = len(self.fio_threads)
        self.logger.info(f"[fio] {self._fio_started} FIO threads started")

    # ── Health status ────────────────────────────────────────────────────────

    def _log_health_status(self, elapsed: int):
        if self.use_client_fio:
            alive = sum(1 for t in self.fio_threads if t.is_alive())
            total = len(self.fio_threads)
            dead = total - alive
            self.logger.info(
                f"[health] {elapsed}/{self.STEADY_STATE_DURATION}s — "
                f"FIO threads: {alive}/{total} alive, {dead} dead"
            )
            if dead > 0:
                self._fio_failures = dead
        else:
            # K8s Job mode: check job status
            try:
                ns = self.k8s_utils.namespace
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get jobs -n {ns} "
                    f"-l app=fio "
                    f"-o jsonpath='{{.items[*].status.conditions[0].type}}' "
                    f"2>/dev/null || true",
                    supress_logs=True,
                )
                conditions = (out or "").split()
                failed = conditions.count("Failed")
                complete = conditions.count("Complete")
                active = len(self.pvc_details) - failed - complete
                self.logger.info(
                    f"[health] {elapsed}/{self.STEADY_STATE_DURATION}s — "
                    f"Jobs: {active} active, {complete} complete, "
                    f"{failed} failed"
                )
                if failed > 0:
                    self._fio_failures = failed
            except Exception as e:
                self.logger.warning(f"[health] Failed to check jobs: {e}")

    # ── Validate ─────────────────────────────────────────────────────────────

    def _phase_validate(self):
        self.logger.info("=== Phase: Validate FIO (K8s) ===")
        if self.use_client_fio:
            alive = sum(1 for t in self.fio_threads if t.is_alive())
            dead = len(self.fio_threads) - alive
            self.logger.info(
                f"[validate] FIO threads: {alive} alive, {dead} dead"
            )
            if dead > 0:
                self._fio_failures = dead
                self.logger.error(
                    f"[validate] {dead} FIO threads died during test"
                )
        else:
            # Check K8s Job statuses
            try:
                ns = self.k8s_utils.namespace
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get jobs -n {ns} "
                    f"-l app=fio "
                    f"-o jsonpath='{{.items[*].status.failed}}' "
                    f"2>/dev/null || true",
                    supress_logs=True,
                )
                failed_counts = [
                    int(x) for x in (out or "").split() if x.strip()
                ]
                total_failed = sum(failed_counts)
                self.logger.info(
                    f"[validate] {total_failed} jobs have failures"
                )
                self._fio_failures = total_failed
            except Exception as e:
                self.logger.warning(f"[validate] Job check failed: {e}")

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Phase: Cleanup (K8s) ===")

        if self.use_client_fio:
            # Wait for FIO threads to complete (up to FIO_WAIT_TIMEOUT)
            alive = sum(1 for t in self.fio_threads if t.is_alive())
            if alive > 0:
                self.logger.info(
                    f"[cleanup] Waiting for {alive} FIO threads to finish "
                    f"(timeout={self.FIO_WAIT_TIMEOUT}s)"
                )
                deadline = time.time() + self.FIO_WAIT_TIMEOUT
                for t in self.fio_threads:
                    remaining = max(0, deadline - time.time())
                    if remaining <= 0:
                        break
                    t.join(timeout=remaining)
                alive = sum(1 for t in self.fio_threads if t.is_alive())
                if alive > 0:
                    self.logger.warning(
                        f"[cleanup] {alive} FIO threads still running "
                        f"after {self.FIO_WAIT_TIMEOUT}s — killing"
                    )
                    clients_used = set(
                        d["Client"]
                        for d in self.lvol_mount_details.values()
                    )
                    for client in clients_used:
                        try:
                            self.ssh_obj.exec_command(
                                node=client,
                                command="bash -lc "
                                        "'pkill -9 -f fio "
                                        "2>/dev/null || true'",
                            )
                        except Exception:
                            pass
                    sleep_n_sec(5)
                else:
                    self.logger.info(
                        "[cleanup] All FIO threads completed"
                    )

            # Unmount all
            for lvol_name, details in self.lvol_mount_details.items():
                try:
                    self.ssh_obj.exec_command(
                        node=details["Client"],
                        command=f"bash -lc 'umount {details['Mount']} "
                                f"2>/dev/null; rm -rf {details['Mount']} "
                                f"2>/dev/null || true'",
                    )
                except Exception:
                    pass

            # Delete lvols individually with device-gone verification
            for lvol_name, details in list(self.lvol_mount_details.items()):
                client = details.get("Client")
                device = details.get("Device")
                ctrl_dev = details.get("ctrl_dev")

                try:
                    self.sbcli_utils.delete_lvol(
                        lvol_name, max_attempt=120, skip_error=True
                    )
                    self.logger.info(f"[cleanup] Deleted {lvol_name}")
                except Exception as e:
                    self.logger.warning(
                        f"[cleanup] {lvol_name} delete failed: {e}"
                    )

                # Verify namespace device is gone on client
                if client and ctrl_dev and device:
                    rescan_cmd = (
                        f"bash -lc 'nvme ns-rescan "
                        f"{get_parent_device(ctrl_dev)} "
                        f"2>/dev/null || true'"
                    )
                    self.ssh_obj.exec_command(
                        node=client, command=rescan_cmd,
                        supress_logs=True,
                    )
                    sleep_n_sec(3)
                    # Check device is gone
                    check_cmd = (
                        f"bash -lc 'test -b {device} && "
                        f"echo EXISTS || echo GONE'"
                    )
                    out, _ = self.ssh_obj.exec_command(
                        node=client, command=check_cmd,
                        supress_logs=True,
                    )
                    if "GONE" in (out or ""):
                        self.logger.info(
                            f"[cleanup] Verified {device} gone "
                            f"on {client}"
                        )
                    else:
                        self.logger.warning(
                            f"[cleanup] {device} still present "
                            f"on {client} after deleting {lvol_name}"
                        )

            # Disconnect NVMe controllers (group by NQN to avoid dupes)
            disconnected_nqns: set = set()
            for lvol_name, details in self.lvol_mount_details.items():
                nqn = details.get("nqn")
                client = details.get("Client")
                if nqn and client and nqn not in disconnected_nqns:
                    try:
                        self.ssh_obj.exec_command(
                            node=client,
                            command=f"bash -lc 'nvme disconnect -n "
                                    f"{nqn} 2>/dev/null || true'",
                        )
                        disconnected_nqns.add(nqn)
                    except Exception:
                        pass
            sleep_n_sec(5)

        # Delete K8s resources
        ns = self.k8s_utils.namespace
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete jobs -n {ns} -l app=fio "
                f"--wait=false --ignore-not-found 2>/dev/null || true"
            )
        except Exception:
            pass
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete configmaps -n {ns} -l app=fio "
                f"--wait=false --ignore-not-found 2>/dev/null || true"
            )
        except Exception:
            pass
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pvc -n {ns} --all "
                f"--wait=false --ignore-not-found 2>/dev/null || true"
            )
        except Exception:
            pass
        sleep_n_sec(10)

        # Safety net: sbcli cleanup
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception:
            pass

        self.logger.info("[cleanup] K8s cleanup complete")

    # ── Batch parallel helper ────────────────────────────────────────────────

    def _batch_exec_k8s(self, items, task_fn, op_name: str):
        """Execute task_fn(item) for each item using ThreadPoolExecutor."""
        total = len(items)
        success = 0
        failures = 0

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for batch_start in range(0, total, self.BATCH_SIZE):
                batch = items[batch_start:batch_start + self.BATCH_SIZE]
                futures = {}
                for item in batch:
                    f = executor.submit(task_fn, item)
                    futures[f] = item

                for f in as_completed(futures):
                    try:
                        f.result(timeout=600)
                        success += 1
                    except Exception as exc:
                        failures += 1
                        self.logger.error(f"[{op_name}] Failed: {exc}")

                done = batch_start + len(batch)
                self.logger.info(
                    f"[{op_name}] progress: {done}/{total} "
                    f"(ok={success} fail={failures})"
                )

        return success, failures
