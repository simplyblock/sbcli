"""
K8sUtils: Kubernetes-specific helper for simplyblock stress/e2e tests.

All sbcli CLI commands are routed through kubectl exec into the
simplyblock-admin-control pod (running on the K3s master node).

    runner → SSH to K3s master → kubectl exec -n simplyblock <admin-pod> -- bash -c '<cmd>'

Container-crash simulation replaces docker stop with kubectl delete pod:

    runner → SSH to K3s master → kubectl delete pod snode-spdk-pod-<x> -n simplyblock

Network outage (interface block/unblock) still uses SSH directly to the
storage-node host via the underlying SshUtils instance — same as bare-metal.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import time
import uuid
from datetime import UTC, datetime

from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


class ControlPlaneUnreachable(RuntimeError):
    """The API server, or the kubelet behind the admin pod, could not be reached.

    Deliberately not the same thing as a command that ran and failed. When
    this is raised nothing has been learned about the cluster under test, so
    a caller must not fold it into a result.
    """


#: stderr fragments meaning "we could not ask", as opposed to "we asked and
#: the answer was empty". Run 41058481784 died on those two being conflated:
#: the lab's only DNS server and default gateway (10.0.0.1, which is also what
#: api-int.ocp.simplyblock.ai resolves to) went away for five minutes, every
#: worker lost the API server, a `kubectl exec` came back with empty stdout,
#: and `_json_or_empty` turned that into `[]` -- which a storage-node shutdown
#: helper then indexed. None of these strings mean the control plane answered.
_CONTROL_PLANE_UNREACHABLE_MARKERS = (
    "error dialing backend",
    "i/o timeout",
    "no route to host",
    "http2: client connection lost",
    "unexpected EOF",
    "TLS handshake timeout",
    "etcdserver: request timed out",
    "the server was unable to return a response",
    "an error on the server",
    "Unable to connect to the server",
    "connection refused",
    "context deadline exceeded",
    "subprocess timed out after",
)


def _is_control_plane_unreachable(stderr: str) -> bool:
    """True when *stderr* says the API server or a kubelet could not be reached."""
    return any(m in (stderr or "") for m in _CONTROL_PLANE_UNREACHABLE_MARKERS)

#: How a v1alpha1 StoragePool's storageClassParameters key spells itself under
#: v1alpha2's typed volumeDefaults. storageClassParameters was a free-form map;
#: volumeDefaults is a struct, so a key with no field here is an error rather
#: than something to pass through and have the apiserver prune.
#:
#: "encryption" is deliberately absent. v1alpha2 has no encryption field on
#: StoragePool at all -- spec is allowedNodes/clusterRef/limits/volumeDefaults,
#: and volumeDefaults has no such key, so asking for one is rejected outright:
#:   strict decoding error: unknown field
#:   "spec.volumeDefaults.enableEncryption"
#: Encryption is a StorageClass setting again, bound to a pool by the
#: storage.simplyblock.io/{namespace,cluster,pool} labels. See
#: create_storage_class.
_SCP_TO_VOLUME_DEFAULTS = {
    "compression": "enableCompression",
    "filesystem": "filesystem",
    "csi.storage.k8s.io/fstype": "filesystem",
}

#: The fields volumeDefaults actually has, for the error message.
_VOLUME_DEFAULT_FIELDS = (
    "iops, throughput, filesystem, enableCompression, enableClientCompression, "
    "enableClientDeduplication, enableReplication, "
    "enableDHCHAP, priorityClass, fabric, maxNamespacesPerSubsystem, "
    "tune2fsReservedBlocks"
)


def pool_volume_defaults(dhchap=False, storage_class_parameters=None):
    """The volumeDefaults a pool request maps to, as {field: value}.

    Shared by the writer and by the reuse check that compares an existing
    StoragePool against what a caller asked for. Those two drifting is how a
    pool gets reused with the wrong StorageClass: the comparison read
    spec.storageClassParameters and spec.dhchap, neither of which v1alpha2 has,
    so it compared two empty dicts and matched any pool at all.
    """
    out = {}
    if dhchap:
        out["enableDHCHAP"] = True
    for key, value in (storage_class_parameters or {}).items():
        field = _SCP_TO_VOLUME_DEFAULTS.get(key)
        if field is None:
            raise ValueError(
                f"storage_class_parameters key '{key}' has no v1alpha2 "
                f"equivalent; volumeDefaults takes {_VOLUME_DEFAULT_FIELDS}")
        out[field] = value
    return out


def _as_yaml_scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class K8sUtils:
    """
    Kubernetes-aware command executor and failover helper.

    Parameters
    ----------
    ssh_obj : SshUtils
        An already-connected SshUtils instance.  kubectl commands are issued
        by SSH-ing to ``mgmt_node`` and running kubectl there.
    mgmt_node : str
        IP of the K3s master node (= first entry of MNODES / K3S_MNODES).
        kubectl must be available and configured on this host.
    namespace : str
        Kubernetes namespace where simplyblock is deployed (default: "simplyblock").
    """

    def __init__(self, ssh_obj, mgmt_node: str, namespace: str = "simplyblock"):
        self.ssh_obj = ssh_obj
        self.mgmt_node = mgmt_node
        self.namespace = namespace
        self._admin_pod: str | None = None
        self.logger = setup_logger(__name__)
        # Use local subprocess when K8S_LOCAL_KUBECTL=1 is set explicitly,
        # or when the runner is on the mgmt node (bastion == mgmt_node) AND
        # this is a k8s deployment (ssh_obj has no real bastion to proxy through).
        _bastion = getattr(ssh_obj, "bastion_server", None)
        _local_env = os.environ.get("K8S_LOCAL_KUBECTL", "").lower() in ("1", "true", "yes")
        _same_as_bastion = bool(_bastion) and mgmt_node == _bastion
        self.use_local_kubectl = _local_env or _same_as_bastion
        if self.use_local_kubectl:
            self.logger.info("[K8sUtils] Local kubectl mode enabled (subprocess)")
        #: Control-plane outage bookkeeping, read by control_plane_downtime().
        #: An outage is open from the first transport failure until the next
        #: call that gets through.
        self._cp_outage_started: float | None = None
        self._cp_downtime_total: float = 0.0
        #: How long exec_sbcli waits out an unreachable control plane before
        #: giving up. Comfortably longer than any blip measured here (the
        #: worst was five minutes) and still bounded, so a genuinely dead
        #: cluster fails the run instead of hanging it.
        self.cp_unreachable_budget: float = float(
            os.environ.get("SB_CP_UNREACHABLE_BUDGET", "900"))

    # ── kubectl dispatch ─────────────────────────────────────────────────────

    def _exec_kubectl(self, cmd: str, supress_logs: bool = False,
                      timeout: int = 300):
        """
        Execute *cmd* either locally via subprocess (when use_local_kubectl=True)
        or via SSH to mgmt_node.  Returns (stdout, stderr) strings.

        *timeout* caps subprocess execution (default 300s / 5 min).
        """
        if self.use_local_kubectl:
            if not supress_logs:
                self.logger.info(f"[K8sUtils] local: {cmd}")
            try:
                result = subprocess.run(
                    ["bash", "-c", cmd],
                    capture_output=True, text=True,
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                msg = f"[K8sUtils] subprocess timed out after {timeout}s: {cmd[:120]}"
                if not supress_logs:
                    self.logger.warning(msg)
                return "", msg
            if not supress_logs:
                if result.stdout.strip():
                    self.logger.info(f"[K8sUtils] stdout: {result.stdout.strip()}")
                if result.stderr.strip():
                    self.logger.info(f"[K8sUtils] stderr: {result.stderr.strip()}")
            return result.stdout, result.stderr
        return self.ssh_obj.exec_command(self.mgmt_node, cmd, supress_logs=supress_logs)

    # ── Admin pod discovery ──────────────────────────────────────────────────

    def get_admin_pod(self, refresh: bool = False,
                      avoid: str | None = None) -> str:
        """
        Return a **Ready** simplyblock-admin-control-* pod.

        The result is cached after the first successful call. Pass
        ``refresh=True`` to force a fresh lookup (e.g. after a restart), and
        *avoid* to name the pod we have just failed against.

        This used to be ``| head -1``, which picks by name order and knows
        nothing about whether the pod can serve. The deployment runs more than
        one replica precisely so that one being unusable is survivable, and
        sorting threw that away: run 41058481784 sent every sbctl call of its
        last half hour at one pod on an unreachable node, and never tried the
        other, which was Ready throughout.

        custom-columns rather than jsonpath on purpose -- jsonpath's range and
        newline syntax does not survive being embedded in a Python string and
        passed through ``bash -c``.
        """
        if self._admin_pod and not refresh:
            return self._admin_pod

        cols = (
            "custom-columns=NAME:.metadata.name,"
            "PHASE:.status.phase,"
            "READY:.status.conditions[?(@.type=='Ready')].status"
        )
        out, _ = self._exec_kubectl(
            f'kubectl get pods -n {self.namespace} --no-headers -o "{cols}" '
            f"| grep simplyblock-admin-control",
            supress_logs=True,
        )
        ready, present = [], []
        for line in (out or "").splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            name, phase, is_ready = parts[0], parts[1], parts[2]
            present.append(name)
            if phase == "Running" and is_ready == "True":
                ready.append(name)

        pool = ready or present
        pod = next((p for p in pool if p != avoid), pool[0] if pool else "")
        if not pod:
            raise RuntimeError(
                f"[K8sUtils] No simplyblock-admin-control pod found in namespace '{self.namespace}'"
            )
        if not ready:
            self.logger.warning(
                "[K8sUtils] no Ready admin pod; falling back to %s", pod)
        self._admin_pod = pod
        self.logger.info(f"[K8sUtils] Admin pod resolved: {pod}")
        return pod

    # -- control-plane reachability ------------------------------------------

    def _note_control_plane_down(self, when: float) -> None:
        if self._cp_outage_started is None:
            self._cp_outage_started = when

    def _note_control_plane_up(self) -> None:
        if self._cp_outage_started is None:
            return
        elapsed = time.time() - self._cp_outage_started
        self._cp_downtime_total += elapsed
        self._cp_outage_started = None
        self.logger.info(
            "[K8sUtils] control plane reachable again after %.0fs "
            "(%.0fs total this run)", elapsed, self._cp_downtime_total)

    def control_plane_downtime(self) -> float:
        """Seconds this run has spent unable to reach the control plane.

        Outage helpers subtract this from their own deadlines. Five minutes of
        dead API server must not consume a five-minute node-shutdown budget,
        which is how run 41058481784 would have failed even with the IndexError
        fixed: the control plane came back 45 seconds after the deadline.
        """
        total = self._cp_downtime_total
        if self._cp_outage_started is not None:
            total += time.time() - self._cp_outage_started
        return total

    # ── sbcli command execution ──────────────────────────────────────────────

    def exec_sbcli(self, command: str, supress_logs: bool = False):
        """
        Execute *command* inside the simplyblock-admin-control pod via kubectl exec.

        Two failure modes are handled, and they are emphatically not the same:

        * **the admin pod is gone** (rollout, eviction, node drain) -- the pod
          name is re-resolved and the command retried. kubectl spells this
          several ways depending on the phase of termination: ``NotFound``,
          ``unable to upgrade connection: pod does not exist``,
          ``pod ... not found``.

        * **the control plane could not be reached** -- the API server is
          down, or it cannot dial the kubelet hosting the pod. Nothing has
          been learned about the cluster under test, so this is waited out
          rather than reported. If it outlasts ``cp_unreachable_budget`` the
          call raises :class:`ControlPlaneUnreachable`; it never returns an
          empty result that a caller would read as data.

        That second branch is the whole point. Run 41058481784 lost 9h22m of
        passing rapid-failover to its absence: the lab's single DNS server and
        gateway at 10.0.0.1 went away for five minutes, every worker lost
        api-int, this method returned ``("", "error dialing backend: ...")``,
        `_json_or_empty` turned the blank stdout into ``[]``, and
        `_graceful_shutdown_node` indexed ``[0]`` on it.

        Returns the same (stdout, stderr) tuple as SshUtils.exec_command.
        """
        if not supress_logs:
            self.logger.info(f"[sbcli] {command}")

        admin_pod = self.get_admin_pod()
        deadline: float | None = None   # armed by the first transport failure
        attempt = 0

        while True:
            attempt += 1
            kubectl_cmd = (
                f"kubectl exec -n {self.namespace} {admin_pod} -- "
                f"bash -c {shlex.quote(command)}"
            )
            stdout, stderr = self._exec_kubectl(kubectl_cmd,
                                                supress_logs=supress_logs)
            _err = stderr or ""

            _pod_gone = (
                "NotFound" in _err
                or "pod does not exist" in _err
                or "pod not found" in _err.lower()
            )
            if _pod_gone:
                self.logger.warning(
                    f"[K8sUtils] Admin pod '{admin_pod}' gone "
                    f"({_err.strip()[:80]}), re-resolving..."
                )
                admin_pod = self.get_admin_pod(refresh=True, avoid=admin_pod)
                continue

            if not _is_control_plane_unreachable(_err):
                self._note_control_plane_up()
                return stdout, stderr

            now = time.time()
            if deadline is None:
                deadline = now + self.cp_unreachable_budget
                self._note_control_plane_down(now)
                self.logger.warning(
                    "[K8sUtils] control plane unreachable via %s: %s -- "
                    "waiting up to %.0fs for it to come back",
                    admin_pod, _err.strip()[:160], self.cp_unreachable_budget)
            if now >= deadline:
                self._note_control_plane_up()
                raise ControlPlaneUnreachable(
                    f"control plane unreachable for "
                    f"{self.cp_unreachable_budget:.0f}s while running "
                    f"{command!r}: {_err.strip()[:300]}"
                )

            # Re-resolve before sleeping: the pod's own kubelet may be the
            # unreachable part, in which case another replica answers at once.
            try:
                admin_pod = self.get_admin_pod(refresh=True, avoid=admin_pod)
            except RuntimeError:
                pass        # listing pods needs the same API server
            sleep_n_sec(min(15, 2 ** min(attempt, 4)))

    # ── K8s node name resolution ─────────────────────────────────────────────

    def _get_k8s_node_name(self, node_ip: str) -> str:
        """Return the K8s node name (hostname) for a given storage-node IP."""
        out, _ = self._exec_kubectl(
            (
                "kubectl get nodes -o wide --no-headers "
                f"| awk '{{print $1, $6}}' | grep '{node_ip}' | awk '{{print $1}}'"
            ),
            supress_logs=True,
        )
        name = out.strip()
        if not name:
            raise RuntimeError(
                f"[K8sUtils] Cannot resolve K8s node name for IP {node_ip!r}"
            )
        return name

    # ── host access ───────────────────────────────────────────────────
    #: How long to let a node-level command run before giving up.
    NODE_CMD_TIMEOUT = 180

    def run_on_node(self, node_ip: str, host_cmd: str,
                    timeout: int | None = None, check: bool = True):
        """Run *host_cmd* in the host namespace of the node owning *node_ip*.

        One entry point for the three platforms the suite runs on:

        * **OpenShift** -- ``oc debug node/<n> -- chroot /host``
        * **Talos** -- no host shell exists at all, so this raises. Talos is
          API-managed; callers that need a host action must go through
          ``talosctl`` (see :meth:`reboot_node`) or accept a substitute and say
          so. Returning a silent no-op here would make every Talos run look
          like it exercised something it did not.
        * **everything else** -- ``kubectl debug node/<n>`` with a chroot

        Returns ``(stdout, stderr)``. With ``check=False`` a failing command
        returns its output instead of raising, which is what the reboot and
        isolation paths want: the connection dies *because the command
        worked*.
        """
        import shlex as _shlex

        node_name = self._get_k8s_node_name(node_ip)
        quoted = _shlex.quote(host_cmd)

        if self.detect_talos():
            raise RuntimeError(
                f"[K8sUtils] Talos has no host shell, so run_on_node cannot "
                f"execute {host_cmd!r} on {node_ip}. Use talosctl, or use a "
                f"substitute and record that the host action did not happen.")

        if self.detect_openshift():
            cmd = (f"oc debug node/{node_name} --quiet=true -- "
                   f"chroot /host bash -c {quoted}")
        else:
            cmd = (f"kubectl debug node/{node_name} -it --quiet=true "
                   f"--image=busybox:latest -- chroot /host sh -c {quoted}")

        try:
            return self._exec_kubectl(
                cmd, timeout=timeout or self.NODE_CMD_TIMEOUT)
        except Exception as exc:                      # noqa: BLE001
            if check:
                raise
            self.logger.info(
                "[K8sUtils] node command on %s returned an error, continuing "
                "as asked (check=False): %s", node_ip, str(exc)[:160])
            return "", str(exc)

    def cordon_node(self, node_ip: str, cordon: bool = True) -> bool:
        """Mark the node unschedulable (or schedulable again)."""
        node_name = self._get_k8s_node_name(node_ip)
        verb = "cordon" if cordon else "uncordon"
        cli = "oc adm" if self.detect_openshift() else "kubectl"
        out, err = self._exec_kubectl(
            f"{cli} {verb} {node_name} 2>&1 || true", timeout=120)
        self.logger.info("[K8sUtils] %s %s: %s", verb, node_name,
                         (out or err or "").strip()[:160])
        return True

    def uncordon_all_workers(self) -> list:
        """Uncordon every worker left SchedulingDisabled. Returns their names.

        A cordon is state on shared infrastructure that outlives the process
        that set it. reboot_node cordons before draining and only uncordons
        on the recovery path, so a run that fails in between leaves the node
        unschedulable -- and the operator then declines it outright
        ("is not used: it is cordoned"), which is a bring-up failure for the
        NEXT run, with a message about stripe width rather than about a
        cordon.

        Masters are left alone: they are cordoned on purpose on some
        clusters, and no storage node lives there.
        """
        out, _ = self._exec_kubectl(
            "kubectl get nodes -o "
            "jsonpath='{range .items[?(@.spec.unschedulable==true)]}"
            "{.metadata.name}{\"\\n\"}{end}' 2>/dev/null || true",
            supress_logs=True)
        names = [n.strip() for n in (out or "").splitlines()
                 if n.strip() and "master" not in n]
        if not names:
            return []
        cli = "oc adm" if self.detect_openshift() else "kubectl"
        for name in names:
            self.logger.warning(
                "[K8sUtils] %s was left cordoned; uncordoning it", name)
            self._exec_kubectl(f"{cli} uncordon {name} 2>&1 || true",
                               timeout=120)
        return names

    def drain_node(self, node_ip: str, timeout_min: int = 10) -> bool:
        """Evacuate the node's pods before it goes down.

        `--ignore-daemonsets` is not optional: the SPDK pod is a DaemonSet
        pod, and drain refuses to run at all if it would have to evict one.
        It is also the honest behaviour -- DaemonSet pods are not evacuated,
        they go down with the node and come back with it, which is what we
        want to observe.

        The Job-backed FIO pods DO get evicted and rescheduled elsewhere, and
        their RWO volumes have to follow them. That is the part worth
        watching, and what the attach check afterwards is for.

        Never fatal. A drain that times out because something will not
        evict is a finding to record, not a reason to abandon the cycle --
        the reboot still tells us what we came for.
        """
        node_name = self._get_k8s_node_name(node_ip)
        cli = "oc adm" if self.detect_openshift() else "kubectl"
        self.logger.info("[K8sUtils] draining %s (up to %dm)",
                         node_name, timeout_min)
        out, err = self._exec_kubectl(
            f"{cli} drain {node_name} --ignore-daemonsets "
            f"--delete-emptydir-data --force --timeout={timeout_min}m 2>&1 "
            f"|| true",
            timeout=timeout_min * 60 + 120)
        text = (out or "") + (err or "")
        # Outcome first, then symptoms. `oc adm drain` retries an eviction
        # that a PodDisruptionBudget refuses and says so each time --
        # "error when evicting pods/... Cannot evict pod as it would violate
        # the pod's disruption budget" -- and then succeeds. Matching the
        # word "error" anywhere in the output called that a failed drain,
        # which it was not: the last line said "node/<n> drained".
        if f"node/{node_name} drained" in text:
            self.logger.info("[K8sUtils] drained %s", node_name)
            return True
        if "error" in text.lower() or "timed out" in text.lower():
            self.logger.warning(
                "[K8sUtils] drain of %s did not complete cleanly: %s",
                node_name, text.strip()[-400:])
            return False
        self.logger.info("[K8sUtils] drained %s", node_name)
        return True

    def wait_node_volumes_detached(self, node_ip: str,
                                   timeout: int = 300) -> bool:
        """Block until no VolumeAttachment is still bound to this node.

        A drain returns when the pods are deleted. The volumes have not moved
        at that point: kubelet on THIS node still has to run
        NodeUnstageVolume, the attach/detach controller then calls
        ControllerUnpublishVolume, and only then does the VolumeAttachment go
        and the claim become attachable somewhere else.

        Rebooting in between kills the kubelet that owes step one, so nothing
        in the cluster can finish the detach until the node returns, and the
        pod the drain relocated reports "Multi-Attach error" for as long as
        that takes. Waiting here costs seconds in the normal case and saves
        the cycle in the bad one.

        Returns False on timeout; the caller decides. Deliberately does not
        force-delete the attachments or taint the node --
        `node.kubernetes.io/out-of-service` is the sanctioned way to force a
        detach, but applying it would change what the cycle asserts about the
        product's own recovery.
        """
        node_name = self._get_k8s_node_name(node_ip)
        if not node_name:
            return True
        deadline = time.time() + timeout
        held = []
        while time.time() < deadline:
            held = [a for a in self.get_volume_attachments()
                    if a.get("node") == node_name and a.get("attached")]
            if not held:
                self.logger.info(
                    "[K8sUtils] %s holds no VolumeAttachment; safe to reboot",
                    node_name)
                return True
            time.sleep(5)
        self.logger.warning(
            "[K8sUtils] %s still holds %d VolumeAttachment(s) after %ds: %s. "
            "Rebooting anyway -- the cycle needs the outage -- but a pod the "
            "drain moved elsewhere will report Multi-Attach until this node "
            "is back, and this line is the reason.",
            node_name, len(held), timeout, [a.get("pv") for a in held[:5]])
        return False

    def reboot_node(self, node_ip: str, drain: bool = True) -> bool:
        """Actually reboot the worker. Returns True if a real reboot was issued.

        With *drain* (the default) this is the documented OpenShift node
        maintenance sequence -- cordon, drain, reboot -- and the caller
        uncordons once the node is Ready again. That is a PLANNED maintenance
        reboot: pods are evacuated before the node goes, which is what an
        operator does to patch a node, and it is a different thing from a node
        vanishing. The unplanned case is already covered elsewhere, by
        container_stop and node_network_isolation, so this does not duplicate
        it.

        Pass drain=False for an undrained reboot, where the node goes with its
        pods still on it.

        Synchronous, and the result is not trusted either way: the reboot
        kills the connection carrying it, so succeeding looks like failing.
        The caller decides the node went down by watching it go NotReady,
        which is the only honest evidence.

        Talos: `talosctl reboot` if the binary is on the runner, otherwise
        False so the caller can fall back and SAY it fell back.
        """
        node_name = self._get_k8s_node_name(node_ip)

        if self.detect_talos():
            # Talos gets the SAME sequence -- cordon, drain, reboot -- because
            # it is a property of Kubernetes, not of the host OS. Only the
            # reboot verb differs, and only because Talos has no host shell to
            # send one to: it is API-managed, so the reboot goes through
            # talosctl instead of a debug pod. cordon_node and drain_node are
            # already platform-aware and fall to plain kubectl here, since
            # detect_openshift() is False.
            #
            # Cordon before checking for talosctl would leave the node
            # unschedulable on a runner that cannot reboot it, so the
            # capability check comes first.
            probe, _ = self._exec_kubectl(
                "command -v talosctl >/dev/null 2>&1 && echo HAVE_TALOSCTL "
                "|| echo NO_TALOSCTL", supress_logs=True, timeout=60)
            if "HAVE_TALOSCTL" not in (probe or ""):
                self.logger.warning(
                    "[K8sUtils] Talos node %s: talosctl not available on the "
                    "runner, cannot issue a real reboot", node_ip)
                return False

            if drain:
                self.cordon_node(node_ip, cordon=True)
                self.drain_node(node_ip)

            out, err = self._exec_kubectl(
                f"talosctl -n {node_ip} reboot 2>&1 || true", timeout=180)
            self.logger.info("[K8sUtils] talosctl reboot issued for %s: %s",
                             node_ip, (out or err or "").strip()[:160])
            return True

        if drain:
            # cordon -> drain -> reboot, the documented OpenShift node
            # maintenance sequence. Cordon first so nothing is scheduled onto
            # a node that is about to go; drain so the workload leaves under
            # its own eviction rules rather than being cut off.
            self.cordon_node(node_ip, cordon=True)
            self.drain_node(node_ip)
            # The drain moved the pods. It did not move their volumes, and
            # the kubelet that has to unstage them is on the node we are
            # about to reboot.
            self.wait_node_volumes_detached(node_ip)

        self.logger.info("[K8sUtils] rebooting node %s (%s)",
                         node_name, node_ip)
        # Synchronously, and plain `reboot` -- the same thing SshUtils does on
        # docker ("`sudo reboot` never returns an exit status: the host is
        # gone before it can be sent").
        #
        # It was backgrounded before, and that was the bug: `oc debug` DELETES
        # its debug pod the moment the command returns, so a backgrounded
        # `sleep 3; reboot` is a child of a pod that is already gone and never
        # runs. On 2026-09-23 the command was issued cleanly, took 48s, and
        # worker-1 stayed Ready. Run in the foreground it works -- confirmed by
        # hand on this cluster, where the node went NotReady immediately.
        #
        # check=False because succeeding looks like failing here: the reboot
        # kills the connection carrying it. The caller decides whether it
        # worked by watching for NotReady, which is the only honest evidence.
        #
        # `systemctl reboot`, not the `reboot` shim. On systemd they are the
        # same thing, but the shim's FLAGS differ between distros and one of
        # them is a trap: the hand-run that proved this route used `reboot -h`,
        # and -h means --halt. worker-1 went down and did not come back.
        # systemctl's verb cannot be read as anything but a reboot, which
        # matters for a command whose failure mode is a node that never
        # returns and a run that waits for it.
        #
        # Not `--force` either: that skips the clean shutdown, which is a
        # harsher fault than this outage is meant to be. A storage node
        # rebooting cleanly is the case under test; killing it outright is
        # what container_stop already does.
        self.run_on_node(node_ip, "systemctl reboot", timeout=60, check=False)
        return True

    def wait_node_condition(self, node_ip: str, ready: bool,
                            timeout: int = 600, poll: int = 10) -> bool:
        """Wait until the node's Ready condition is *ready*. True if it got there."""
        import time as _time

        node_name = self._get_k8s_node_name(node_ip)
        deadline = _time.time() + timeout
        want = "True" if ready else "False"
        while _time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get node {node_name} "
                f"-o jsonpath='{{.status.conditions[?(@.type==\"Ready\")].status}}'",
                supress_logs=True, timeout=60)
            got = (out or "").strip().strip("'")
            # A node that has gone away entirely reads as neither.
            if got == want or (not ready and got in ("Unknown", "")):
                self.logger.info("[K8sUtils] node %s Ready=%s", node_name, got)
                return True
            _time.sleep(poll)
        self.logger.warning(
            "[K8sUtils] node %s did not reach Ready=%s within %ds",
            node_name, want, timeout)
        return False

    # ── storage-network cut (shared by every k8s outage test) ─────────
    #
    # One implementation for lblk, continuous_k8s_native_failover,
    # continuous_failover_ha_k8s and the security suite. They had four
    # copies; three of them blanket-DROPped everything on the node, which
    # takes out OVN, FoundationDB and the cluster's own health monitor along
    # with the storage path, and then "restored" with `iptables -F`.

    def peer_data_ips(self, storage_nodes, node_ip):
        """Peers' DATA-network addresses, given the storage-node list.

        *storage_nodes* is the list of dicts the control plane returns.

        The data NIC is what a storage outage should cut. Management carries
        spdk-proxy, the SNodeAPI, OVN geneve, FoundationDB, the kubelet and
        simplyblock-monitoring; cutting it means that when the monitor is
        scheduled on the node under test, our own rule starves it of its
        peers and it demotes all of them. That produced a cluster collapse
        twice, which was written up as a product defect before dev spotted
        the co-location.

        Falls back to mgmt_ip when a node has no data NIC recorded, with a
        warning -- silently falling back would reintroduce exactly that bug.
        """
        peers = []
        for n in storage_nodes or []:
            if n.get("mgmt_ip") == node_ip:
                continue
            ip = ""
            for nic in (n.get("data_nics") or []):
                ip = nic.get("ip4_address") or ""
                if ip:
                    break
            if not ip:
                ip = n.get("mgmt_ip") or ""
                if ip:
                    self.logger.warning(
                        "[K8sUtils] storage node %s has no data NIC address; "
                        "falling back to its mgmt IP for the cut. That is the "
                        "network simplyblock-monitoring uses, so this cut may "
                        "demote peers that are perfectly healthy.", ip)
            if ip:
                peers.append(ip)
        return peers

    @staticmethod
    def _peer_drop_add(peers):
        return "; ".join(
            f"iptables -A INPUT -s {p} -j DROP; "
            f"iptables -A OUTPUT -d {p} -j DROP" for p in peers)

    @staticmethod
    def _peer_drop_undo(peers):
        """Delete exactly what we added. Never `iptables -F`.

        -D matches the whole rule, so this has to mirror the add. Three
        passes because a retried cycle can add a rule more than once.
        """
        return "; ".join(
            f"for i in 1 2 3; do "
            f"iptables -D INPUT -s {p} -j DROP 2>/dev/null; "
            f"iptables -D OUTPUT -d {p} -j DROP 2>/dev/null; done"
            for p in peers) + "; true"

    #: Where the host-armed undo records that it fired.
    NET_UNDO_MARKER = "/tmp/sb_net_undo.stamp"

    def arm_host_net_undo(self, node_ip, peers, delay):
        """Schedule the undo on the HOST, to fire in *delay* seconds.

        Returns True when it is armed somewhere that outlives the SPDK pod.

        This is the part the old copies got wrong. They scheduled it with
        `nsenter --target 1` from inside the SPDK pod, under a comment saying
        it survives the container dying. The pod is hostNetwork but not
        hostPID, so PID 1 there is the container's own and the timer dies
        with it. run_on_node goes in through `oc debug node/` instead, which
        chroots /host and gets real systemd.

        The SELinux relabel is not optional: a script written to /tmp is
        user_tmp_t, which init_t may not execute, and systemd reports that as
        "Failed to locate executable", which reads like a missing file.
        """
        return self.arm_host_undo_script(
            node_ip, self._peer_drop_undo(peers), delay, tag="netundo")

    def arm_host_undo_script(self, node_ip, inner, delay, tag="undo"):
        """Run *inner* on the host after *delay* seconds, outliving the pod.

        The generic form of :meth:`arm_host_net_undo`, so the interface-
        scoped NIC outage can use the same mechanism rather than keeping its
        own `nsenter --target 1` version, which never reached the host.

        Returns True when systemd (or at worst setsid) took it.
        """
        import random as _random
        import time as _time
        unit = f"sb-{tag}-{int(_time.time())}-{_random.getrandbits(16):04x}"
        undo = inner
        script = (
            f"set -e; "
            f"cat > /tmp/sb_{tag}_undo.sh <<'EOS'\n"
            f"#!/bin/sh\n"
            f"sleep {delay}\n"
            f"{undo}\n"
            f"date -u +'net-undo %Y-%m-%dT%H:%M:%SZ' >> {self.NET_UNDO_MARKER}\n"
            f"EOS\n"
            f"chmod +x /tmp/sb_{tag}_undo.sh; "
            f"chcon -t bin_t /tmp/sb_{tag}_undo.sh 2>/dev/null || true; "
            f"if command -v systemd-run >/dev/null 2>&1 && "
            f"systemd-run --collect --unit={unit} /tmp/sb_{tag}_undo.sh; "
            f"then echo armed-via-systemd; else "
            f"echo armed-via-setsid; "
            f"setsid nohup /tmp/sb_{tag}_undo.sh </dev/null >/dev/null 2>&1 & "
            f"fi")
        try:
            out, _err = self.run_on_node(node_ip, script, timeout=120,
                                         check=False)
        except Exception as exc:                      # noqa: BLE001
            # Talos raises by design: it has no host shell.
            self.logger.warning(
                "[K8sUtils] could not arm the undo on the host for %s: %s",
                node_ip, str(exc)[:160])
            return False
        if "armed-via" not in (out or ""):
            self.logger.warning(
                "[K8sUtils] host undo for %s did not confirm it armed; said: "
                "%s", node_ip, (out or "<nothing>").strip()[:200])
            return False
        return True

    def cut_storage_network(self, node_ip, peers, duration):
        """Cut *node_ip* off its peers' storage network for *duration*.

        Verifies the rules landed -- a silent no-op here is indistinguishable
        from a successful isolation and produces a test that proves nothing.
        Raises RuntimeError if they did not, after undoing whatever did.
        """
        import shlex as _shlex
        if not peers:
            raise RuntimeError(
                f"[K8sUtils] no peer storage nodes to isolate {node_ip} from")
        add = self._peer_drop_add(peers)
        self.exec_in_spdk_container(node_ip, f"sudo sh -c {_shlex.quote(add)}")
        show = "iptables -S INPUT; iptables -S OUTPUT"
        out, _err = self.exec_in_spdk_container(
            node_ip, f"sudo sh -c {_shlex.quote(show)}")
        applied = sum(1 for p in peers if f"-s {p}/32" in (out or "")
                      or f"-d {p}/32" in (out or ""))
        if applied < len(peers):
            self.restore_storage_network(node_ip, peers)
            raise RuntimeError(
                f"[K8sUtils] iptables did not take on {node_ip}: wanted "
                f"{len(peers)} peers blocked, saw {applied}. Rules undone. "
                f"iptables -S said: {(out or '<nothing>')[:300]}")
        mgmt_hit = [ln for ln in (out or "").splitlines()
                    if "-j DROP" in ln and "192.168." in ln]
        if mgmt_hit:
            self.logger.warning(
                "[K8sUtils] a DROP rule on %s is on the MANAGEMENT network: "
                "%s. That is the network simplyblock-monitoring uses; if the "
                "monitor is on this node it will report its peers as failed.",
                node_ip, mgmt_hit[:2])
        on_host = self.arm_host_net_undo(node_ip, peers, duration + 30)
        if not on_host:
            undo = self._peer_drop_undo(peers)
            self.exec_in_spdk_container(node_ip, (
                f"sudo sh -c {_shlex.quote(f'(sleep {duration + 30}; {undo}) >/dev/null 2>&1 &')}"))
            self.logger.warning(
                "[K8sUtils] backstop for %s is in the SPDK pod, not on the "
                "host. If this cut takes the node offline the pod goes with "
                "it and the cut will not be undone.", node_ip)
        self.logger.info(
            "[K8sUtils] %s cut off from %d peer(s) %s for %ds; undo armed %s",
            node_ip, len(peers), ",".join(peers), duration,
            "on the host" if on_host else "in the pod (fallback)")
        return duration

    def restore_storage_network(self, node_ip, peers):
        """Remove the peer DROP rules. Safe to call twice. Never raises.

        Pod first because it is cheap and is all Talos has; host second
        because a cut long enough to take the node offline removes the pod,
        and that is the case that matters -- it is what ended run
        20261002-202420 with "No snode-spdk-pod found" as its verdict on a
        cycle that was testing a network cut.
        """
        import shlex as _shlex
        if not peers:
            return True
        undo = self._peer_drop_undo(peers)
        show = "iptables -S INPUT; iptables -S OUTPUT"
        out = None
        try:
            self.exec_in_spdk_container(node_ip, f"sudo sh -c {_shlex.quote(undo)}")
            out, _err = self.exec_in_spdk_container(
                node_ip, f"sudo sh -c {_shlex.quote(show)}")
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning(
                "[K8sUtils] cannot restore %s through its SPDK pod (%s); "
                "going in through the host instead", node_ip, str(exc)[:140])
            try:
                self.run_on_node(node_ip, undo, timeout=120, check=False)
                out, _err = self.run_on_node(node_ip, show, timeout=120,
                                             check=False)
            except Exception as exc2:                 # noqa: BLE001
                self.logger.error(
                    "[K8sUtils] could not reach %s by either route, so its "
                    "peer DROP rules are probably still in place: %s",
                    node_ip, str(exc2)[:160])
                return False
        left = [ln for ln in (out or "").splitlines() if "-j DROP" in ln]
        if left:
            self.logger.warning(
                "[K8sUtils] DROP rules still on %s after restore: %s",
                node_ip, left[:4])
            return False
        self.logger.info("[K8sUtils] network restored on %s", node_ip)
        return True

    def relocate_monitor_off(self, node_ip, timeout=180):
        """Move simplyblock-monitoring off *node_ip* before isolating it.

        :meth:`isolate_node` cuts EVERYTHING (``! -i lo -j DROP``) and has to:
        the point of that outage is to make the kubelet miss its heartbeats
        so the scheduler evicts the node's pods, and the kubelet is on the
        management network. So unlike the storage-network cut, this one
        cannot be narrowed to spare the control plane.

        Which leaves the other half of the problem. simplyblock-monitoring is
        scheduled onto a storage node, and when the node it lands on is the
        one being isolated, it loses its RPCs to all three peers, concludes
        all three have failed, and demotes them -- the cluster goes degraded
        and then suspended off a single-node outage. That is what run
        20261003-080237 did, and it was written up as a product defect twice
        before dev spotted the co-location.

        Deleting the pod lets the deployment put it on another node, so the
        monitor keeps a clear view of the cluster while one node goes dark.
        That is the behaviour the test means to exercise; a blinded monitor
        is not.

        Returns True when the monitor is known to be elsewhere. Best effort:
        if it cannot be found or moved, the caller is told and decides.
        """
        try:
            out, _err = self._exec_kubectl(
                f"kubectl get pods -n {self.namespace} -o wide --no-headers "
                f"2>/dev/null | grep -i monitoring || true",
                supress_logs=True, timeout=120)
            node = self._get_k8s_node_name(node_ip)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning(
                "[K8sUtils] could not locate simplyblock-monitoring before "
                "isolating %s: %s", node_ip, str(exc)[:140])
            return False

        here = [c.split()[0] for c in (out or "").splitlines()
                if len(c.split()) >= 7 and c.split()[6] == node]
        if not here:
            self.logger.info(
                "[K8sUtils] simplyblock-monitoring is not on %s; isolating "
                "without moving it", node)
            return True

        self.logger.warning(
            "[K8sUtils] simplyblock-monitoring %s is on %s, the node about "
            "to be fully isolated. Moving it first -- left there it would "
            "lose every peer and demote all of them, which collapses the "
            "cluster and tells us nothing about the outage.", here, node)
        for pod in here:
            self._exec_kubectl(
                f"kubectl delete pod {pod} -n {self.namespace} "
                f"--ignore-not-found --wait=false 2>&1 || true", timeout=120)

        deadline = time.time() + timeout
        while time.time() < deadline:
            sleep_n_sec(10)
            try:
                out, _err = self._exec_kubectl(
                    f"kubectl get pods -n {self.namespace} -o wide "
                    f"--no-headers 2>/dev/null | grep -i monitoring || true",
                    supress_logs=True, timeout=120)
            except Exception:                         # noqa: BLE001
                continue
            rows = [c.split() for c in (out or "").splitlines()
                    if len(c.split()) >= 7]
            running = [r for r in rows if r[2] == "Running" and r[6] != node]
            if running and not [r for r in rows if r[6] == node]:
                self.logger.info(
                    "[K8sUtils] simplyblock-monitoring is now on %s",
                    running[0][6])
                return True
        self.logger.warning(
            "[K8sUtils] simplyblock-monitoring did not move off %s within "
            "%ds. Isolating anyway, but if the peers go schedulable during "
            "this outage, that is why.", node, timeout)
        return False

    def isolate_node(self, node_ip: str, duration: int) -> bool:
        """Cut the node off completely for *duration*, restoring itself.

        Total isolation, not just the storage ports: the point is to make the
        kubelet miss its heartbeats so the node goes NotReady and the
        scheduler evicts its pods, which is the behaviour worth testing and
        which a storage-port cut deliberately avoids.

        Self-restoring from the HOST, which is what makes this safe to run at
        all. Nothing can reach the node while the cut holds -- that is the
        point -- so the undo cannot be driven from outside; it is scheduled on
        the node before the cut lands. Two independent restores are armed, a
        timer and a boot-time flush, so a node that reboots mid-cut still
        comes back reachable.

        Handed to systemd rather than backgrounded, because the delivery
        vehicle is ``oc debug node/<n>`` -- an ephemeral pod that is torn down
        the moment the command returns, taking its cgroup and every child
        process with it. A `nohup ... &` script that opens with `sleep 2` is
        killed inside that sleep, before it ever reaches its first iptables
        call: run 20260929-205137 armed this on worker-2, the command returned
        clean, and nothing whatsoever happened for the next four minutes. A
        transient systemd unit is owned by PID 1 instead, so it outlives the
        pod that asked for it.

        The marker file is how a caller tells the two failure shapes apart
        afterwards. "Never armed" is a broken harness; "armed, but the node
        stayed Ready" is a finding about the cluster. Without it they look
        identical from outside, which is exactly how the bug above survived.
        """
        marker = "/tmp/sb_isolate.stamp"
        # Named here, not with $$ in the script. The command crosses two
        # shells before systemd sees it, and $$ did not survive the trip:
        # run 20260930-032853 answered
        #   Invalid unit name "sb-isolate-$" escaped as "sb-isolate-\x24"
        # so every node and every cycle asked for the same unit. --collect
        # cleans one up as it exits, which is the only reason consecutive
        # cycles did not start colliding. A name built in Python cannot be
        # eaten by a shell.
        unit = f"sb-isolate-{uuid.uuid4().hex[:8]}"
        script = (
            # Loopback first, so the scheduling command itself can return and
            # so the node can still talk to itself under the blanket DROP.
            f"set -e; "
            f"iptables -I INPUT 1 -i lo -j ACCEPT; "
            f"iptables -I OUTPUT 1 -o lo -j ACCEPT; "
            f"rm -f {marker}; "
            f"cat > /tmp/sb_isolate.sh <<'EOS'\n"
            f"#!/bin/sh\n"
            f"sleep 2\n"
            f"date -u +'armed %Y-%m-%dT%H:%M:%SZ' >> {marker}\n"
            f"iptables -I INPUT 2 ! -i lo -j DROP\n"
            f"iptables -I OUTPUT 2 ! -o lo -j DROP\n"
            f"sleep {duration}\n"
            f"iptables -D INPUT ! -i lo -j DROP 2>/dev/null || true\n"
            f"iptables -D OUTPUT ! -o lo -j DROP 2>/dev/null || true\n"
            f"date -u +'restored %Y-%m-%dT%H:%M:%SZ' >> {marker}\n"
            f"EOS\n"
            f"chmod +x /tmp/sb_isolate.sh; "
            # SELinux is enforcing on these hosts, and a file written into
            # /tmp is user_tmp_t, which init_t may not execute -- systemd
            # reports that as "Failed to locate executable ...: Permission
            # denied", which reads like the file is missing. Relabelling it
            # bin_t is what makes systemd-run able to start it at all;
            # verified on the lab, where without this the unit dies 203/EXEC.
            f"chcon -t bin_t /tmp/sb_isolate.sh 2>/dev/null || true; "
            # --collect so the unit does not linger in failed state and block
            # the next cycle from reusing the name. The whole thing sits in
            # the `if` CONDITION rather than its body, so that systemd-run
            # being present but unable to reach the host manager falls through
            # to the fallback instead of tripping `set -e` and leaving the
            # node uncut -- which is the failure this whole change is about.
            f"if command -v systemd-run >/dev/null 2>&1 && "
            f"systemd-run --collect --unit={unit} /tmp/sb_isolate.sh; "
            f"then echo armed-via-systemd; else "
            # setsid is the best available on a host without systemd: it at
            # least escapes the session, though not the pod's cgroup.
            f"echo armed-via-setsid; "
            f"setsid nohup /tmp/sb_isolate.sh </dev/null >/dev/null 2>&1 & "
            f"fi"
        )
        self.logger.info(
            "[K8sUtils] isolating %s completely for %ds (self-restoring on "
            "the host)", node_ip, duration)
        self.run_on_node(node_ip, script, timeout=120, check=False)
        return True

    #: Where :meth:`isolate_node` records that its cut actually ran.
    ISOLATION_MARKER = "/tmp/sb_isolate.stamp"

    def isolation_marker(self, node_ip: str) -> str:
        """What the last :meth:`isolate_node` on this node left behind.

        Empty string when the cut never armed.

        Read from the HOST. isolate_node writes this marker through
        run_on_node, which chroots /host, so the file is on the host
        filesystem. This used to read it with `nsenter --target 1 --mount`
        from inside the SPDK pod, which lands in the CONTAINER's mount
        namespace -- the pod is hostNetwork but not hostPID, so PID 1 there
        is its own. It would have returned "" whether or not the cut armed,
        which is the exact failure the marker exists to distinguish.

        Currently unreferenced; fixed rather than deleted because the
        distinction it draws -- "never armed" is a broken harness, "armed
        but nothing happened" is a finding about the cluster -- is worth
        having, and a method that silently reads the wrong filesystem is a
        trap for whoever picks it up next.
        """
        try:
            out, _err = self.run_on_node(
                node_ip, f"cat {self.ISOLATION_MARKER} 2>/dev/null || true",
                timeout=120, check=False)
            return (out or "").strip()
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[K8sUtils] could not read the isolation "
                                "marker on %s: %s", node_ip, str(exc)[:120])
            return ""

    #: Substrings that mean a pod could not get its volume. Matched
    #: case-insensitively against event messages.
    VOLUME_ATTACH_MARKERS = (
        "multi-attach",
        "failedattachvolume",
        "volume is already exclusively attached",
        "failedmount",
    )

    def pods_stuck_on_volumes(self, namespace: str | None = None,
                              within_sec: int = 900) -> list[str]:
        """Pods that are NOT running AND are complaining about a volume.

        This, not the raw event list, is the question worth asking. A
        Multi-Attach event on its own is routine: delete a pod using an RWO
        volume and create another one immediately, and the second is told the
        volume is still exclusively attached until the first attachment is
        released. The attach controller retries and it clears. Our own
        seed/md5 utility pods do exactly that back to back, so an event-only
        check fails on the test's own setup -- which is what happened on the
        k8s run of 2026-09-23, where a Multi-Attach raised at 09:59:13 during
        volume seeding failed a cycle that ran at 10:05.

        What actually matters is whether anything is STILL waiting. A pod that
        is Running got its volume, whatever was logged on the way. So: take
        the pods that are not Running or Succeeded, and report only those with
        a recent volume complaint against them.
        """
        ns = namespace or self.namespace
        # Phase alone is the wrong question, and asking it cost run
        # 41015882185 a 1h58m cycle. A pod running an INIT container reports
        # Pending, and our FIO Jobs run `fio-warmup` as an init container --
        # for minutes. Such a pod plainly has its volume: the kubelet mounts
        # it before any container can start. The clone that failed that run
        # had, in its own event list and in this order:
        #
        #   FailedAttachVolume      Multi-Attach error for volume ...
        #   SuccessfulAttachVolume  AttachVolume.Attach succeeded
        #
        # It was eight minutes into its warmup, Pending, with a resolved
        # Multi-Attach event still inside the window -- and was reported as
        # "a volume did not re-attach".
        #
        # So ask whether anything has STARTED. A container that has ever run,
        # init or main, running or terminated, is proof the volume mounted. A
        # pod genuinely stuck behind Multi-Attach never reaches that point:
        # it has no container statuses at all. custom-columns prints <none>
        # for a field a pod does not have, so a missing list is not an error.
        cols = ",".join((
            ":metadata.name",
            ":status.phase",
            ":status.initContainerStatuses[*].state.running.startedAt",
            ":status.initContainerStatuses[*].state.terminated.startedAt",
            ":status.containerStatuses[*].state.running.startedAt",
            ":status.containerStatuses[*].state.terminated.startedAt",
        ))
        out, _ = self._exec_kubectl(
            f"kubectl get pods -n {ns} "
            f"-o custom-columns='{cols}' --no-headers "
            f"2>/dev/null || true", supress_logs=True, timeout=120)
        pending = set()
        for line in (out or "").splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            name, phase, started = parts[0], parts[1], parts[2:]
            if phase in ("Running", "Succeeded"):
                continue
            if any(v != "<none>" for v in started):
                # Mounted. Whatever it is waiting on, it is not the volume.
                continue
            pending.add(name)
        if not pending:
            return []

        stuck = []
        for entry in self.multi_attach_errors(namespace=ns,
                                              within_sec=within_sec):
            # entries read "[<age>] <pod>: <message>"
            name = entry.split("] ", 1)[-1].split(":", 1)[0].strip()
            if name in pending:
                stuck.append(entry)
        return stuck

    def multi_attach_errors(self, namespace: str | None = None,
                            within_sec: int = 900) -> list[str]:
        """Volume attach errors from the last *within_sec* seconds.

        A pod rescheduled after an eviction has to take its PV with it, and
        RWO volumes are where that goes wrong: if the old attachment is not
        released the new pod sits in ContainerCreating with
        ``Multi-Attach error for volume``. Silent until someone looks, which
        is why this is checked rather than assumed.

        The time bound is the point. Events live about an hour and these
        suites loop every few minutes, so an unbounded query makes iteration 5
        fail on iteration 2's events -- and once a real failure lands, every
        later iteration echoes it and a new failure is indistinguishable from
        the old one. Bounded, each iteration is judged on its own outage.

        An event with no usable timestamp is INCLUDED: a check that hunts a
        rare failure should not drop evidence because a field was missing.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get events -n {ns} --field-selector type=Warning "
            f"-o json 2>/dev/null || true",
            supress_logs=True, timeout=120)
        try:
            items = (json.loads(out or "{}") or {}).get("items", []) or []
        except ValueError:
            self.logger.warning(
                "[K8sUtils] could not parse events as JSON; "
                "reporting no attach errors rather than guessing")
            return []

        now = datetime.now(UTC)
        bad = []
        for ev in items:
            msg = (ev.get("message") or "").strip()
            if not any(m in msg.lower() for m in self.VOLUME_ATTACH_MARKERS):
                continue
            stamp = (ev.get("lastTimestamp") or ev.get("eventTime")
                     or (ev.get("series") or {}).get("lastObservedTime"))
            age = None
            if stamp:
                try:
                    age = (now - datetime.fromisoformat(
                        stamp)).total_seconds()
                except ValueError:
                    age = None
            if age is not None and age > within_sec:
                continue
            where = (ev.get("involvedObject") or {}).get("name", "?")
            when = f"{age:.0f}s ago" if age is not None else "age unknown"
            bad.append(f"[{when}] {where}: {msg[:180]}")
        return bad

    def get_all_k8s_node_names(self) -> list[str]:
        """Return a list of ALL K8s node hostnames."""
        out, _ = self._exec_kubectl(
            "kubectl get nodes --no-headers -o custom-columns=':metadata.name'",
            supress_logs=True,
        )
        return [n.strip() for n in out.strip().splitlines() if n.strip()]

    def detect_openshift(self) -> bool:
        """Return True if the cluster is OpenShift.

        Checks for the ``openshift-apiserver`` namespace which only
        exists on OpenShift clusters.  This avoids false positives on
        machines where the ``oc`` CLI is installed but the target
        cluster is not OpenShift (e.g. Talos).

        The result is cached after the first call.
        """
        if hasattr(self, "_is_openshift"):
            return self._is_openshift
        try:
            out, _ = self._exec_kubectl(
                "kubectl get namespace openshift-apiserver "
                "--no-headers 2>/dev/null && echo OCP_YES || echo OCP_NO",
                supress_logs=True,
            )
            self._is_openshift = "OCP_YES" in out
        except Exception:
            self._is_openshift = False
        self.logger.info(
            f"[K8sUtils] Platform detection: openshift={self._is_openshift}"
        )
        return self._is_openshift

    def detect_talos(self) -> bool:
        """Return True if the cluster nodes run Talos Linux.

        Checks the ``osImage`` field of the first node.  The result is
        cached after the first call.
        """
        if hasattr(self, "_is_talos"):
            return self._is_talos
        try:
            out, _ = self._exec_kubectl(
                "kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.osImage}'",
                supress_logs=True,
            )
            self._is_talos = "Talos" in (out or "")
        except Exception:
            self._is_talos = False
        self.logger.info(
            f"[K8sUtils] Platform detection: talos={self._is_talos}"
        )
        return self._is_talos

    # ── SPDK pod operations ──────────────────────────────────────────────────

    def get_spdk_pod_name(self, node_ip: str) -> str:
        """
        Return the name of the ``snode-spdk-pod-*`` pod running on the
        storage node with the given IP.

        Raises RuntimeError if the pod cannot be found.
        """
        k8s_node = self._get_k8s_node_name(node_ip)
        out, _ = self._exec_kubectl(
            (
                f"kubectl get pods -n {self.namespace} -o wide --no-headers "
                f"| awk '{{print $1, $7}}' "
                f"| grep '{k8s_node}' | grep snode-spdk | awk '{{print $1}}'"
            ),
            supress_logs=True,
        )
        pod = out.strip()
        if not pod:
            raise RuntimeError(
                f"[K8sUtils] No snode-spdk-pod found on K8s node {k8s_node!r} (IP: {node_ip})"
            )
        self.logger.info(f"[K8sUtils] SPDK pod for {node_ip}: {pod}")
        return pod

    def stop_spdk_pod(self, node_ip: str) -> str:
        """
        Force-delete the ``snode-spdk-pod-*`` for the given storage node IP.

        Kubernetes will automatically recreate the pod (DaemonSet / StatefulSet).
        Returns the pod name that was deleted.
        """
        pod_name = self.get_spdk_pod_name(node_ip)
        self.logger.info(
            f"[K8sUtils] Force-deleting SPDK pod {pod_name!r} on node {node_ip}"
        )
        self._exec_kubectl(
            (
                f"kubectl delete pod {pod_name} -n {self.namespace} "
                f"--grace-period=0 --force 2>&1 || true"
            ),
        )
        return pod_name

    def exec_in_spdk_container(self, node_ip: str, command: str) -> tuple:
        """Execute a command inside the spdk-container of the SPDK pod
        for the given storage node IP.

        Returns (stdout, stderr).
        """
        pod_name = self.get_spdk_pod_name(node_ip)
        return self._exec_kubectl(
            f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} -- "
            f"bash -c {shlex.quote(command)}"
        )

    def _find_spdk_sock(self, pod_name: str) -> str:
        """Return the spdk.sock path inside spdk-container (searches /mnt/ramdisk)."""
        out, _ = self._exec_kubectl(
            f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} -- "
            f"bash -c 'find /mnt/ramdisk -name spdk.sock -maxdepth 3 2>/dev/null | head -1'",
            supress_logs=True,
        )
        sock = out.strip()
        if not sock:
            raise RuntimeError(f"[K8sUtils] spdk.sock not found in {pod_name}")
        return sock

    def dump_lvstore_k8s(self, storage_node_id: str,
                          storage_node_ip: str, logs_path: str,
                          sbcli_cmd: str = "sbctl") -> None:
        """
        K8s equivalent of ssh_utils.dump_lvstore:
          1. Run sbcli sn dump-lvstore via admin pod.
          2. Parse dump file path from output.
          3. kubectl cp the file from spdk-container → logs_path/<pod_name>/lvstore_dumps/.
        """
        try:
            out, err = self.exec_sbcli(
                f"{sbcli_cmd} --dev -d sn dump-lvstore {storage_node_id}"
            )
            combined = (out or "") + (err or "")

            dump_file = None
            for line in combined.splitlines():
                if "LVS dump file will be here" in line:
                    # Line format: "...: INFO: LVS dump file will be here: /etc/simplyblock/..."
                    # Split on the marker text to reliably extract the path
                    parts = line.split("LVS dump file will be here:", 1)
                    if len(parts) == 2:
                        dump_file = parts[1].strip()
                    break

            if not dump_file:
                self.logger.warning(
                    f"[dump_lvstore_k8s] No dump file path in output for {storage_node_id}"
                )
                return

            pod_name = self.get_spdk_pod_name(storage_node_ip)
            dest_dir = os.path.join(logs_path, pod_name, "lvstore_dumps")
            os.makedirs(dest_dir, exist_ok=True)
            safe_name = os.path.basename(dump_file).replace(":", "_")
            dest_path = os.path.join(dest_dir, safe_name)

            # kubectl cp misinterprets colons in filenames as pod:path
            # separators, so copy to a colon-free temp path first.
            kexec = f"kubectl exec -n {self.namespace} {pod_name} -c spdk-container --"
            tmp_path = f"/tmp/{safe_name}"
            self._exec_kubectl(f"{kexec} cp {dump_file} {tmp_path}")
            self._exec_kubectl(
                f"kubectl cp -n {self.namespace} {pod_name}:{tmp_path} "
                f"-c spdk-container {dest_path}"
            )
            self._exec_kubectl(f"{kexec} rm -f {tmp_path}", supress_logs=True)
            self.logger.info(f"[dump_lvstore_k8s] {dump_file} → {dest_path}")
        except Exception as e:
            self.logger.warning(f"[dump_lvstore_k8s] FAILED node={storage_node_id}: {e}")

    def fetch_distrib_logs_k8s(self, storage_node_id: str,
                                storage_node_ip: str, logs_path: str) -> bool:
        """
        K8s equivalent of ssh_utils.fetch_distrib_logs:
          1. Find spdk.sock inside spdk-container.
          2. Get bdevs via RPC, collect distrib_* names.
          3. For each distrib: create JSON config and run rpc_sock.py
             with timeout + retry (120s first, then 600s).
          4. kubectl cp result files from /tmp inside container →
             logs_path/<pod_name>/distrib_logs/.
        Returns True (non-fatal failures are logged and skipped).
        """
        try:
            pod_name = self.get_spdk_pod_name(storage_node_ip)
            sock = self._find_spdk_sock(pod_name)
            dest_dir = os.path.join(logs_path, pod_name, "distrib_logs")
            os.makedirs(dest_dir, exist_ok=True)

            kexec = (
                f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} --"
            )
            rpc_base = f"{kexec} sudo python spdk/scripts/rpc.py -s {sock}"

            # 1. Get bdevs
            bdev_out, _ = self._exec_kubectl(f"{rpc_base} bdev_get_bdevs", supress_logs=True)
            try:
                bdevs = json.loads(bdev_out)
                distribs = sorted({
                    b.get("name", "")
                    for b in bdevs
                    if isinstance(b, dict) and str(b.get("name", "")).startswith("distrib_")
                })
            except Exception as e:
                self.logger.warning(f"[fetch_distrib_logs_k8s] bdev parse failed: {e}")
                return True

            if not distribs:
                self.logger.warning(f"[fetch_distrib_logs_k8s] No distrib_* bdevs on {storage_node_ip}")
                return True

            self.logger.info(f"[fetch_distrib_logs_k8s] distribs={distribs} pod={pod_name}")

            # 2. Dump each distrib with timeout + retry
            for distrib in distribs:
                try:
                    json_cfg = json.dumps({
                        "subsystems": [{
                            "subsystem": "distr",
                            "config": [{
                                "method": "distr_debug_placement_map_dump",
                                "params": {"name": distrib}
                            }]
                        }]
                    })
                    stack_file = f"/tmp/stack_{distrib}.json"
                    rpc_log = f"/tmp/rpc_{distrib}.log"

                    # Write JSON config into the container
                    self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(f"echo '{json_cfg}' > {stack_file}"),
                        supress_logs=True,
                    )

                    # Try with 120s timeout first, then retry with 600s
                    rpc_succeeded = False
                    for attempt, tmo in enumerate([120, 600], 1):
                        self.logger.info(
                            f"[fetch_distrib_logs_k8s] Dumping {distrib} "
                            f"(attempt {attempt}, timeout={tmo}s)"
                        )
                        rpc_cmd = (
                            f"timeout {tmo} kubectl exec {pod_name} "
                            f"-c spdk-container -n {self.namespace} -- "
                            f"bash -c "
                            + shlex.quote(
                                f"python scripts/rpc_sock.py {stack_file} {sock} "
                                f"> {rpc_log} 2>&1"
                            )
                            + "; echo EXIT_CODE=$?"
                        )
                        rpc_out, rpc_err = self._exec_kubectl(rpc_cmd, supress_logs=True)
                        combined = (rpc_out or "") + (rpc_err or "")
                        if "EXIT_CODE=124" in combined or "EXIT_CODE=137" in combined:
                            self.logger.warning(
                                f"[fetch_distrib_logs_k8s] {distrib} RPC timed out "
                                f"after {tmo}s (attempt {attempt})"
                            )
                            continue
                        rpc_succeeded = True
                        break

                    if not rpc_succeeded:
                        self.logger.warning(
                            f"[fetch_distrib_logs_k8s] {distrib} RPC timed out on "
                            f"all attempts — skipping"
                        )
                        self._exec_kubectl(
                            f"{kexec} bash -c "
                            + shlex.quote(f"rm -f {stack_file} {rpc_log} || true"),
                            supress_logs=True,
                        )
                        continue

                    # Read the RPC log
                    log_out, _ = self._exec_kubectl(
                        f"{kexec} bash -c 'cat {rpc_log} 2>/dev/null || true'",
                        supress_logs=True,
                    )
                    self.logger.info(
                        f"[fetch_distrib_logs_k8s] {distrib} rpc_log: "
                        f"{(log_out or '').strip()[:500]}"
                    )

                    # Copy the RPC log file out
                    rpc_log_dest = os.path.join(dest_dir, f"rpc_{distrib}.log")
                    self._exec_kubectl(
                        f"kubectl cp -n {self.namespace} {pod_name}:{rpc_log} "
                        f"-c spdk-container {rpc_log_dest}"
                    )

                    # Collect any /tmp files matching this distrib name
                    ls_out, _ = self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(f"ls /tmp/ 2>/dev/null | grep -F '{distrib}' || true"),
                        supress_logs=True,
                    )
                    for fname in (ls_out or "").splitlines():
                        fname = fname.strip()
                        if not fname:
                            continue
                        dest = os.path.join(dest_dir, fname)
                        self._exec_kubectl(
                            f"kubectl cp -n {self.namespace} {pod_name}:/tmp/{fname} "
                            f"-c spdk-container {dest}"
                        )
                        self.logger.info(f"[fetch_distrib_logs_k8s] copied /tmp/{fname} → {dest}")

                    # Cleanup temp files in container
                    self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(f"rm -f {stack_file} {rpc_log} || true"),
                        supress_logs=True,
                    )
                except Exception as e:
                    self.logger.warning(f"[fetch_distrib_logs_k8s] distrib={distrib} error: {e}")

            return True
        except Exception as e:
            self.logger.warning(f"[fetch_distrib_logs_k8s] FAILED node={storage_node_ip}: {e}")
            return True

    def wait_spdk_pod_running(self, node_ip: str, timeout: int = 600) -> None:
        """
        Block until the ``snode-spdk-pod-*`` on the given storage node IP
        reaches the *Running* state, or raise TimeoutError.
        """
        k8s_node = self._get_k8s_node_name(node_ip)
        self.logger.info(
            f"[K8sUtils] Waiting for snode-spdk-pod on {k8s_node} to be Running "
            f"(timeout={timeout}s)..."
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                (
                    f"kubectl get pods -n {self.namespace} -o wide --no-headers "
                    f"| grep snode-spdk | grep '{k8s_node}' | awk '{{print $3}}' || true"
                ),
                supress_logs=True,
            )
            if out.strip() == "Running":
                self.logger.info(
                    f"[K8sUtils] snode-spdk-pod on {k8s_node} is Running."
                )
                return
            time.sleep(15)
        raise TimeoutError(
            f"[K8sUtils] snode-spdk-pod on {k8s_node} did not reach Running within {timeout}s"
        )

    def restart_spdk_pod(self, node_ip: str) -> None:
        """
        K8s equivalent of ssh_utils.stop_spdk_process:
        delete the SPDK pod on the given node so Kubernetes restarts it automatically.
        """
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
            self.logger.info(f"[restart_spdk_pod] Deleting pod {pod_name} on {node_ip}")
            self._exec_kubectl(f"kubectl delete pod {pod_name} -n {self.namespace}")
            self.logger.info(f"[restart_spdk_pod] Pod {pod_name} deleted; waiting for restart")
        except Exception as e:
            self.logger.warning(f"[restart_spdk_pod] FAILED for {node_ip}: {e}")

    # ── Cluster credentials ──────────────────────────────────────────────────

    def get_cluster_credentials(self, sbcli_cmd: str = "sbctl") -> tuple:
        """
        Fetch CLUSTER_ID and CLUSTER_SECRET by running sbcli inside the admin pod.

        Returns (cluster_id, cluster_secret) as strings.
        """
        out_id, _ = self.exec_sbcli(
            f"{sbcli_cmd} cluster list"
            r" | grep -Eo '[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}'"
            " | head -1"
        )
        cluster_id = out_id.strip()
        if not cluster_id:
            raise RuntimeError(
                "[K8sUtils] Could not extract cluster_id via kubectl exec"
            )

        out_sec, _ = self.exec_sbcli(
            f"{sbcli_cmd} cluster get-secret {cluster_id}"
        )
        cluster_secret = out_sec.strip().splitlines()[-1].strip()
        if not cluster_secret:
            raise RuntimeError(
                f"[K8sUtils] Could not get cluster_secret for {cluster_id}"
            )

        return cluster_id, cluster_secret

    # ── Pod readiness utilities ──────────────────────────────────────────────

    def list_files_in_spdk_pod(self, node_ip: str, path: str) -> list:
        """
        List files in *path* inside the ``spdk-container`` of the SPDK pod
        running on *node_ip*.  Returns a list of filename strings (no paths).

        Used as a K8s substitute for ``ssh_obj.list_files(node_ip, path)``
        when checking for core dumps at ``/etc/simplyblock/``.
        """
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
            out, _ = self._exec_kubectl(
                f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} -- "
                f"bash -c 'ls {shlex.quote(path)} 2>/dev/null || true'",
                supress_logs=True,
            )
            return [f.strip() for f in out.splitlines() if f.strip()]
        except Exception as e:
            self.logger.warning(f"[list_files_in_spdk_pod] node={node_ip} path={path}: {e}")
            return []

    # ── Core dump collection ────────────────────────────────────────────────

    def copy_core_dumps_from_spdk_pod(self, node_ip: str,
                                       logs_path: str) -> list[str]:
        """Copy core dump files from inside the SPDK pod to *logs_path*.

        Looks for ``*.core*.zst`` and ``core.*`` files in
        ``/etc/simplyblock/`` inside the spdk-container and copies them
        via ``kubectl cp`` (same pattern as :meth:`dump_lvstore_k8s`).

        Returns a list of local paths of copied files.
        """
        copied: list[str] = []
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[coredump] Cannot find SPDK pod for {node_ip}, "
                f"skipping in-pod core dump copy: {exc}"
            )
            return copied

        files = self.list_files_in_spdk_pod(node_ip, "/etc/simplyblock/")
        core_files = [
            f for f in files
            if "core" in f.lower() and "tmp_cores" not in f
        ]
        if not core_files:
            return copied

        dest_dir = os.path.join(logs_path, pod_name, "core_dumps")
        os.makedirs(dest_dir, exist_ok=True)
        kexec = (
            f"kubectl exec -n {self.namespace} {pod_name} -c spdk-container --"
        )

        for fname in core_files:
            src = f"/etc/simplyblock/{fname}"
            # kubectl cp misinterprets colons — copy to a colon-free
            # temp path first (same workaround as dump_lvstore_k8s).
            safe_name = fname.replace(":", "_")
            tmp_path = f"/tmp/{safe_name}"
            dest_path = os.path.join(dest_dir, safe_name)
            try:
                self._exec_kubectl(
                    f"{kexec} cp {shlex.quote(src)} {tmp_path}",
                    supress_logs=True,
                )
                self._exec_kubectl(
                    f"kubectl cp -n {self.namespace} "
                    f"{pod_name}:{tmp_path} -c spdk-container "
                    f"{shlex.quote(dest_path)}",
                    supress_logs=True,
                    timeout=600,
                )
                self._exec_kubectl(
                    f"{kexec} rm -f {tmp_path}", supress_logs=True
                )
                if os.path.exists(dest_path):
                    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
                    self.logger.info(
                        f"[coredump] Copied pod core dump {fname} from "
                        f"{pod_name} ({size_mb:.1f} MB) -> {dest_path}"
                    )
                    copied.append(dest_path)
                else:
                    self.logger.warning(
                        f"[coredump] kubectl cp succeeded but file not "
                        f"found at {dest_path}"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[coredump] Failed to copy {fname} from {pod_name}: {exc}"
                )
        return copied

    def collect_host_core_dumps(self, node_ip: str, local_dir: str,
                                max_size_mb: int = 500) -> list[str]:
        """Collect host-level core dumps from a K8s node.

        Cores land on the NODE at ``/var/lib/systemd/coredump/``, because the
        host's ``core_pattern`` pipes to ``systemd-coredump``. They are never
        written inside the SPDK container.

        This used to try the running SPDK pod first, reading
        ``/proc/1/root/var/lib/systemd/coredump``. That cannot work: the SPDK
        pod does not set ``hostPID``, so PID 1 in its namespace is its own
        entrypoint and ``/proc/1/root`` resolves to the *container* root. The
        probe always found an empty directory, wrote a "Host core dumps"
        listing reading ``total 0``, and returned before the working paths
        below could run, so real cores sat uncollected on the hosts (run
        ``k8s_native_resilient_failover-20260906-112437``: four cores on the
        nodes, none collected). That path is removed; host access now always
        goes through a debug pod.

        Parameters
        ----------
        node_ip : str
            Storage-node management IP.
        local_dir : str
            Local directory to save collected data.
        max_size_mb : int
            Maximum core dump file size (MB) to copy. Files larger
            than this are logged but not copied.

        Returns
        -------
        list[str]
            Local paths of all saved files (listings, info, dumps).
        """
        saved: list[str] = []
        os.makedirs(local_dir, exist_ok=True)

        # ── Host access via a debug pod. OpenShift uses `oc debug node/`;
        #    everything else gets a privileged pod that hostPath-mounts the
        #    coredump directory. ────────────────────────────────────────────
        try:
            node_name = self._get_k8s_node_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[coredump] Cannot resolve K8s node name for {node_ip}, "
                f"skipping host core dump fallback: {exc}"
            )
            return saved

        try:
            saved = self._collect_host_core_dumps_fallback(
                node_name, node_ip, local_dir, max_size_mb,
            )
        except Exception as exc:
            self.logger.warning(
                f"[coredump] Fallback collection failed for "
                f"{node_ip} ({node_name}): {exc}"
            )
        return saved

    def _collect_host_core_dumps_fallback(
        self, node_name: str, node_ip: str, local_dir: str,
        max_size_mb: int,
    ) -> list[str]:
        """Collect host core dumps when the SPDK pod is not running.

        Uses platform-aware fallback:
        - **OpenShift**: ``oc debug node/<node> -- chroot /host``
        - **Vanilla K8s**: ephemeral privileged pod with ``nsenter``
        - **Talos Linux**: ephemeral privileged pod with ``hostPath``
          volume mount (no host binaries needed)
        """
        if self.detect_openshift():
            return self._fallback_via_oc_debug(
                node_name, node_ip, local_dir, max_size_mb
            )
        return self._fallback_via_privileged_pod(
            node_name, node_ip, local_dir, max_size_mb
        )

    def _fallback_via_oc_debug(
        self, node_name: str, node_ip: str, local_dir: str,
        max_size_mb: int,
    ) -> list[str]:
        """OpenShift fallback: use ``oc debug node/`` for host access."""
        saved: list[str] = []
        label = node_ip.replace(".", "_")

        for cmd_name, host_cmd in [
            ("coredump_listing", "ls -la /var/lib/systemd/coredump/"),
            (
                "coredumpctl_list",
                "coredumpctl list --no-pager 2>/dev/null || echo UNAVAILABLE",
            ),
            (
                "coredumpctl_info",
                "coredumpctl info --no-pager 2>/dev/null || true",
            ),
        ]:
            try:
                out, _ = self._exec_kubectl(
                    f"oc debug node/{node_name} -- chroot /host "
                    f"bash -c {shlex.quote(host_cmd)} 2>/dev/null || true",
                    supress_logs=True,
                    timeout=120,
                )
                if out and out.strip() and "UNAVAILABLE" not in out:
                    fpath = os.path.join(
                        local_dir, f"{cmd_name}_{label}.txt"
                    )
                    with open(fpath, "w") as f:
                        f.write(out)
                    saved.append(fpath)
                    self.logger.info(
                        f"[coredump] Saved {cmd_name} for {node_ip} "
                        f"(oc debug fallback)"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[coredump] oc debug {cmd_name} failed on "
                    f"{node_name}: {exc}"
                )
        return saved

    def _fallback_via_privileged_pod(
        self, node_name: str, node_ip: str, local_dir: str,
        max_size_mb: int,
    ) -> list[str]:
        """Vanilla K8s / Talos fallback: privileged pod with hostPath mount.

        Uses the container's own ``ls``/``sh`` on the mounted host
        directory, so no host binaries are required (Talos-compatible).
        """
        import hashlib

        saved: list[str] = []
        label = node_ip.replace(".", "_")
        hash_suffix = hashlib.md5(
            f"coredump-{node_name}-{time.time()}".encode()
        ).hexdigest()[:8]
        pod_name = f"coredump-collector-{hash_suffix}"
        ns = self.namespace

        yaml_spec = (
            f"apiVersion: v1\n"
            f"kind: Pod\n"
            f"metadata:\n"
            f"  name: {pod_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  nodeName: {node_name}\n"
            f"  hostPID: true\n"
            f"  tolerations:\n"
            f"  - operator: Exists\n"
            f"  containers:\n"
            f"  - name: collector\n"
            f"    image: busybox:1.37\n"
            f"    imagePullPolicy: IfNotPresent\n"
            f"    command: ['sleep', '300']\n"
            f"    securityContext:\n"
            f"      privileged: true\n"
            f"    volumeMounts:\n"
            f"    - name: host-coredumps\n"
            f"      mountPath: /host-coredumps\n"
            f"      readOnly: true\n"
            f"  volumes:\n"
            f"  - name: host-coredumps\n"
            f"    hostPath:\n"
            f"      path: /var/lib/systemd/coredump\n"
            f"      type: DirectoryOrCreate\n"
            f"  restartPolicy: Never\n"
        )

        try:
            # Deploy the collector pod
            self._exec_kubectl(
                f"kubectl delete pod {pod_name} -n {ns} "
                f"--force --grace-period=0 2>/dev/null || true",
                supress_logs=True,
            )
            self._exec_kubectl(
                f"cat <<'COREDUMP_EOF' | kubectl apply -f -\n"
                f"{yaml_spec}COREDUMP_EOF",
                supress_logs=True,
            )
            self._exec_kubectl(
                f"kubectl wait pod/{pod_name} -n {ns} "
                f"--for=condition=Ready --timeout=120s 2>/dev/null || true",
                supress_logs=True,
            )

            kexec = f"kubectl exec {pod_name} -n {ns} --"

            # 1. List core dumps via container's own ls
            out, _ = self._exec_kubectl(
                f"{kexec} ls -la /host-coredumps/ 2>/dev/null || echo EMPTY",
                supress_logs=True,
            )
            listing_path = os.path.join(
                local_dir, f"coredump_listing_{label}.txt"
            )
            with open(listing_path, "w") as f:
                f.write(
                    f"# Host core dumps on {node_ip} ({node_name}) "
                    f"[fallback pod]\n"
                )
                f.write("# Path: /var/lib/systemd/coredump/\n\n")
                f.write(out)
            saved.append(listing_path)

            if "EMPTY" in out or not out.strip():
                self.logger.info(
                    f"[coredump] No host-level core dumps on {node_ip} "
                    f"(fallback)"
                )
                return saved

            # Parse core file names
            core_files = []
            for line in out.strip().splitlines():
                parts = line.split()
                if (
                    parts
                    and "core" in line.lower()
                    and not line.startswith("total")
                ):
                    core_files.append(parts[-1])

            if core_files:
                self.logger.warning(
                    f"[coredump] HOST CORE DUMPS on {node_ip} "
                    f"(fallback): {core_files}"
                )

            # 2. Try coredumpctl via nsenter (best-effort, won't work on Talos)
            for cmd_name, host_cmd in [
                (
                    "coredumpctl_list",
                    "coredumpctl list --no-pager",
                ),
                (
                    "coredumpctl_info",
                    "coredumpctl info --no-pager",
                ),
            ]:
                try:
                    out, _ = self._exec_kubectl(
                        f"{kexec} nsenter -t 1 -m -u -i -n -- "
                        f"sh -c '{host_cmd} 2>/dev/null' "
                        f"2>/dev/null || true",
                        supress_logs=True,
                        timeout=120,
                    )
                    if out and out.strip():
                        fpath = os.path.join(
                            local_dir, f"{cmd_name}_{label}.txt"
                        )
                        with open(fpath, "w") as f:
                            f.write(out)
                        saved.append(fpath)
                        self.logger.info(
                            f"[coredump] Saved {cmd_name} for {node_ip} "
                            f"(fallback nsenter)"
                        )
                except Exception:
                    pass  # Expected on Talos

            # 3. Copy actual core dump files under size threshold
            for fname in core_files:
                try:
                    size_out, _ = self._exec_kubectl(
                        f"{kexec} stat -c '%s' "
                        f"/host-coredumps/{shlex.quote(fname)} "
                        f"2>/dev/null || echo 0",
                        supress_logs=True,
                    )
                    size_bytes = int(
                        size_out.strip().strip("'") or "0"
                    )
                    size_mb = size_bytes / (1024 * 1024)
                    self.logger.info(
                        f"[coredump] {node_ip}: {fname} = "
                        f"{size_mb:.1f} MB (fallback)"
                    )
                    if max_size_mb > 0 and size_mb > max_size_mb:
                        self.logger.warning(
                            f"[coredump] Skipping copy of {fname} on "
                            f"{node_ip} ({size_mb:.1f} MB > "
                            f"{max_size_mb} MB limit)"
                        )
                        continue
                except Exception:
                    continue

                safe_name = fname.replace(":", "_")
                local_path = os.path.join(
                    local_dir, f"{label}_{safe_name}"
                )
                try:
                    self._exec_kubectl(
                        f"kubectl cp -n {ns} "
                        f"{pod_name}:/host-coredumps/{shlex.quote(fname)} "
                        f"{shlex.quote(local_path)}",
                        supress_logs=True,
                        timeout=600,
                    )
                    if os.path.exists(local_path):
                        self.logger.info(
                            f"[coredump] Copied host core dump {fname} "
                            f"from {node_ip} ({size_mb:.1f} MB) "
                            f"-> {local_path}"
                        )
                        saved.append(local_path)
                except Exception as exc:
                    self.logger.warning(
                        f"[coredump] Failed to copy {fname} from "
                        f"{node_ip} (fallback): {exc}"
                    )
        finally:
            # Always clean up the collector pod
            try:
                self._exec_kubectl(
                    f"kubectl delete pod {pod_name} -n {ns} "
                    f"--force --grace-period=0 2>/dev/null || true",
                    supress_logs=True,
                )
            except Exception:
                pass

        return saved

    def wait_pod_ready(self, pod_name_prefix: str, timeout: int = 300) -> str:
        """
        Wait until a pod whose name starts with *pod_name_prefix* is Running.

        Returns the full pod name.
        """
        self.logger.info(
            f"[K8sUtils] Waiting for pod matching prefix {pod_name_prefix!r} to be Running..."
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                (
                    f"kubectl get pods -n {self.namespace} --no-headers "
                    f"-o custom-columns=:metadata.name,:status.phase "
                    f"| grep '{pod_name_prefix}' | head -1"
                ),
                supress_logs=True,
            )
            parts = out.strip().split()
            if len(parts) == 2 and parts[1] == "Running":
                self.logger.info(f"[K8sUtils] Pod {parts[0]} is Running.")
                return parts[0]
            time.sleep(10)
        raise TimeoutError(
            f"[K8sUtils] Pod with prefix {pod_name_prefix!r} not Running within {timeout}s"
        )

    # ── Generic YAML apply / delete ─────────────────────────────────────────

    def apply_yaml(self, yaml_content: str, namespace: str | None = None,
                   request_timeout: str = "60s"):
        """Apply a YAML manifest via ``kubectl apply -f -``."""
        ns = namespace or self.namespace
        escaped = yaml_content.replace("'", "'\\''")
        return self._exec_kubectl(
            f"echo '{escaped}' | kubectl apply -n {ns} "
            f"--request-timeout={request_timeout} -f -"
        )

    def apply_yaml_cluster_scoped(self, yaml_content: str):
        """Apply a cluster-scoped YAML manifest (no namespace flag)."""
        escaped = yaml_content.replace("'", "'\\''")
        return self._exec_kubectl(f"echo '{escaped}' | kubectl apply -f -")

    def delete_resource(self, kind: str, name: str, namespace: str | None = None):
        """Delete a K8s resource by kind and name."""
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl delete {kind} {name} -n {ns} --ignore-not-found --wait=false"
        )

    def get_resource_json(self, kind: str, name: str, namespace: str | None = None) -> dict:
        """Get a K8s resource as parsed JSON.  Returns ``{}`` if not found."""
        ns = namespace or self.namespace
        out, err = self._exec_kubectl(
            f"kubectl get {kind} {name} -n {ns} -o json 2>/dev/null || true",
            supress_logs=True,
        )
        text = out.strip()
        if not text or "NotFound" in (err or ""):
            return {}
        try:
            return json.loads(text)
        except Exception:
            return {}

    # ── StorageClass & VolumeSnapshotClass (cluster-scoped) ──────────────────

    def _storage_cluster_cr_name(self, namespace: str | None = None):
        """The StorageCluster CR's metadata.name, or "" if none is found.

        This is the name the storage.simplyblock.io/cluster label wants, which
        is NOT the cluster uuid that goes in StorageClass parameters. Returns
        "" rather than guessing a default: a label pointing at a cluster that
        does not exist binds the class to nothing, and a caller that knows the
        name should pass it rather than rely on this.
        """
        ns = namespace or self.namespace
        try:
            out, _err = self._exec_kubectl(
                f"kubectl get storageclusters -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true",
                supress_logs=True)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[K8sUtils] could not resolve the "
                                "StorageCluster CR name: %s", str(exc)[:120])
            return ""
        names = [n.strip() for n in (out or "").strip().splitlines() if n.strip()]
        return names[0] if names else ""

    def create_storage_class(self, name: str, cluster_id: str, pool_name: str,
                             ndcs: int = 1, npcs: int = 1, fs_type: str = "ext4",
                             compression: bool = False, encryption: bool = False,
                             fabric: str = "tcp",
                             max_namespace_per_subsys: int = 30,
                             dhchap_node_selector: str | None = None,
                             cluster_name: str | None = None, namespace: str | None = None):
        """Create a simplyblock CSI StorageClass.

        cluster_name/namespace: used for the three labels that attach this
            class to its StoragePool --
            ``storage.simplyblock.io/{namespace,cluster,pool}``. Encryption is
            a StorageClass setting again in v1alpha2 (there is no encryption
            field on StoragePool), and the labels are what tie the setting to
            a pool; a class carrying ``encryption`` with no labels is not
            associated with any pool. cluster_name is the StorageCluster CR
            NAME, deliberately separate from cluster_id, which is its uuid and
            goes in parameters. Resolved from the cluster when not given.

        dhchap_node_selector: the pool's node label key, shaped
            ``storage.simplyblock.io/storage-pool.<pool uuid>``. Renamed and
            reshaped from the old ``dhchap_node_label`` /
            ``simplyblock.io/pool.<ns>.<cluster>.<pool>``, which dev confirmed
            does not work any more. Required for a
            DHCHAP pool: without it the CSI driver provisions the volume with
            no ``nodeAffinity``, so any node mounts it and allowedNodes is not
            enforced at all. With it, the driver writes a matching
            nodeAffinity onto the PV and a non-allowed node fails to mount.
            Deliberately paired with ``volumeBindingMode: Immediate`` below and
            NOT with ``allowedTopologies`` — the operator's own generated class
            uses allowedTopologies, which only resolves once the CSI node
            driver has re-registered and picked the label up as a topology key.
        """
        dhchap_param = (
            f"  dhchap_node_selector: {dhchap_node_selector}\n"
            if dhchap_node_selector else ""
        )
        ns = namespace or self.namespace
        cl_name = cluster_name or self._storage_cluster_cr_name()
        # Lowercase, matching the documented example. These are CSI parameter
        # STRINGS, not YAML booleans, so the driver compares them as text and
        # str(True) == "True" is not obviously equal to "true".
        _b = lambda v: "true" if v else "false"       # noqa: E731
        labels = ""
        if cl_name:
            labels = (
                f"  labels:\n"
                f"    storage.simplyblock.io/namespace: {ns}\n"
                f"    storage.simplyblock.io/cluster: {cl_name}\n"
                f"    storage.simplyblock.io/pool: {pool_name}\n"
            )
        else:
            self.logger.warning(
                "[K8sUtils] StorageClass '%s': no StorageCluster CR name "
                "resolved, so it carries no pool labels. encryption=%s will "
                "not be associated with pool '%s'.", name, encryption, pool_name)
        yaml_content = (
            f"allowVolumeExpansion: true\n"
            f"apiVersion: storage.k8s.io/v1\n"
            f"kind: StorageClass\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"{labels}"
            f"parameters:\n"
            f"  cluster_id: \"{cluster_id}\"\n"
            f"  compression: \"{_b(compression)}\"\n"
            f"  csi.storage.k8s.io/fstype: {fs_type}\n"
            f"{dhchap_param}"
            f"  distr_ndcs: \"{ndcs}\"\n"
            f"  distr_npcs: \"{npcs}\"\n"
            f"  encryption: \"{_b(encryption)}\"\n"
            f"  fabric: {fabric}\n"
            f"  lvol_priority_class: \"0\"\n"
            f"  max_namespace_per_subsys: \"{max_namespace_per_subsys}\"\n"
            f"  pool_name: {pool_name}\n"
            f"  qos_r_mbytes: \"0\"\n"
            f"  qos_rw_iops: \"0\"\n"
            f"  qos_rw_mbytes: \"0\"\n"
            f"  qos_w_mbytes: \"0\"\n"
            f"  replicate: \"false\"\n"
            f"  tune2fs_reserved_blocks: \"0\"\n"
            f"provisioner: csi.simplyblock.io\n"
            f"reclaimPolicy: Delete\n"
            f"volumeBindingMode: Immediate\n"
        )
        # StorageClass parameters and volumeBindingMode are immutable —
        # delete first to allow recreation with different parameters.
        self.logger.info(f"[K8sUtils] Deleting existing StorageClass '{name}' (if any)")
        self._exec_kubectl(f"kubectl delete storageclass {name} --ignore-not-found")
        self.logger.info(f"[K8sUtils] Creating StorageClass '{name}'")
        self.apply_yaml_cluster_scoped(yaml_content)

    def create_volume_snapshot_class(self, name: str = "simplyblock-csi-snapshotclass"):
        """Create a VolumeSnapshotClass for the simplyblock CSI driver.

        If the class already exists (e.g. created by Helm), it is left as-is.
        """
        out, _ = self._exec_kubectl(
            f"kubectl get volumesnapshotclass {name} --no-headers 2>/dev/null || true",
            supress_logs=True,
        )
        if out.strip():
            self.logger.info(f"[K8sUtils] VolumeSnapshotClass '{name}' already exists, skipping creation")
            return

        yaml_content = (
            f"apiVersion: snapshot.storage.k8s.io/v1\n"
            f"kind: VolumeSnapshotClass\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"driver: csi.simplyblock.io\n"
            f"deletionPolicy: Delete\n"
        )
        self.logger.info(f"[K8sUtils] Creating VolumeSnapshotClass '{name}'")
        self.apply_yaml_cluster_scoped(yaml_content)

    def delete_storage_class(self, name: str):
        """Delete a StorageClass (cluster-scoped)."""
        self._exec_kubectl(f"kubectl delete storageclass {name} --ignore-not-found")

    def delete_volume_snapshot_class(self, name: str):
        """Delete a VolumeSnapshotClass (cluster-scoped)."""
        self._exec_kubectl(
            f"kubectl delete volumesnapshotclass {name} --ignore-not-found"
        )

    # ── PVC operations ───────────────────────────────────────────────────────

    def create_pvc(self, name: str, size: str, storage_class: str,
                   namespace: str | None = None, node_id: str | None = None,
                   volume_mode: str | None = None):
        """Create a PersistentVolumeClaim (provisions an lvol via CSI).

        Args:
            node_id: If provided, adds ``simplybk/host-id`` annotation to pin
                     the PVC to a specific storage node.
            volume_mode: ``"Block"`` for a raw device with no filesystem,
                     consumed through ``volumeDevices``. Omitted means the
                     cluster default, which is Filesystem. spdkcsi supports
                     both -- see GetBlock() in
                     csi-driver/internal/csi/node/publish.go.
        """
        ns = namespace or self.namespace
        annotations = ""
        if node_id:
            annotations = (
                f"  annotations:\n"
                f"    simplybk/host-id: {node_id}\n"
            )
        mode = f"  volumeMode: {volume_mode}\n" if volume_mode else ""
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: PersistentVolumeClaim\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"{annotations}"
            f"spec:\n"
            f"  accessModes:\n"
            f"  - ReadWriteOnce\n"
            f"{mode}"
            f"  resources:\n"
            f"    requests:\n"
            f"      storage: {size}\n"
            f"  storageClassName: {storage_class}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating PVC '{name}' size={size} "
            f"mode={volume_mode or 'Filesystem'} node={node_id or 'auto'}")
        self.apply_yaml(yaml_content, namespace=ns)

    #: Every spelling of the pin, because the operator normalises between
    #: them: a claim created with the legacy simplybk/host-id comes back
    #: carrying storage.simplyblock.io/selected-storage-node, and removing
    #: only the one that was written leaves the claim pinned.
    PIN_ANNOTATIONS = (
        "storage.simplyblock.io/selected-storage-node",
        "simplyblock.io/selected-storage-node",
        "simplybk/host-id",
    )

    def unpin_pvc(self, pvc_name: str, namespace: str | None = None):
        """Remove the storage-node pin from a claim.

        A pinned volume CANNOT BE MIGRATED. The operator refuses the
        operation outright -- "volume pvc-... is pinned to storage node
        ..., and a pinned volume cannot be moved; change or remove the pin
        on its claim instead" -- so anything that pins a claim in order to
        place it has to unpin before it can move it.

        That is not hypothetical: pinning is the only way to get several
        claims onto ONE node, which is the only way to get them into one
        NVMe-oF subsystem, which is what a batch case needs. Build the
        group pinned, then unpin, then migrate.
        """
        ns = namespace or self.namespace
        keys = " ".join(f"{a}-" for a in self.PIN_ANNOTATIONS)
        out, err = self._exec_kubectl(
            f"kubectl annotate pvc {pvc_name} -n {ns} {keys} "
            f"--overwrite 2>&1 || true", supress_logs=True)
        self.logger.info("[K8sUtils] unpinned PVC %s", pvc_name)
        return (out or "") + (err or "")

    def pvc_pin(self, pvc_name: str, namespace: str | None = None) -> str:
        """The node a claim is pinned to, or "" when it is not pinned."""
        ns = namespace or self.namespace
        res = self.get_resource_json("pvc", pvc_name, namespace=ns)
        ann = ((res.get("metadata") or {}).get("annotations") or {})
        for a in self.PIN_ANNOTATIONS:
            if ann.get(a):
                return str(ann[a])
        return ""

    def create_raw_device_pod(self, pod_name: str, pvc_name: str,
                              device_path: str = "/dev/rawlblk",
                              namespace: str | None = None,
                              image: str = "dockerpinata/fio:2.1",
                              timeout: int = 300):
        """Long-lived pod exposing a Block PVC as a raw device, with fio.

        volumeDevices, not volumeMounts: the latter is filesystem-only and a
        Block PVC cannot be mounted. The device appears at *device_path* with
        no filesystem on it at all, which is the point -- a filesystem journal
        can absorb or reshape a torn write, and these tests exist to catch one.

        Long-lived rather than a Job because the verifier runs stamp, churn and
        verify as separate commands against the same device, and a Job would
        exit between them.
        """
        ns = namespace or self.namespace
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: Pod\n"
            f"metadata:\n"
            f"  name: {pod_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  restartPolicy: Never\n"
            f"  containers:\n"
            f"  - name: raw\n"
            f"    image: {image}\n"
            f"    command: [\"sleep\", \"infinity\"]\n"
            f"    securityContext:\n"
            f"      privileged: true\n"
            f"    volumeDevices:\n"
            f"    - name: raw-volume\n"
            f"      devicePath: {device_path}\n"
            f"  volumes:\n"
            f"  - name: raw-volume\n"
            f"    persistentVolumeClaim:\n"
            f"      claimName: {pvc_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating raw-device pod '{pod_name}' for PVC "
            f"'{pvc_name}' at {device_path}")
        self.apply_yaml(yaml_content, namespace=ns)
        self.wait_pod_running(pod_name, namespace=ns, timeout=timeout)
        return device_path

    def create_clone_pvc(self, name: str, size: str, storage_class: str,
                         snapshot_name: str, namespace: str | None = None):
        """Create a PVC restored from a VolumeSnapshot (clone)."""
        ns = namespace or self.namespace
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: PersistentVolumeClaim\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  storageClassName: {storage_class}\n"
            f"  dataSource:\n"
            f"    name: {snapshot_name}\n"
            f"    kind: VolumeSnapshot\n"
            f"    apiGroup: snapshot.storage.k8s.io\n"
            f"  accessModes:\n"
            f"  - ReadWriteOnce\n"
            f"  resources:\n"
            f"    requests:\n"
            f"      storage: {size}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating clone PVC '{name}' from snapshot '{snapshot_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def resize_pvc(self, name: str, new_size: str, namespace: str | None = None):
        """Patch a PVC to request a larger size."""
        ns = namespace or self.namespace
        patch = f'{{"spec":{{"resources":{{"requests":{{"storage":"{new_size}"}}}}}}}}'
        self.logger.info(f"[K8sUtils] Resizing PVC '{name}' to {new_size}")
        self._exec_kubectl(
            f"kubectl patch pvc {name} -n {ns} -p '{patch}' --type merge"
        )

    def wait_pvc_bound(self, name: str, timeout: int = 300,
                       namespace: str | None = None) -> bool:
        """Poll until PVC phase is ``Bound``.  Returns True on success."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pvc {name} -n {ns} -o jsonpath='{{.status.phase}}' 2>/dev/null || true",
                supress_logs=True,
            )
            if out.strip() == "Bound":
                self.logger.info(f"[K8sUtils] PVC '{name}' is Bound")
                return True
            self.logger.info(f"[K8sUtils] Waiting for PVC '{name}' to bind (current: {out.strip()!r})…")
            time.sleep(5)
        raise TimeoutError(f"[K8sUtils] PVC '{name}' not Bound within {timeout}s")

    def delete_pvc(self, name: str, namespace: str | None = None):
        """Delete a PVC."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting PVC '{name}'")
        self.delete_resource("pvc", name, namespace=ns)

    def get_pvc_status(self, name: str, namespace: str | None = None) -> dict:
        """Return ``{phase, capacity}`` for a PVC."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} -o jsonpath="
            f"'{{.status.phase}} {{.status.capacity.storage}}' 2>/dev/null || true",
            supress_logs=True,
        )
        parts = out.strip().split()
        return {
            "phase": parts[0] if parts else "",
            "capacity": parts[1] if len(parts) > 1 else "",
        }

    def get_pv_capacity(self, pv_name: str) -> str:
        """Return a PV's ``spec.capacity.storage`` (e.g. ``'10Gi'``), or ''."""
        out, _ = self._exec_kubectl(
            f"kubectl get pv {pv_name} "
            f"-o jsonpath='{{.spec.capacity.storage}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return out.strip()

    def get_pvc_conditions(self, name: str, namespace: str | None = None) -> list:
        """Return the PVC's condition *types* as a list of strings.

        The one that matters for expansion is ``FileSystemResizePending``:
        the external resizer sets it once ``ControllerExpandVolume`` has
        succeeded but the driver asked for node-side expansion too.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} "
            f"-o jsonpath='{{.status.conditions[*].type}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return out.split()

    def wait_pv_capacity(self, pv_name: str, expected: str,
                         timeout: int = 300) -> bool:
        """Poll until a PV's ``spec.capacity.storage`` equals *expected*.

        This is the CONTROLLER side of a CSI expansion, and the only side that
        can complete while the volume is detached.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            cap = self.get_pv_capacity(pv_name)
            if cap == expected:
                self.logger.info(
                    f"[K8sUtils] PV {pv_name!r} capacity is {cap}")
                return True
            time.sleep(5)
        self.logger.warning(
            f"[K8sUtils] PV {pv_name!r} capacity never reached {expected} "
            f"within {timeout}s (last: {self.get_pv_capacity(pv_name)!r})")
        return False

    def wait_pvc_capacity(self, name: str, expected: str, timeout: int = 300,
                          namespace: str | None = None) -> bool:
        """Poll until a PVC's ``status.capacity.storage`` equals *expected*.

        This is the NODE side of a CSI expansion. For a Filesystem-mode volume
        the driver returns ``node_expansion_required: true`` and kubelet only
        runs ``NodeExpandVolume`` while some pod has the volume mounted -- so
        on a DETACHED claim this never converges, by design. Use
        :meth:`wait_pv_capacity` there instead.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pvc {name} -n {ns} "
                f"-o jsonpath='{{.status.capacity.storage}}' 2>/dev/null || true",
                supress_logs=True,
            )
            last = out.strip()
            if last == expected:
                self.logger.info(
                    f"[K8sUtils] PVC {name!r} status.capacity is {last}")
                return True
            time.sleep(5)
        self.logger.warning(
            f"[K8sUtils] PVC {name!r} status.capacity never reached "
            f"{expected} within {timeout}s (last: {last!r})")
        return False

    def get_mount_size_bytes(self, pod_name: str, mount_path: str = "/spdkvol",
                             namespace: str | None = None) -> int:
        """Return the size in bytes of *mount_path*'s filesystem inside a pod.

        Reads the filesystem as the workload sees it, which is what a
        node-side expansion actually has to change -- a grown block device
        with an ungrown filesystem is not a usable resize.
        """
        out, _ = self.exec_in_pod(
            pod_name, f"df -B1 {mount_path} | tail -1 | awk '{{print $2}}'",
            namespace=namespace,
        )
        try:
            return int((out or "").strip())
        except ValueError:
            return 0

    def get_pvc_volume_handle(self, name: str, namespace: str | None = None) -> str:
        """Return the CSI volumeHandle (lvol ID) backing a bound PVC, or ''."""
        ns = namespace or self.namespace
        # Get the PV name from the PVC
        pv, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} "
            f"-o jsonpath='{{.spec.volumeName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        pv = pv.strip()
        if not pv:
            return ""
        # Get the volumeHandle from the PV
        handle, _ = self._exec_kubectl(
            f"kubectl get pv {pv} "
            f"-o jsonpath='{{.spec.csi.volumeHandle}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return handle.strip()

    def get_pvc_pv_name(self, name: str, namespace: str | None = None) -> str:
        """Return the PersistentVolume name backing a bound PVC, or ''."""
        ns = namespace or self.namespace
        pv, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} "
            f"-o jsonpath='{{.spec.volumeName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return pv.strip()

    def get_pvc_primary_k8s_node(self, pvc_name: str, sbcli_utils,
                                namespace: str | None = None) -> str | None:
        """Return the K8s node hostname where the primary storage node of a PVC lives.

        Resolves PVC → volumeHandle → lvol → storage node → mgmt_ip → K8s node name.
        Returns None if any step fails.
        """
        try:
            vol_handle = self.get_pvc_volume_handle(pvc_name, namespace=namespace)
            if not vol_handle:
                return None
            lvol_id = vol_handle.split(":")[-1] if ":" in vol_handle else vol_handle
            lvol_details = sbcli_utils.get_lvol_details(lvol_id)
            if not lvol_details:
                return None
            node_id = lvol_details[0].get("node_id")
            if not node_id:
                return None
            node_details = sbcli_utils.get_storage_node_details(node_id)
            if not node_details:
                return None
            node_ip = node_details[0]["mgmt_ip"]
            return self._get_k8s_node_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[K8sUtils] Failed to resolve primary k8s node for PVC {pvc_name}: {exc}"
            )
            return None

    def log_fio_pvc_mapping(self, pvc_details: dict, clone_details: dict | None = None,
                            extra_details: dict | None = None,
                            snapshot_details: dict | None = None):
        """Log a table mapping FIO Job → PVC → lvol ID for debugging.

        Parameters
        ----------
        pvc_details : dict
            ``{pvc_name: {"job_name": ..., "node_id": ..., "storage_class": ..., ...}}``
        clone_details : dict | None
            Same structure for clone PVCs, with optional ``snap_name`` key.
        extra_details : dict | None
            Any additional PVC sets (e.g. new-node PVCs).
        snapshot_details : dict | None
            ``{snap_name: {"pvc_name": parent_pvc}}`` for parent PVC lookup.
        """
        all_entries = []
        for label, details in [("pvc", pvc_details),
                                ("clone", clone_details),
                                ("extra", extra_details)]:
            if not details:
                continue
            for name, info in details.items():
                job = info.get("job_name") or "N/A"
                vol_handle = self.get_pvc_volume_handle(name)
                storage_node = info.get("node_id", "N/A") or "N/A"
                sc = info.get("storage_class", "N/A") or "N/A"
                snap = info.get("snap_name", "") or ""
                parent_pvc = ""
                if snap and snapshot_details:
                    parent_pvc = snapshot_details.get(snap, {}).get("pvc_name", "")

                # Resolve FIO pod's K8s node
                fio_node = "N/A"
                if job and job != "N/A":
                    try:
                        pod = self.get_job_pod_name(job)
                        if pod:
                            fio_node = self.get_pod_node_name(pod) or "N/A"
                    except Exception:
                        pass

                fs_type = info.get("fs_type", "N/A") or "N/A"

                all_entries.append({
                    "type": label,
                    "name": name or "N/A",
                    "job": job,
                    "lvol_id": vol_handle or "N/A",
                    "storage_node": storage_node,
                    "storage_class": sc,
                    "fs_type": fs_type,
                    "snap_name": snap,
                    "parent_pvc": parent_pvc,
                    "fio_k8s_node": fio_node,
                })

        if not all_entries:
            return

        self.logger.info("=" * 190)
        self.logger.info("FIO Job → PVC/Clone → Lvol → Worker Mapping")
        self.logger.info("-" * 190)
        self.logger.info(
            f"{'FIO Job':<30} {'PVC/Clone':<25} {'Lvol ID':<40} "
            f"{'Storage Node':<40} {'FIO K8s Node':<20} {'SC':<28} "
            f"{'FS':<6} {'Snapshot':<20} {'Parent PVC':<25} {'Type':<6}"
        )
        self.logger.info("-" * 190)
        for e in all_entries:
            self.logger.info(
                f"{e['job']:<30} {e['name']:<25} {e['lvol_id']:<40} "
                f"{e['storage_node']:<40} {e['fio_k8s_node']:<20} {e['storage_class']:<28} "
                f"{e['fs_type']:<6} {e['snap_name']:<20} {e['parent_pvc']:<25} {e['type']:<6}"
            )
        self.logger.info("=" * 190)
        return all_entries

    # ── VolumeSnapshot operations ────────────────────────────────────────────

    def create_volume_snapshot(self, name: str, pvc_name: str,
                               snapshot_class: str = "simplyblock-csi-snapshotclass",
                               namespace: str | None = None):
        """Create a VolumeSnapshot from a PVC.

        If a stale VolumeSnapshot with the same name already exists (e.g.
        from a previous test run that did not clean up), it is deleted
        first to avoid ``persistentVolumeClaimName is immutable`` errors
        from ``kubectl apply``.
        """
        ns = namespace or self.namespace
        # Remove any stale VolumeSnapshot with the same name to avoid
        # immutable-field collisions from a prior test.
        existing = self.get_resource_json("volumesnapshot", name, namespace=ns)
        if existing:
            existing_pvc = (existing.get("spec", {})
                           .get("source", {})
                           .get("persistentVolumeClaimName", ""))
            if existing_pvc != pvc_name:
                self.logger.warning(
                    f"[K8sUtils] Stale VolumeSnapshot '{name}' found "
                    f"(source PVC '{existing_pvc}' != '{pvc_name}'), "
                    f"deleting before re-creating"
                )
                self.delete_volume_snapshot(name, namespace=ns, wait=True)
        yaml_content = (
            f"apiVersion: snapshot.storage.k8s.io/v1\n"
            f"kind: VolumeSnapshot\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  volumeSnapshotClassName: {snapshot_class}\n"
            f"  source:\n"
            f"    persistentVolumeClaimName: {pvc_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating VolumeSnapshot '{name}' from PVC '{pvc_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_volume_snapshot_ready(self, name: str, timeout: int = 300,
                                    namespace: str | None = None) -> bool:
        """Poll until VolumeSnapshot ``readyToUse`` is true."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get volumesnapshot {name} -n {ns} "
                f"-o jsonpath='{{.status.readyToUse}}' 2>/dev/null || true",
                supress_logs=True,
            )
            if out.strip() == "true":
                self.logger.info(f"[K8sUtils] VolumeSnapshot '{name}' is ready")
                return True
            self.logger.info(
                f"[K8sUtils] Waiting for VolumeSnapshot '{name}' readyToUse "
                f"(current: {out.strip()!r})…"
            )
            time.sleep(5)
        raise TimeoutError(
            f"[K8sUtils] VolumeSnapshot '{name}' not ready within {timeout}s"
        )

    def get_volume_snapshot_handle(self, name: str, namespace: str | None = None) -> str:
        """Return the backend snapshot UUID for a VolumeSnapshot, or ''.

        Resolves VolumeSnapshot → boundVolumeSnapshotContent →
        VolumeSnapshotContent.status.snapshotHandle.  The CSI snapshotHandle
        may be a composite ``cluster:node:snap_uuid``; the bare UUID (last
        ``:``-separated segment) is returned so it matches ``sbcli snapshot
        list``.
        """
        ns = namespace or self.namespace
        content, _ = self._exec_kubectl(
            f"kubectl get volumesnapshot {name} -n {ns} -o jsonpath="
            f"'{{.status.boundVolumeSnapshotContentName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        content = content.strip()
        if not content:
            return ""
        handle, _ = self._exec_kubectl(
            f"kubectl get volumesnapshotcontent {content} -o jsonpath="
            f"'{{.status.snapshotHandle}}' 2>/dev/null || true",
            supress_logs=True,
        )
        handle = handle.strip()
        if not handle:
            return ""
        return handle.rsplit(":", 1)[-1] if ":" in handle else handle

    def get_volume_snapshot_phase(self, name: str, namespace: str | None = None) -> str:
        """Return VolumeSnapshot readyToUse status string ('' if absent)."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get volumesnapshot {name} -n {ns} "
            f"-o jsonpath='{{.status.readyToUse}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return out.strip()

    def delete_volume_snapshot(self, name: str, namespace: str | None = None,
                               wait: bool = False):
        """Delete a VolumeSnapshot.

        When *wait* is True the call blocks until the VolumeSnapshot is fully
        removed, preventing stale-object collisions in subsequent tests.
        """
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting VolumeSnapshot '{name}'"
                         f"{' (waiting)' if wait else ''}")
        if wait:
            self._exec_kubectl(
                f"kubectl delete volumesnapshot {name} -n {ns} "
                f"--ignore-not-found --wait=true --timeout=120s"
            )
        else:
            self.delete_resource("volumesnapshot", name, namespace=ns)

    def has_client_nodes(self) -> bool:
        """Return True if any K8s node has the 'client' role label."""
        out, _ = self._exec_kubectl(
            "kubectl get nodes -l node-role.kubernetes.io/client "
            "--no-headers 2>/dev/null | wc -l",
            supress_logs=True,
        )
        return int(out.strip() or "0") > 0

    # ── FIO Job operations ───────────────────────────────────────────────────

    def create_fio_job(self, job_name: str, pvc_name: str, configmap_name: str,
                       fio_config: str, namespace: str | None = None,
                       image: str = "dockerpinata/fio:2.1",
                       cleanup_before_fio: bool = False,
                       avoid_node: str | None = None,
                       warmup_config: str | None = None,
                       node_name: str | None = None,
                       node_selector: str | None = None,
                       prefer_node: str | None = None,
                       backoff_limit: int = 0):
        """Create a ConfigMap with FIO config and a Job that runs FIO against a PVC.

        Args:
            backoff_limit: how many times Kubernetes may replace a failed pod.
                Defaults to 0, which is what every caller had before this was a
                parameter: one pod, no retries, a failure is final.

                That default is wrong for any job an outage is aimed at. A pod
                evicted with its node counts as a failed pod, so at 0 the Job is
                marked Failed and no replacement is ever placed -- which is how
                run 20261005-081722 spent 900s waiting for a reschedule that
                could not happen. Jobs that are expected to be moved need a real
                budget, or the move cannot be observed at all.

                Raising it does not hide FIO failures: get_job_pod_names selects
                on job-name and so lists terminated pods, and
                _collect_fio_findings reads every one of them.
            cleanup_before_fio: If True, add an init container that removes old
                FIO data files from the volume before FIO starts. Useful for
                clone PVCs that inherit files from the source.
            avoid_node: Optional K8s node hostname to avoid scheduling the FIO
                pod on (typically the primary storage node for the lvol).
                When set, a nodeAffinity rule excludes that node so the FIO
                pod runs on a secondary / non-primary node instead.
            warmup_config: Optional FIO config for a sequential write pass.
                When provided, an init container runs FIO with this config
                to pre-fill every block with valid MD5 verify headers (same
                bs, randseed, filenames, size as the main config) before the
                main randrw test.  This ensures a later verify_only pass can
                verify the entire file, not just the blocks randrw touched.
            node_name: hard-pin the job's pod to this node via ``spec.nodeName``,
                bypassing the scheduler (including StorageClass allowedTopologies).
                Mutually exclusive with avoid_node / client-node affinity, which
                are skipped when this is set.
        """
        ns = namespace or self.namespace
        # Indent fio_config for YAML embedding (each line indented by 8 spaces)
        indented_cfg = "\n".join(
            f"      {line}" for line in fio_config.strip().splitlines()
        )
        # Indent warmup config for YAML embedding
        indented_warmup = ""
        if warmup_config:
            indented_warmup = "\n".join(
                f"      {line}" for line in warmup_config.strip().splitlines()
            )
        init_containers_list = []
        if cleanup_before_fio:
            init_containers_list.append(
                "      - name: cleanup-old-fio\n"
                "        image: busybox:1.37\n"
                "        imagePullPolicy: IfNotPresent\n"
                "        command: [\"sh\", \"-c\", \"rm -f /spdkvol/*fio*\"]\n"
                "        volumeMounts:\n"
                "        - mountPath: /spdkvol\n"
                "          name: benchmark-volume\n"
            )
        if warmup_config:
            # FIO warmup init container: sequential write pass to pre-fill
            # all data files with valid verify headers matching the main config.
            init_containers_list.append(
                "      - name: fio-warmup\n"
                f"        image: {image}\n"
                "        imagePullPolicy: IfNotPresent\n"
                "        command: [\"fio\", \"/fio/fio-warmup.cfg\"]\n"
                "        volumeMounts:\n"
                "        - mountPath: /spdkvol\n"
                "          name: benchmark-volume\n"
                "        - mountPath: /fio\n"
                "          name: fio-config\n"
            )
        init_containers = ""
        if init_containers_list:
            init_containers = "      initContainers:\n" + "".join(init_containers_list)
        node_affinity_block = ""
        tolerations_block = ""
        node_name_line = f"      nodeName: {node_name}\n" if node_name else ""
        if sum(bool(x) for x in (node_name, node_selector, prefer_node)) > 1:
            raise ValueError(
                "create_fio_job: pass at most one of node_name, "
                "node_selector, prefer_node")
        if prefer_node:
            # Where to START, not where to stay. node_selector is a hard
            # nodeSelector: a pod carrying one cannot be rescheduled anywhere
            # else, so when its node goes away the Job's replacement sits
            # Pending forever and the outage suites can never exercise the
            # thing that matters most -- a client being moved off a dead node
            # and getting its volume back. A preference schedules it on the
            # same node in the normal case and lets the scheduler place it
            # elsewhere when that node is gone.
            node_affinity_block = (
                "        nodeAffinity:\n"
                "          preferredDuringSchedulingIgnoredDuringExecution:\n"
                "          - weight: 100\n"
                "            preference:\n"
                "              matchExpressions:\n"
                "              - key: kubernetes.io/hostname\n"
                "                operator: In\n"
                "                values:\n"
                f"                - {prefer_node}\n"
            )
        if node_selector:
            # nodeSelector keeps the scheduler in the loop, which a
            # WaitForFirstConsumer StorageClass requires in order to bind.
            node_name_line = (
                "      nodeSelector:\n"
                f"        kubernetes.io/hostname: {node_selector}\n")
        client_nodes_exist = (
            not node_name and not node_selector and not prefer_node
            and self.has_client_nodes())
        if client_nodes_exist:
            # Hard-pin FIO pods to client-role nodes
            node_affinity_block = (
                "        nodeAffinity:\n"
                "          requiredDuringSchedulingIgnoredDuringExecution:\n"
                "            nodeSelectorTerms:\n"
                "            - matchExpressions:\n"
                "              - key: node-role.kubernetes.io/client\n"
                "                operator: Exists\n"
            )
            # Tolerate the client-node taint so pods can schedule there
            tolerations_block = (
                "      tolerations:\n"
                "      - key: \"node-role\"\n"
                "        operator: \"Equal\"\n"
                "        value: \"client\"\n"
                "        effect: \"NoSchedule\"\n"
            )
            self.logger.info(
                f"[K8sUtils] Client nodes detected — FIO job '{job_name}' "
                f"pinned to client nodes (with toleration)"
            )
        elif (not node_name and not node_selector and not prefer_node
              and avoid_node):
            # No client nodes — at least avoid the primary storage node
            node_affinity_block = (
                f"        nodeAffinity:\n"
                f"          preferredDuringSchedulingIgnoredDuringExecution:\n"
                f"          - weight: 100\n"
                f"            preference:\n"
                f"              matchExpressions:\n"
                f"              - key: kubernetes.io/hostname\n"
                f"                operator: NotIn\n"
                f"                values:\n"
                f"                - {avoid_node}\n"
            )
        warmup_cfg_entry = ""
        if warmup_config:
            warmup_cfg_entry = (
                f"  fio-warmup.cfg: |\n"
                f"{indented_warmup}\n"
            )
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: ConfigMap\n"
            f"metadata:\n"
            f"  name: {configmap_name}\n"
            f"  namespace: {ns}\n"
            f"data:\n"
            f"  fio.cfg: |\n"
            f"{indented_cfg}\n"
            f"{warmup_cfg_entry}"
            f"---\n"
            f"apiVersion: batch/v1\n"
            f"kind: Job\n"
            f"metadata:\n"
            f"  name: {job_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  backoffLimit: {backoff_limit}\n"
            f"  template:\n"
            f"    metadata:\n"
            f"      labels:\n"
            f"        app: fio-benchmark\n"
            f"    spec:\n"
            f"{node_name_line}"
            f"      affinity:\n"
            f"        podAntiAffinity:\n"
            f"          preferredDuringSchedulingIgnoredDuringExecution:\n"
            f"          - weight: 100\n"
            f"            podAffinityTerm:\n"
            f"              labelSelector:\n"
            f"                matchLabels:\n"
            f"                  app: fio-benchmark\n"
            f"              topologyKey: kubernetes.io/hostname\n"
            f"{node_affinity_block}"
            f"{init_containers}"
            f"{tolerations_block}"
            f"      containers:\n"
            f"      - name: fio-benchmark\n"
            f"        image: {image}\n"
            f"        imagePullPolicy: IfNotPresent\n"
            f"        command: [\"fio\", \"--eta=always\", \"--status-interval=5\", \"/fio/fio.cfg\"]\n"
            f"        volumeMounts:\n"
            f"        - mountPath: /spdkvol\n"
            f"          name: benchmark-volume\n"
            f"        - mountPath: /fio\n"
            f"          name: fio-config\n"
            f"      volumes:\n"
            f"      - name: benchmark-volume\n"
            f"        persistentVolumeClaim:\n"
            f"          claimName: {pvc_name}\n"
            f"      - name: fio-config\n"
            f"        configMap:\n"
            f"          name: {configmap_name}\n"
            f"      restartPolicy: Never\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating FIO Job '{job_name}' on PVC '{pvc_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_job_complete(self, job_name: str, timeout: int = 600,
                          namespace: str | None = None) -> str:
        """Wait for a Job to reach Complete or Failed.

        Returns ``'succeeded'``, ``'failed'``, or ``'timeout'``.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get job {job_name} -n {ns} "
                f"-o jsonpath='{{.status.succeeded}} {{.status.failed}}' "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            parts = out.strip().split()
            succeeded = parts[0] if parts else ""
            failed = parts[1] if len(parts) > 1 else ""
            if succeeded and int(succeeded) >= 1:
                self.logger.info(f"[K8sUtils] Job '{job_name}' succeeded")
                return "succeeded"
            if failed and int(failed) >= 1:
                self.logger.warning(f"[K8sUtils] Job '{job_name}' failed")
                self._dump_job_diagnostics(job_name, ns, "failed")
                return "failed"
            time.sleep(10)
        self.logger.warning(f"[K8sUtils] Job '{job_name}' timed out after {timeout}s")
        self._dump_job_diagnostics(job_name, ns, "timed out")
        return "timeout"

    def _dump_job_diagnostics(self, job_name: str, namespace: str,
                              what: str) -> None:
        """Say why a Job did not finish, while its pods still exist.

        A timed-out fio job used to be reported as
        "did not succeed (status=timeout)" and nothing else, and the test
        then asserted -- so the pod, its events and its fio output were all
        torn down with the namespace before anyone could look. That is one
        whole run spent to learn a job did not finish. Everything here is
        read-only and bounded, and runs BEFORE the caller asserts.

        The three questions a stuck fio job actually raises: did the pod ever
        start (phase/waiting reason), did the volume mount (events), and did
        fio run and say something (logs).
        """
        self.logger.error(
            f"[fio-diag] Job '{job_name}' {what}. Dumping diagnostics...")

        def _show(label, cmd):
            try:
                out, _ = self._exec_kubectl(cmd, supress_logs=True)
                out = (out or "").strip()
                if out:
                    self.logger.error("[fio-diag] %s:\n%s", label, out)
                else:
                    self.logger.error("[fio-diag] %s: <empty>", label)
            except Exception as exc:                          # noqa: BLE001
                self.logger.warning(f"[fio-diag] {label} failed: {exc}")

        ns = namespace
        _show("job", f"kubectl describe job {job_name} -n {ns} 2>&1 | tail -40")
        # Pod phase and, when it never started, the reason it is waiting --
        # ImagePullBackOff and a volume that will not attach look identical
        # from the job's side.
        _show("pods", f"kubectl get pods -n {ns} -l job-name={job_name} "
                      f"-o wide 2>&1")
        # describe rather than a jsonpath: it carries the container's waiting
        # reason, the volume it is mounting and the pod's own events in one
        # read, and those are the three ways an fio job hangs without ever
        # producing output.
        _show("pod detail",
              f"kubectl describe pods -n {ns} -l job-name={job_name} "
              f"2>&1 | tail -60")
        _show("events", f"kubectl get events -n {ns} --sort-by=.lastTimestamp "
                        f"2>&1 | grep -i {job_name} | tail -25")
        # fio's own output. --tail bounds it; a job that never started
        # returns nothing and that is itself the answer.
        _show("fio log", f"kubectl logs -n {ns} -l job-name={job_name} "
                         f"--tail=60 --all-containers 2>&1")

    def job_active(self, job_name: str, namespace: str | None = None) -> bool:
        """True while a Job still has a pod running.

        Deliberately reads .status.active rather than asking whether the Job
        has pods: a Job that has completed keeps its pod around in Completed
        state, so pod existence stays true forever and anything using it as a
        liveness signal never notices the work stopped.

        An unreadable status returns True. Callers use this to decide whether
        to relaunch, and relaunching on top of a job that is still writing is
        worse than waiting one more cycle to find out.
        """
        ns = namespace or self.namespace
        try:
            out, _ = self._exec_kubectl(
                f"kubectl get job {job_name} -n {ns} "
                f"-o jsonpath='{{.status.active}}' 2>/dev/null || true",
                supress_logs=True,
            )
        except Exception as exc:                           # noqa: BLE001
            self.logger.warning(
                "[K8sUtils] job_active(%s) could not read .status.active "
                "(%s); reporting it ALIVE, which blocks a relaunch. If this "
                "repeats, that is the bug, not the workload.",
                job_name, str(exc)[:120])
            return True
        raw = (out or "").strip().strip("'")
        if not raw:
            # No active count at all: either the Job is gone or it has
            # finished. Both mean nothing is running.
            return False
        try:
            return int(raw) >= 1
        except ValueError:
            # The value that produced run 40809136442's "22 jobs still
            # running" on 22 jobs that had finished. Named here so the next
            # occurrence does not need to be inferred from the symptom.
            self.logger.warning(
                "[K8sUtils] job_active(%s): .status.active is %r, which is "
                "not a count. Reporting ALIVE; a finished job read this way "
                "is never relaunched and its volume goes idle.",
                job_name, raw[:120])
            return True

    def get_job_pod_names(self, job_name: str, namespace: str | None = None) -> list:
        """Get all pod names created by a Job."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pods -n {ns} --selector=job-name={job_name} "
            f"--no-headers -o custom-columns=:metadata.name",
            supress_logs=True,
        )
        return [p.strip() for p in out.strip().splitlines() if p.strip()]

    def get_job_pod_name(self, job_name: str, namespace: str | None = None) -> str:
        """Get the first pod name created by a Job."""
        pods = self.get_job_pod_names(job_name, namespace=namespace)
        return pods[0] if pods else ""

    def job_pod_node(self, job_name: str, namespace: str | None = None) -> str | None:
        """Node hosting the Running pod of *job_name*, or None.

        Deliberately only Running pods: a Job whose pod is Pending has no
        node worth recording, and a Terminating one is already leaving.
        """
        ns = namespace or self.namespace
        out, _err = self._exec_kubectl(
            f"kubectl get pods -n {ns} --selector=job-name={job_name} "
            f"--field-selector=status.phase=Running "
            f"-o jsonpath='{{.items[0].spec.nodeName}}' 2>/dev/null || true",
            supress_logs=True, timeout=120)
        return (out or "").strip().strip("'") or None

    def assert_clean_reschedule(self, job_name: str, from_node: str,
                                timeout: int = 900,
                                namespace: str | None = None) -> str:
        """A Job whose node died must come back somewhere else, cleanly.

        This is the assertion no k8s outage suite was making. The suites
        either pinned FIO so hard it could not move, or kept its node out of
        the outage list, so the single most important client-side behaviour --
        a pod losing its node and getting its RWO volume back somewhere else
        -- was never once exercised.

        Three things have to hold, and they fail in different ways:

        * a replacement is placed at all (the Job controller did its job);
        * it is on a DIFFERENT node -- landing back on the original means the
          node recovered before the scheduler moved anything, which is not the
          case under test and must not be reported as though it were;
        * nothing complained about the volume. An RWO volume whose old
          attachment is not released leaves the new pod in ContainerCreating
          behind "Multi-Attach error", and that is the failure this is hunting.

        Slow on purpose. When a node goes NotReady rather than being drained,
        kubernetes will not detach its RWO volumes until taint-based eviction
        and then the force-detach timer have both elapsed -- minutes, not
        seconds. A short timeout here would report a product failure every
        time kubernetes was merely being patient.

        Returns the new node name.
        """
        deadline = time.time() + timeout
        seen = None
        while time.time() < deadline:
            seen = self.job_pod_node(job_name, namespace=namespace)
            if seen and seen != from_node:
                break
            sleep_n_sec(10)
        else:
            seen = self.job_pod_node(job_name, namespace=namespace)

        if not seen:
            stuck = self.pods_stuck_on_volumes(namespace=namespace)
            raise RuntimeError(
                f"[reschedule] {job_name} was on {from_node}, that node went "
                f"away, and {timeout}s later no replacement pod is Running "
                f"anywhere. The Job controller should have placed one."
                + (f" Pods are stuck on volumes: {stuck[:6]}" if stuck else
                   " Nothing is stuck on a volume, so the pod is not being "
                   "blocked by an attachment -- look at scheduling."))
        if seen == from_node:
            raise RuntimeError(
                f"[reschedule] {job_name} is Running on {seen}, the same node "
                f"the outage hit. Nothing was actually rescheduled, so this "
                f"cycle proves nothing about moving a client off a dead node "
                f"-- most likely the node came back before the scheduler "
                f"acted, or the pod carries a hard nodeSelector.")

        stuck = self.pods_stuck_on_volumes(namespace=namespace)
        if stuck:
            raise RuntimeError(
                f"[reschedule] {job_name} moved {from_node} -> {seen}, but "
                f"pod(s) cannot get their volume:\n    "
                + "\n    ".join(stuck[:6])
                + "\nAn RWO volume that does not detach from the old node in "
                  "time leaves the replacement wedged in ContainerCreating.")
        self.logger.info(
            "[reschedule] %s moved %s -> %s and no pod is stuck on a volume",
            job_name, from_node, seen)
        return seen

    def get_pod_node_name(self, pod_name: str, namespace: str | None = None) -> str:
        """Return the K8s node hostname where a pod is/was scheduled."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pod {pod_name} -n {ns} "
            f"-o jsonpath='{{.spec.nodeName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return out.strip()

    def get_pod_status_detail(self, pod_name: str,
                              namespace: str | None = None) -> dict:
        """Return pod phase and container-level waiting reason.

        Returns a dict with keys:
          - ``phase``: Pod phase (Pending, Running, Succeeded, Failed, Unknown)
          - ``reason``: Human-readable waiting reason if any container is
            stuck (e.g. ``PodInitializing``, ``ContainerCreating``,
            ``CrashLoopBackOff``, ``ErrImagePull``).  Empty string when
            no container is in a waiting state.
          - ``message``: Optional detail message from the waiting state.
        """
        ns = namespace or self.namespace
        # Phase
        phase_out, _ = self._exec_kubectl(
            f"kubectl get pod {pod_name} -n {ns} "
            f"-o jsonpath='{{.status.phase}}' 2>/dev/null || true",
            supress_logs=True,
        )
        phase = phase_out.strip() or "Unknown"

        # Container waiting reason (init + regular containers)
        # jsonpath: check initContainerStatuses first, then containerStatuses
        reason = ""
        message = ""
        for path in (
            ".status.initContainerStatuses[?(@.state.waiting)].state.waiting",
            ".status.containerStatuses[?(@.state.waiting)].state.waiting",
        ):
            out, _ = self._exec_kubectl(
                f"kubectl get pod {pod_name} -n {ns} "
                f"-o jsonpath='{{range {path}}}{{.reason}}|{{.message}}{{end}}'"
                f" 2>/dev/null || true",
                supress_logs=True,
            )
            parts = out.strip().split("|", 1)
            if parts[0]:
                reason = parts[0]
                message = parts[1] if len(parts) > 1 else ""
                break

        return {"phase": phase, "reason": reason, "message": message}

    def get_pod_events(self, pod_name: str, namespace: str | None = None) -> str:
        """Return ``<reason>: <message>`` lines for events on a pod.

        Catches things ``get_pod_status_detail`` can't see, like a
        ``FailedMount`` warning event (kubelet's volume manager failing
        ``NodeStageVolume``), which is a Pod event, not a container
        waiting-state reason.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get events -n {ns} "
            f"--field-selector involvedObject.name={pod_name} "
            f"--sort-by=.lastTimestamp "
            f"-o jsonpath='{{range .items[*]}}{{.reason}}: {{.message}}{{\"\\n\"}}{{end}}' "
            f"2>/dev/null || true",
            supress_logs=True,
        )
        return out or ""

    def get_pod_logs(self, pod_name: str, namespace: str | None = None,
                     tail: int = 200, container: str | None = None) -> str:
        """Get pod logs (last *tail* lines), optionally from one container.

        Without *container* this is `kubectl logs <pod>`, which returns the
        single REGULAR container and nothing else -- see
        get_pod_logs_all_containers for why that matters here.
        """
        ns = namespace or self.namespace
        sel = f"-c {container} " if container else ""
        out, _ = self._exec_kubectl(
            f"kubectl logs {pod_name} -n {ns} {sel}--tail={tail} "
            f"2>/dev/null || true",
            supress_logs=True,
        )
        return out

    def get_pod_container_names(self, pod_name: str,
                                namespace: str | None = None) -> list:
        """Init containers first, then regular ones, in spec order."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pod {pod_name} -n {ns} -o jsonpath="
            f"'{{.spec.initContainers[*].name}} {{.spec.containers[*].name}}' "
            f"2>/dev/null || true", supress_logs=True)
        return (out or "").split()

    def get_pod_logs_all_containers(self, pod_name: str,
                                    namespace: str | None = None,
                                    tail: int = 20000) -> str:
        """Every container's log, init containers included.

        `kubectl logs <pod>` with no -c returns ONLY the regular container,
        silently -- verified against a pod with five init containers, which
        printed the main container's log with no warning and no mention of
        the others. Our FIO Jobs run `fio-warmup` as an init container, for
        eight minutes under outages in run 41015882185, and nothing ever read
        it. Every IO error that pass hit was reported as zero errors, because
        no code path had ever looked at the container that produced them.

        The default tail is large for the same reason: fio writes `io_u
        error` when it happens, not in its summary, so a short tail drops the
        evidence at the start of a long run and reports zero again.
        """
        ns = namespace or self.namespace
        names = self.get_pod_container_names(pod_name, namespace=ns)
        if not names:
            return self.get_pod_logs(pod_name, namespace=ns, tail=tail)
        chunks = []
        for name in names:
            text = self.get_pod_logs(pod_name, namespace=ns, tail=tail,
                                     container=name)
            if not text:
                continue
            # Deliberately free of "error"/"fail": validate_fio_job greps the
            # whole blob for those words and a header must not be a finding.
            chunks.append(f"===== container {name} on {pod_name} ====="
                          f"{chr(10)}{text}")
        return chr(10).join(chunks)

    def delete_job(self, job_name: str, namespace: str | None = None):
        """Delete a Job (cascading to its pods)."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting Job '{job_name}'")
        self._exec_kubectl(
            f"kubectl delete job {job_name} -n {ns} "
            f"--ignore-not-found --cascade=foreground"
        )

    def delete_configmap(self, name: str, namespace: str | None = None):
        """Delete a ConfigMap."""
        ns = namespace or self.namespace
        self.delete_resource("configmap", name, namespace=ns)

    def cleanup_stale_fio_resources(self, namespace: str | None = None):
        """Remove leftover FIO Jobs, ConfigMaps, PVCs, and VolumeSnapshots
        from any previous test run so tests start clean."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Cleaning stale test resources in namespace {ns}...")
        cmds = [
            # Delete FIO jobs by label
            f"kubectl delete jobs -n {ns} -l app=fio-benchmark --ignore-not-found",
            # Delete FIO configmaps (prefixed fiocfg- or fio-cfg-)
            (f"kubectl get configmaps -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep -E '^(fiocfg-|fio-cfg-)' | xargs -r kubectl delete configmap -n {ns} --ignore-not-found"),
            # Delete clone PVCs (prefixed clone-)
            (f"kubectl get pvc -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep '^clone-' | xargs -r kubectl delete pvc -n {ns} --ignore-not-found"),
            # Delete VolumeSnapshots (prefixed snap- or snapshot-)
            (f"kubectl get volumesnapshot -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep -E '^(snap-|snapshot-)' | xargs -r kubectl delete volumesnapshot -n {ns} --ignore-not-found --wait=true"),
            # Delete test PVCs (various prefixes)
            (f"kubectl get pvc -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep -E '^(pvc-|mig-pvc-|add-pvc-)' | xargs -r kubectl delete pvc -n {ns} --ignore-not-found"),
        ]
        for cmd in cmds:
            try:
                self._exec_kubectl(cmd)
            except Exception as exc:
                self.logger.warning(f"[K8sUtils] Stale resource cleanup step failed: {exc}")
        self.logger.info("[K8sUtils] Stale test resource cleanup done.")

    # ── CRD patch operations (StorageNode / StorageCluster) ────────────────

    def resolve_storage_node_cr_name(self, node_uuid: str,
                                      namespace: str | None = None) -> str:
        """Resolve a storage node UUID to its StorageNode CR name.

        The operator creates StorageNode CRs with random names
        (e.g. ``simplyblock-node-nklffw``).  This method lists all
        StorageNode CRs and finds the one whose ``status.uuid``
        matches *node_uuid*.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node (from sbcli / API).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        str
            The ``metadata.name`` of the matching StorageNode CR.

        Raises
        ------
        ValueError
            If no StorageNode CR matches the given UUID.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get storagenodes.storage.simplyblock.io -n {ns} "
            f"-o json 2>/dev/null || true",
            supress_logs=True,
        )
        try:
            data = json.loads(out.strip()) if out and out.strip() else {}
        except Exception:
            data = {}

        for item in data.get("items", []):
            status = item.get("status", {})
            cr_uuid = status.get("uuid", "")
            if cr_uuid == node_uuid:
                cr_name = item["metadata"]["name"]
                self.logger.info(
                    f"[K8sUtils] Resolved node UUID {node_uuid} -> "
                    f"StorageNode CR '{cr_name}'"
                )
                return cr_name

        raise ValueError(
            f"[K8sUtils] No StorageNode CR found with UUID {node_uuid} "
            f"in namespace {ns}"
        )

    def create_storage_node_ops(self, name: str,
                                 storage_node_ref: str,
                                 action: str,
                                 target_worker_node: str | None = None,
                                 reattach_volume: bool = False,
                                 new_ssd_pcie: list[str] | None = None,
                                 namespace: str | None = None):
        """Create a StorageNodeOps CR to trigger a node operation.

        Replaces the old pattern of patching StorageNodeSet with
        ``spec.action``.  The operator watches StorageNodeOps CRs
        and executes the requested operation.

        Parameters
        ----------
        name : str
            Name for the StorageNodeOps CR
            (e.g. ``migrate-83c2a579-to-worker-4``).
        storage_node_ref : str
            Name of the StorageNode CR to operate on
            (e.g. ``simplyblock-node-nklffw``).
        action : str
            Operation: ``migrate``, ``restart``, ``suspend``,
            ``resume``, ``remove``, or ``shutdown``.
        target_worker_node : str | None
            Target worker node name (required for ``migrate``).
        reattach_volume : bool
            Whether to reattach volumes after the operation.
        new_ssd_pcie : list[str] | None
            PCIe addresses for new SSDs on the target worker.
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace

        # v1alpha2. The older version is still served but needs the
        # conversion webhook, and this install does not deploy one --
        # "service simplyblock-operator-conversion-webhook-service not found"
        # is what every v1alpha1 read and write returns. The shape differs:
        # storageNodeRef is nodeRef, the action enum is capitalised, and
        # targetWorkerNode/newSsdPcie moved under a migrate block.
        action_v2 = {
            "shutdown": "Shutdown", "restart": "Restart", "suspend": "Suspend",
            "resume": "Resume", "remove": "Remove", "migrate": "Migrate",
        }.get(str(action).lower(), str(action))

        spec_lines = (
            f"  nodeRef: {storage_node_ref}\n"
            f"  action: {action_v2}\n"
        )
        if reattach_volume:
            spec_lines += "  reattachVolume: true\n"
        if target_worker_node or new_ssd_pcie:
            spec_lines += "  migrate:\n"
            if target_worker_node:
                spec_lines += f"    targetWorkerNode: {target_worker_node}\n"
            if new_ssd_pcie:
                spec_lines += "    newSsdPcie:\n"
                for pcie in new_ssd_pcie:
                    spec_lines += f'      - "{pcie}"\n'

        yaml_content = (
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: StorageNodeOps\n"
            "metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            "spec:\n"
            f"{spec_lines}"
        )

        self.logger.info(
            f"[K8sUtils] Creating StorageNodeOps '{name}' "
            f"(action={action}, ref={storage_node_ref})"
        )
        return self.apply_yaml(yaml_content, namespace=ns)

    def wait_storage_node_ops_done(self, name: str, timeout: int = 600,
                                    namespace: str | None = None) -> dict:
        """Poll until StorageNodeOps reaches ``Succeeded`` phase.

        Parameters
        ----------
        name : str
            StorageNodeOps CR name.
        timeout : int
            Maximum wait time in seconds (default 600).
        namespace : str | None
            Override namespace.

        Returns
        -------
        dict
            The StorageNodeOps resource JSON on success.

        Raises
        ------
        AssertionError
            If the operation reaches ``Failed`` phase.
        TimeoutError
            If the operation does not reach ``Succeeded`` within
            *timeout*.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        phase = "unknown"
        while time.time() < deadline:
            res = self.get_resource_json(
                "storagenodeops.storage.simplyblock.io", name, namespace=ns,
            )
            status = res.get("status", {})
            phase = (status.get("phase") or "").strip()
            # subPhase was removed in v1alpha2 and replaced by step.state;
            # read both so the log says something on either version.
            sub_phase = ((status.get("subPhase") or "")
                         or (status.get("step") or {}).get("state", "")).strip()

            if phase == "Succeeded":
                self.logger.info(
                    f"[K8sUtils] StorageNodeOps '{name}' Succeeded"
                )
                return res
            # Aborted is a v1alpha2 addition. Left out, an aborted operation
            # was indistinguishable from a slow one and the wait spun to its
            # full timeout instead of failing where the cause was still legible.
            if phase in ("Failed", "Aborted"):
                self._dump_storage_node_ops_diagnostics(name, ns)
                raise AssertionError(
                    f"StorageNodeOps '{name}' {phase.lower()}: "
                    f"{status.get('message', 'no message')}"
                )
            self.logger.info(
                f"[K8sUtils] Waiting for StorageNodeOps '{name}' "
                f"(phase={phase}, subPhase={sub_phase})"
            )
            time.sleep(15)
        self._dump_storage_node_ops_diagnostics(name, ns)
        raise TimeoutError(
            f"StorageNodeOps '{name}' did not reach Succeeded within "
            f"{timeout}s (last phase={phase})"
        )

    # ── PersistentVolumeOps: a volume move, the way k8s raises one ───────
    #
    # On Kubernetes nothing calls ``sbctl volume migrate``. A move is a
    # cluster-scoped PersistentVolumeOps, and the operator performs the
    # whole two-phase handshake itself: ``Validating`` POSTs the migration
    # and starts one Job per CONSUMING NODE to connect and verify the
    # target paths, ``Migrating`` continues it (which is what starts the
    # copy), and ``Verifying`` deletes those Jobs and clears the
    # controllers they left behind.  The spec is
    # operator/docs/designs/crd-redesign/design-persistentvolumeops.md §5.
    #
    # Which is why a test must NOT connect the target paths here.  On
    # docker the harness does it because nothing else will; doing it on
    # k8s races the operator's own Jobs for the same paths, and
    # ``Verifying`` exists precisely because paths a checker left behind
    # once "outlived the Jobs, poisoned the data path, and blocked every
    # later migration on that volume".
    #
    # VolumeMigration is a DIFFERENT kind and is not this one.  It is the
    # registered/legacy spelling, kept one release for drain-on-upgrade,
    # and operator cmd/main.go defaults --legacy-volume-migration to
    # false -- so on a default install a VolumeMigration CR is reconciled
    # by nobody and the move silently never happens.
    PVO_KIND = "persistentvolumeops.storage.simplyblock.io"

    def create_persistent_volume_ops(self, name: str, pv_name: str,
                                     target_node_ref: str,
                                     target_namespace: str | None = None):
        """Raise a volume move as a PersistentVolumeOps.  Returns (out, err).

        Cluster-scoped, unlike its StorageNodeOps sibling -- the PV it
        names is cluster-scoped too, so there is no namespace for the
        operation to live in.  ``targetNodeRef`` needs BOTH name and
        namespace (the CRD marks them required): it locates a StorageNode,
        which is namespaced.
        """
        ns = target_namespace or self.namespace
        yaml_content = (
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: PersistentVolumeOps\n"
            "metadata:\n"
            f"  name: {name}\n"
            "spec:\n"
            "  action: Migrate\n"
            f"  persistentVolumeName: {pv_name}\n"
            "  migrate:\n"
            "    targetNodeRef:\n"
            f"      name: {target_node_ref}\n"
            f"      namespace: {ns}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating PersistentVolumeOps '{name}': migrate "
            f"{pv_name} -> StorageNode {ns}/{target_node_ref}"
        )
        return self.apply_yaml_cluster_scoped(yaml_content)

    def get_persistent_volume_ops(self, name: str) -> dict:
        """The operation as JSON, or ``{}`` when it does not exist."""
        out, err = self._exec_kubectl(
            f"kubectl get {self.PVO_KIND} {name} -o json 2>/dev/null || true",
            supress_logs=True,
        )
        text = (out or "").strip()
        if not text or "NotFound" in (err or ""):
            return {}
        try:
            return json.loads(text)
        except Exception:                             # noqa: BLE001
            return {}

    def list_persistent_volume_ops(self) -> list:
        """Every operation on the cluster.  Cheap enough to poll."""
        out, _ = self._exec_kubectl(
            f"kubectl get {self.PVO_KIND} -o json 2>/dev/null || true",
            supress_logs=True,
        )
        try:
            return json.loads((out or "").strip() or "{}").get("items", [])
        except Exception:                             # noqa: BLE001
            return []

    def abort_persistent_volume_ops(self, name: str):
        """Ask a running operation to stop at its next step and unwind.

        Expressible from ``Validating`` and ``Migrating`` only.  Once the
        copy has finished the volume has already moved and there is
        nothing to undo, so the step graph rejects the edge and the
        controller reports an IllegalTransitionError rather than leaving a
        half-undone move.  An abort that arrives that late is therefore
        not an error in the test -- it is the documented answer.
        """
        patch = '{"spec":{"abort":true}}'
        return self._exec_kubectl(
            f"kubectl patch {self.PVO_KIND} {name} --type merge "
            f"-p '{patch}'"
        )

    def delete_persistent_volume_ops(self, name: str):
        """Delete one operation.

        Safe at every step that admits it, and not a way to drop paths on
        the floor: a non-terminal object carries a finalizer that clears
        only once "the backend migration is canceled, the Jobs are
        deleted, no path is left connected, and the volume's lock is
        released" (§5.1).  A finalizer that cannot finish BLOCKS the
        delete and leaves the object with the reason in
        ``status.message``, which is the intended outcome rather than a
        hang to work around.

        Two consequences for a test.  A delete during ``Verifying`` is
        REFUSED by admission -- that step declares no abort edge and is
        itself the cleanup, so there is nothing to unwind.  And a delete
        leaves no record, so "stop it and keep the evidence" is
        :meth:`abort_persistent_volume_ops` alone; this is only for
        discarding it afterwards.
        """
        return self._exec_kubectl(
            f"kubectl delete {self.PVO_KIND} {name} "
            f"--ignore-not-found --wait=false"
        )

    def _dump_storage_node_ops_diagnostics(self, ops_name: str,
                                            namespace: str) -> None:
        """Log diagnostic info when a StorageNodeOps fails or times out."""
        self.logger.error(
            f"[nodeops-diag] StorageNodeOps '{ops_name}' did not succeed. "
            f"Dumping diagnostics..."
        )
        try:
            out, _ = self._exec_kubectl(
                f"kubectl describe storagenodeops.storage.simplyblock.io "
                f"{ops_name} -n {namespace}",
                supress_logs=True,
            )
            self.logger.error(f"[nodeops-diag] describe:\n{out}")
        except Exception as e:
            self.logger.warning(f"[nodeops-diag] describe failed: {e}")

        try:
            out, _ = self._exec_kubectl(
                f"kubectl get events -n {namespace} --sort-by=.lastTimestamp "
                f"--field-selector involvedObject.name={ops_name} "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(f"[nodeops-diag] events:\n{out}")
        except Exception as e:
            self.logger.warning(f"[nodeops-diag] events query failed: {e}")

        try:
            out, _ = self._exec_kubectl(
                f"kubectl logs -n {namespace} "
                f"-l app=simplyblock-operator --tail=50 "
                f"--all-containers 2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(
                    f"[nodeops-diag] operator logs (tail 50):\n{out}"
                )
        except Exception as e:
            self.logger.warning(f"[nodeops-diag] operator logs failed: {e}")

    def cleanup_stale_node_ops(self, namespace: str | None = None):
        """Remove leftover StorageNodeOps CRs from previous test runs."""
        ns = namespace or self.namespace
        self.logger.info(
            f"[K8sUtils] Cleaning stale StorageNodeOps in {ns}..."
        )
        self._exec_kubectl(
            f"kubectl delete storagenodeops.storage.simplyblock.io --all "
            f"-n {ns} --ignore-not-found --wait=false 2>/dev/null || true"
        )

    def patch_storage_node_add_workers(self, new_workers: list,
                                        storage_node_set_ref: str = "simplyblock-node",
                                        namespace: str | None = None,
                                        cluster_ref: str = "simplyblock-cluster",
                                        timeout: int = 3600):
        """Add worker nodes by growing the cluster through a discovery run.

        This used to create a StorageNode CR per worker with
        ``spec.overrides.expand: true``, reading driveSizeRange and pcieModel
        off the parent StorageNodeSet. Neither half of that still works:

        * The StorageNodeSet is gone. Nothing reconciles one any more, so the
          bring-up no longer creates it and the read came back empty -- the new
          nodes would have been created with no device selection at all.
        * A hand-written StorageNode has no cluster. v1alpha1 has no clusterRef
          field, and the conversion webhook derives it from a controller owner
          reference of kind StorageCluster (controllingClusterName in
          storagenode_conversion.go). A bare CR has no owner, so it converted
          to a node with an empty ClusterRef, belonging to nothing.

        Growing a cluster is now a second discovery naming the existing one:
        the draft it writes carries the same clusterRef, and approving it
        creates the nodes already marked as an expansion, which the control
        plane reads as a request to rebalance onto them.

        Parameters
        ----------
        new_workers : list[str]
            Kubernetes node names to add (e.g. ``["worker-4", "worker-5"]``).
        storage_node_set_ref : str
            Retained for call compatibility; used only to name the objects.
        namespace : str | None
            Override namespace (default ``self.namespace``).
        cluster_ref : str
            The StorageCluster to grow.
        timeout : int
            Seconds to wait for the expansion to finish.
        """
        ns = namespace or self.namespace
        if not new_workers:
            self.logger.info("[K8sUtils] no workers to add")
            return None

        suffix = "-".join(w.split(".")[0] for w in new_workers)[:30]
        config_name = f"grow-{suffix}"
        ops_name = f"discover-{config_name}"[:63]

        worker_yaml = "".join(f"      - {w}\n" for w in new_workers)
        yaml_content = (
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: OperatorOps\n"
            "metadata:\n"
            f"  name: {ops_name}\n"
            f"  namespace: {ns}\n"
            "spec:\n"
            "  action: Discover\n"
            "  discover:\n"
            f"    configName: {config_name}\n"
            f"    clusterRef: {cluster_ref}\n"
            "    enableControlPlaneNodes: false\n"
            "    workers:\n"
            f"{worker_yaml}"
        )
        self.logger.info(
            f"[K8sUtils] growing {cluster_ref} with {new_workers} "
            f"via discovery '{ops_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

        config = self._await_discovery_config(ops_name, ns, timeout=900)
        self.approve_deployment_config(config, namespace=ns, timeout=timeout)
        return config

    def _await_discovery_config(self, ops_name: str, namespace: str,
                                timeout: int = 900) -> str:
        """Wait for a Discover run to write its draft, and name it."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self.get_resource_json(
                "operatorops.storage.simplyblock.io", ops_name,
                namespace=namespace,
            )
            status = res.get("status", {}) if res else {}
            ref = status.get("configRef")
            if ref:
                self.logger.info(
                    f"[K8sUtils] discovery '{ops_name}' wrote "
                    f"ClusterDeploymentConfig '{ref}'"
                )
                return ref
            if status.get("phase") == "Failed":
                raise AssertionError(
                    f"discovery '{ops_name}' failed: "
                    f"{status.get('message', 'no message')}"
                )
            self.logger.info(
                f"[K8sUtils] waiting for discovery '{ops_name}' "
                f"(phase={status.get('phase', '-')})"
            )
            time.sleep(10)
        raise TimeoutError(
            f"discovery '{ops_name}' wrote no config within {timeout}s")

    def approve_deployment_config(self, name: str, namespace: str | None = None,
                                  timeout: int = 3600) -> dict:
        """Approve a draft and wait for the expansion to finish.

        Approval is one way -- "approval cannot be withdrawn", and an approved
        document is immutable -- so anything that needs editing must already
        have been written.
        """
        ns = namespace or self.namespace
        self._exec_kubectl(
            f"kubectl patch clusterdeploymentconfigs.storage.simplyblock.io "
            f"{name} -n {ns} --type=merge "
            f"-p '{{\"spec\":{{\"approved\":true}}}}'"
        )
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            res = self.get_resource_json(
                "clusterdeploymentconfigs.storage.simplyblock.io", name,
                namespace=ns,
            )
            status = res.get("status", {}) if res else {}
            phase = status.get("phase", "")
            step = (status.get("step") or {}).get("state", "")
            cur = f"{phase}/{step}: {status.get('message', '')}"
            if cur != last:
                self.logger.info(f"[K8sUtils] {name}: {cur}")
                last = cur
            if phase == "Expanded":
                self.logger.info(
                    f"[K8sUtils] {name} expanded; "
                    f"{len(status.get('nodeRefs') or [])} storage node(s)"
                )
                return res
            if phase == "Failed":
                raise AssertionError(
                    f"deployment config '{name}' failed: "
                    f"{status.get('message', 'no message')}"
                )
            time.sleep(15)
        raise TimeoutError(
            f"deployment config '{name}' did not finish within {timeout}s "
            f"(last {last})")

    def patch_storage_cluster_expand(self, name: str = "simplyblock-cluster",
                                      namespace: str | None = None):
        """Patch StorageCluster CRD to trigger cluster expansion.

        .. note::
           With the new StorageNode CR model, expansion is triggered
           automatically when a StorageNode CR is created with
           ``overrides.expand: true``.  This method is retained for
           backward compatibility but may no longer be needed.

        Parameters
        ----------
        name : str
            StorageCluster CR name (default ``simplyblock-cluster``).
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace
        cmd = (
            f"kubectl patch storageclusters.storage.simplyblock.io {name} "
            f"-n {ns} --type=merge "
            f"-p '{{\"spec\":{{\"action\":\"expand\"}}}}'"
        )
        self.logger.info(
            f"[K8sUtils] Patching StorageCluster '{name}' to trigger expansion"
        )
        out, err = self._exec_kubectl(cmd)
        return out, err

    def wait_cluster_settled(self, name: str = "simplyblock-cluster",
                             timeout: int = 1800,
                             namespace: str | None = None) -> str:
        """Wait for the StorageCluster to leave Rebalancing and reach Online.

        Node operations are held while the cluster rebalances --
        storagenodeops_controller.go:794, "cluster %s is rebalancing; the
        operation resumes when it settles" -- so firing the next outage before
        the previous one has settled does not overlap the two, it parks the new
        operation in the gate and spends its step deadline waiting.

        Waiting on the sbcli status does not catch it. A rebalancing cluster
        reports status ``active`` and only the CR's phase says Rebalancing, so
        the check that reads status alone sees a settled cluster and proceeds.
        Since 2026-09-28 the phase is published, which is what makes this
        possible at all.

        Degraded is not waited out. A node that is genuinely down keeps the
        cluster degraded indefinitely, and blocking here would turn the
        caller's own timeout into this one; the caller has already established
        the node it broke is back before getting here. Rebalancing is the
        transient this exists for.

        Returns the last phase seen, so a caller can log what it settled into.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl -n {ns} get storagecluster {name} "
                f"-o jsonpath='{{.status.phase}}' 2>/dev/null || true",
                supress_logs=True,
            )
            phase = (out or "").strip()
            if phase and phase != last:
                self.logger.info(f"[cluster] phase: {phase}")
                last = phase
            if phase in ("Online", "Degraded"):
                return phase
            if not phase:
                # No phase at all is an older operator that never publishes
                # one. Returning beats blocking for the full timeout on a
                # field that is never going to appear.
                self.logger.info(
                    "[cluster] no status.phase on this build; not waiting for "
                    "the cluster to settle")
                return ""
            time.sleep(10)

        self.logger.warning(
            f"[cluster] still {last or 'unknown'} after {timeout}s; continuing. "
            f"The next node operation will sit in the operator's cluster gate "
            f"until it settles.")
        return last

    def wait_spdk_pods_ready(self, expected_count: int, timeout: int = 600,
                              namespace: str | None = None) -> int:
        """Wait until at least *expected_count* snode-spdk pods are Running.

        Parameters
        ----------
        expected_count : int
            Minimum number of Running snode-spdk pods to wait for.
        timeout : int
            Maximum seconds to wait (default 600).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        int
            Number of Running pods once threshold is met.

        Raises
        ------
        TimeoutError
            If threshold is not met within *timeout* seconds.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pods -n {ns} -l role=simplyblock-storage-node "
                f"--no-headers 2>/dev/null || true",
                supress_logs=True,
            )
            running = 0
            for line in out.strip().splitlines():
                if "Running" in line:
                    running += 1
            if running >= expected_count:
                self.logger.info(
                    f"[K8sUtils] {running}/{expected_count} snode-spdk pods Running"
                )
                return running
            self.logger.info(
                f"[K8sUtils] Waiting for snode-spdk pods: "
                f"{running}/{expected_count} Running…"
            )
            time.sleep(10)
        raise TimeoutError(
            f"[K8sUtils] Only {running}/{expected_count} snode-spdk pods "
            f"Running after {timeout}s"
        )

    def patch_storage_node_migrate(self, node_uuid: str, target_worker: str,
                                     new_ssd_pcie: list[str] | None = None,
                                     reattach_volume: bool = False,
                                     name: str = "simplyblock-node",
                                     namespace: str | None = None):
        """Trigger node migration via a StorageNodeOps CR.

        Resolves the StorageNode CR name from the node UUID, then
        creates a ``StorageNodeOps`` CR with ``action=migrate``.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node to migrate.
        target_worker : str
            Kubernetes node name to migrate the storage node onto.
        new_ssd_pcie : list[str] | None
            PCIe addresses for new SSDs on the target worker.
        reattach_volume : bool
            Whether to reattach volumes after migration.
        name : str
            Unused (kept for backward compatibility).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        tuple[str, str]
            ``(ops_name, storage_node_cr)`` — the StorageNodeOps CR
            name and the resolved StorageNode CR name.
        """
        ns = namespace or self.namespace

        storage_node_cr = self.resolve_storage_node_cr_name(
            node_uuid, namespace=ns,
        )

        uuid_prefix = node_uuid[:8] if len(node_uuid) >= 8 else node_uuid
        worker_suffix = target_worker.replace(".", "-").replace("_", "-")
        ts = int(time.time())
        ops_name = f"migrate-{uuid_prefix}-to-{worker_suffix}-{ts}"

        self.logger.info(
            f"[K8sUtils] Migrating storage node {node_uuid} "
            f"(CR={storage_node_cr}) to worker '{target_worker}'"
            f" (newSsdPcie={new_ssd_pcie}, reattachVolume={reattach_volume})"
        )

        self.create_storage_node_ops(
            name=ops_name,
            storage_node_ref=storage_node_cr,
            action="migrate",
            target_worker_node=target_worker,
            reattach_volume=reattach_volume,
            new_ssd_pcie=new_ssd_pcie,
            namespace=ns,
        )

        return ops_name, storage_node_cr

    def patch_storage_node_restart(self, node_uuid: str,
                                    spdk_image: str | None = None,
                                    spdk_proxy_image: str | None = None,
                                    name: str = "simplyblock-node",
                                    namespace: str | None = None):
        """Trigger node restart via a StorageNodeOps CR.

        If *spdk_image* or *spdk_proxy_image* are provided, the
        StorageNodeSet is patched with the new images first (these
        are set-level config fields), then a ``StorageNodeOps`` CR
        with ``action=restart`` is created.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node to restart.
        spdk_image : str | None
            New SPDK container image (e.g. ``registry/spdk:tag``).
        spdk_proxy_image : str | None
            New SPDK proxy container image.
        name : str
            StorageNodeSet CR name for image patches
            (default ``simplyblock-node``).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        tuple[str, str]
            ``(ops_name, storage_node_cr)`` — the StorageNodeOps CR
            name and the resolved StorageNode CR name.
        """
        ns = namespace or self.namespace

        # If new images are provided, patch the StorageNodeSet first
        # (spdkImage/spdkProxyImage are set-level fields, not per-op)
        if spdk_image or spdk_proxy_image:
            patch_dict: dict = {"spec": {}}
            if spdk_image:
                patch_dict["spec"]["spdkImage"] = spdk_image
            if spdk_proxy_image:
                patch_dict["spec"]["spdkProxyImage"] = spdk_proxy_image

            patch_json = json.dumps(patch_dict)
            cmd = (
                f"kubectl patch storagenodesets.storage.simplyblock.io {name} "
                f"-n {ns} --type=merge -p '{patch_json}'"
            )
            self.logger.info(
                f"[K8sUtils] Patching StorageNodeSet '{name}' with new images"
                + (f" spdkImage={spdk_image}" if spdk_image else "")
                + (f" spdkProxyImage={spdk_proxy_image}" if spdk_proxy_image else "")
            )
            self._exec_kubectl(cmd)

        storage_node_cr = self.resolve_storage_node_cr_name(
            node_uuid, namespace=ns,
        )

        uuid_prefix = node_uuid[:8] if len(node_uuid) >= 8 else node_uuid
        ts = int(time.time())
        ops_name = f"restart-{uuid_prefix}-{ts}"

        self.logger.info(
            f"[K8sUtils] Restarting storage node {node_uuid} "
            f"(CR={storage_node_cr})"
        )

        self.create_storage_node_ops(
            name=ops_name,
            storage_node_ref=storage_node_cr,
            action="restart",
            namespace=ns,
        )

        return ops_name, storage_node_cr

    def validate_fio_job(self, job_name: str, namespace: str | None = None,
                         timeout: int = 600) -> bool:
        """Check Job succeeded and ALL pod logs have no FIO error keywords.

        Checks every pod created by the Job (not just the latest) so that
        failures from earlier attempts that were retried via backoffLimit
        are not silently masked.

        Returns True if valid.  Raises RuntimeError on failure.
        """
        ns = namespace or self.namespace

        # Quick pre-check: fail fast on image pull errors (unrecoverable).
        # PodInitializing and ContainerCreating are normal transient states
        # (e.g. init container running fio-warmup) — let them proceed.
        pod_name_pre = self.get_job_pod_name(job_name, namespace=ns)
        if pod_name_pre:
            detail = self.get_pod_status_detail(pod_name_pre, namespace=ns)
            reason = detail.get("reason", "")
            if reason in ("ErrImagePull", "ImagePullBackOff"):
                raise RuntimeError(
                    f"FIO Job '{job_name}' pod '{pod_name_pre}' never "
                    f"started: {reason} — {detail.get('message', '')}"
                )

        status = self.wait_job_complete(job_name, namespace=ns, timeout=timeout)
        if status != "succeeded":
            # Include pod status detail in the error for diagnostics
            diag = ""
            pod_name_diag = self.get_job_pod_name(job_name, namespace=ns)
            if pod_name_diag:
                detail = self.get_pod_status_detail(
                    pod_name_diag, namespace=ns
                )
                diag = (
                    f" (pod phase={detail.get('phase')}, "
                    f"reason={detail.get('reason')}, "
                    f"msg={detail.get('message', '')!r})"
                )
            raise RuntimeError(
                f"FIO Job '{job_name}' did not succeed "
                f"(status={status}){diag}"
            )
        pod_names = self.get_job_pod_names(job_name, namespace=ns)
        if not pod_names:
            self.logger.warning(
                f"[K8sUtils] Could not find pod for Job '{job_name}'; skipping log check"
            )
            return True
        for pod_name in pod_names:
            # Every container, not the default one. A warmup that failed used
            # to leave this check reporting a clean job.
            logs = self.get_pod_logs_all_containers(pod_name, namespace=ns)
            if not logs:
                continue
            logs_lower = logs.lower()
            # FIO numeric error codes (e.g. err=110, err=5). Report ALL of
            # them plus a sample of the io_u lines: one error code alone says
            # nothing about how much of the volume was affected, and the
            # read/write split is the first thing needed to tell a failover
            # path problem from a data-placement one.
            #
            # Note this check is reached even when the Job status is
            # "succeeded" -- FIO can exit 0 for the Job while having reported
            # I/O errors internally, which is exactly how a cluster-wide
            # write failure once looked like a single unlucky volume.
            err_codes = sorted(set(re.findall(r'\berr= ?([1-9]\d*)\b', logs)))
            if err_codes:
                io_u = re.findall(r'io_u error[^\n]*', logs)
                reads = sum(1 for line in io_u if "read offset" in line)
                writes = sum(1 for line in io_u if "write offset" in line)
                sample = chr(10).join(f"      {line}" for line in io_u[:3])
                raise RuntimeError(
                    f"FIO Job '{job_name}' pod '{pod_name}' reported "
                    f"err={','.join(err_codes)} "
                    f"({len(io_u)} io_u error line(s): {reads} read, "
                    f"{writes} write)"
                    + (f"{chr(10)}{sample}" if sample else "")
                )
            fail_words = ["error", "fail", "interrupt", "terminate"]
            for word in fail_words:
                if word in logs_lower:
                    raise RuntimeError(
                        f"FIO Job '{job_name}' pod '{pod_name}' logs "
                        f"contain '{word}'"
                    )
        return True

    # ── StorageBackup CRD operations ─────────────────────────────────────────

    # ── backup, the v1alpha2 way ─────────────────────────────────────────
    #
    # On-demand backup is a Kubernetes operation again. It was not, for a
    # while: v1alpha2's StorageBackup arrived as {clusterRef, backupID} --
    # a record of a backup that already existed -- and the only kind that
    # acted was StorageBackupOps, whose action enum held nothing but
    # Restore. create_storage_backup raised NotImplementedError through
    # that period rather than write a document that could not work.
    #
    # Manohar Reddy, 2026-10-13: "the changes related to on demand backups
    # is merged now". StorageBackup.spec.source is the request --
    # "Source asks for a new backup of a claim, the way a VolumeSnapshot
    # asks" -- and storagebackup_controller.go now exists to serve it.
    #
    # THREE SHAPES THAT DO NOT MATCH EACH OTHER, and getting one wrong is
    # a strict-decoding rejection rather than a warning:
    #
    #   StorageBackup.spec.clusterRef        STRING  (just the name)
    #   StorageBackupPolicy.spec.clusterRef  STRING  (just the name)
    #   StorageBackupOps.spec.clusterRef     OBJECT  {name, namespace}
    #   StorageBackupOps.spec.backupRef      OBJECT  {name, namespace}
    #
    # The old BackupRestore / BackupPolicy / BackupImport kinds still
    # exist and are still reconciled, but their controllers are named
    # legacy_* in the operator now. New work uses the three above.

    SB_KIND = "storagebackups.storage.simplyblock.io"
    SBOPS_KIND = "storagebackupops.storage.simplyblock.io"
    SBPOLICY_KIND = "storagebackuppolicies.storage.simplyblock.io"

    def create_storage_backup(self, name: str, pvc_name: str,
                              cluster_name: str = "simplyblock-cluster",
                              namespace: str | None = None):
        """Ask for a backup of a claim. Namespaced, and any namespace does.

        ``spec.source.claimName`` names a PVC in THIS object's namespace,
        so the backup is raised beside the claim it backs up. Only
        policy-created backups behave differently: those always land in
        the operator's own namespace, whatever the claim's, which dev
        called out explicitly -- so a test that looks for a policy's
        output in the application namespace will not find it.

        source ONLY, never alongside clusterRef. The CRD rejects both:

            set exactly one of source (to request a backup) or
            clusterRef (to record one the store holds)

        They are two different operations on one kind -- source asks the
        cluster to take a backup, clusterRef adopts one the object store
        already holds. Sending both failed validation on every create,
        which is why all 26 backup classes failed on 20261016 without a
        single backup being taken.

        cluster_name is accepted and ignored so callers need not change;
        the claim already determines the cluster.
        """
        ns = namespace or self.namespace
        yaml_content = (
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: StorageBackup\n"
            "metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            "spec:\n"
            "  source:\n"
            f"    claimName: {pvc_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] StorageBackup '{name}' in {ns}: backing up claim "
            f"'{pvc_name}' on cluster '{cluster_name}'")
        return self.apply_yaml(yaml_content, namespace=ns)

    #: What a finished backup looks like. Not "Done" -- the enum is
    #: Pending/Creating/Available/Failed, and waiting for a phase the CRD
    #: cannot produce times out after the full budget while the backup sat
    #: Available the whole time.
    SB_READY = "Available"

    def wait_storage_backup_done(self, name: str, timeout: int = 300,
                                 namespace: str | None = None) -> dict:
        """Poll until the StorageBackup reports Available."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        phase = ""
        while time.time() < deadline:
            res = self.get_resource_json(self.SB_KIND, name, namespace=ns)
            status = res.get("status") or {}
            phase = str(status.get("phase") or "")
            if phase == self.SB_READY:
                bid = ((status.get("backup") or {}).get("backupID") or "?")
                self.logger.info(
                    f"[K8sUtils] StorageBackup '{name}' is {phase} "
                    f"(backupID={bid})")
                return res
            if phase == "Failed":
                self._dump_backup_diagnostics(name, ns)
                raise AssertionError(
                    f"StorageBackup '{name}' failed: "
                    f"{status.get('message') or status}")
            self.logger.info(
                f"[K8sUtils] Waiting for StorageBackup '{name}' "
                f"(phase={phase or 'none yet'})")
            time.sleep(10)
        self._dump_backup_diagnostics(name, ns)
        raise TimeoutError(
            f"StorageBackup '{name}' did not reach {self.SB_READY} within "
            f"{timeout}s (last phase={phase or 'none'})")

    def get_storage_backup_id(self, name: str,
                              namespace: str | None = None) -> str:
        """The control plane's id for a completed backup, or ""."""
        res = self.get_resource_json(self.SB_KIND, name,
                                     namespace=namespace or self.namespace)
        return str(((res.get("status") or {}).get("backup")
                    or {}).get("backupID") or "")

    def list_storage_backups(self, namespace: str | None = None) -> list:
        """Every StorageBackup in a namespace, or in all of them.

        ``namespace="--all-namespaces"`` is worth having here: a policy
        writes its backups into the operator's namespace rather than the
        claim's, so a caller looking only where it put the claim finds
        nothing.
        """
        ns = namespace or self.namespace
        scope = ("--all-namespaces" if ns == "--all-namespaces"
                 else f"-n {ns}")
        out, _ = self._exec_kubectl(
            f"kubectl get {self.SB_KIND} {scope} -o json 2>/dev/null || true",
            supress_logs=True)
        try:
            return json.loads((out or "").strip() or "{}").get("items", [])
        except Exception:                             # noqa: BLE001
            return []

    def delete_storage_backup(self, name: str, namespace: str | None = None):
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl delete {self.SB_KIND} {name} -n {ns} "
            f"--ignore-not-found --wait=false")

    def create_storage_backup_ops_restore(
            self, name: str, backup_name: str, claim_name: str,
            target_pool: str, backup_namespace: str | None = None,
            cluster_name: str = "simplyblock-cluster",
            cluster_namespace: str | None = None,
            namespace: str | None = None,
            claim_labels: dict | None = None,
            claim_annotations: dict | None = None):
        """Restore a backup into a NEW claim, in this object's namespace.

        The restore lands where the StorageBackupOps lives, which is what
        makes cross-namespace restore expressible: the backup is named
        with its own namespace in ``backupRef``, and the claim is created
        in the operation's.

        ``clusterRef`` and ``backupRef`` are OBJECTS here even though
        StorageBackup spells the same word as a plain string. Passing a
        string is worse than an error: the operator's own startup check
        treats a string ref as "the old format", refuses to start the
        StorageBackupOps controller at all, and logs
        "StorageBackupOps controller not started: these operations use the
        old string..." -- so one stale object disables every restore in
        the cluster, not just its own.
        """
        ns = namespace or self.namespace
        b_ns = backup_namespace or ns
        c_ns = cluster_namespace or self.namespace

        def _block(label, d):
            if not d:
                return ""
            out = f"    {label}:\n"
            for k, v in d.items():
                out += f"      {k}: \"{v}\"\n"
            return out

        yaml_content = (
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: StorageBackupOps\n"
            "metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            "spec:\n"
            "  action: Restore\n"
            "  clusterRef:\n"
            f"    name: {cluster_name}\n"
            f"    namespace: {c_ns}\n"
            "  backupRef:\n"
            f"    name: {backup_name}\n"
            f"    namespace: {b_ns}\n"
            "  restore:\n"
            f"    claimName: {claim_name}\n"
            f"    targetPool: {target_pool}\n"
            + _block("claimLabels", claim_labels)
            + _block("claimAnnotations", claim_annotations)
        )
        self.logger.info(
            f"[K8sUtils] StorageBackupOps '{name}' in {ns}: restore "
            f"{b_ns}/{backup_name} -> claim '{claim_name}' in pool "
            f"'{target_pool}'")
        return self.apply_yaml(yaml_content, namespace=ns)

    def wait_storage_backup_ops_done(self, name: str, timeout: int = 600,
                                     namespace: str | None = None) -> dict:
        """Poll until a StorageBackupOps reports Succeeded."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        phase = ""
        while time.time() < deadline:
            res = self.get_resource_json(self.SBOPS_KIND, name, namespace=ns)
            status = res.get("status") or {}
            phase = str(status.get("phase") or "")
            step = str((status.get("step") or {}).get("state") or "")
            if phase == "Succeeded":
                self.logger.info(
                    f"[K8sUtils] StorageBackupOps '{name}' Succeeded "
                    f"(claim={status.get('claimName')}, "
                    f"pv={status.get('persistentVolumeName')})")
                return res
            if phase in ("Failed", "Aborted"):
                self._dump_backup_diagnostics(name, ns)
                raise AssertionError(
                    f"StorageBackupOps '{name}' {phase}: "
                    f"{status.get('message') or status}")
            self.logger.info(
                f"[K8sUtils] Waiting for StorageBackupOps '{name}' "
                f"(phase={phase or 'none yet'}, step={step or '-'})")
            time.sleep(10)
        self._dump_backup_diagnostics(name, ns)
        raise TimeoutError(
            f"StorageBackupOps '{name}' did not succeed within {timeout}s "
            f"(last phase={phase or 'none'})")

    def delete_storage_backup_ops(self, name: str,
                                  namespace: str | None = None):
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl delete {self.SBOPS_KIND} {name} -n {ns} "
            f"--ignore-not-found --wait=false")

    def legacy_storage_backup_ops(self) -> list:
        """Ops written in the OLD string-ref format, which disable restore.

        The operator refuses to start its StorageBackupOps controller when
        any of these exist, cluster-wide. One left behind by an older run
        means every restore in the next run hangs in Pending with nothing
        obviously wrong, so a lane should check for them and say so rather
        than wait out its timeout.
        """
        out, _ = self._exec_kubectl(
            f"kubectl get {self.SBOPS_KIND} --all-namespaces -o json "
            f"2>/dev/null || true", supress_logs=True)
        try:
            items = json.loads((out or "").strip() or "{}").get("items", [])
        except Exception:                             # noqa: BLE001
            return []
        bad = []
        for it in items:
            spec = it.get("spec") or {}
            if isinstance(spec.get("backupRef"), str) or \
                    isinstance(spec.get("clusterRef"), str):
                m = it.get("metadata") or {}
                bad.append(f"{m.get('namespace')}/{m.get('name')}")
        return bad

    def create_storage_backup_policy(
            self, name: str, cluster_name: str = "simplyblock-cluster",
            schedule: str | None = None, max_versions: int | None = None,
            max_age: str | None = None,
            match_labels: dict | None = None,
            namespace: str | None = None):
        """A scheduled policy over a claim SELECTOR, not a named claim.

        Two differences from the legacy BackupPolicy that a test has to
        know. The policy picks its claims with ``claimSelector`` rather
        than being annotated onto one; and whatever namespace the claims
        live in, the StorageBackups it produces are created in the
        OPERATOR's namespace. Dev, 2026-10-13: "the StorageBackup created
        by backup policies behaviour is unchanged, they are always getting
        created in operator namespace which is: simplyblock".
        """
        ns = namespace or self.namespace
        body = (
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: StorageBackupPolicy\n"
            "metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            "spec:\n"
            f"  clusterRef: {cluster_name}\n"
        )
        if schedule:
            body += f"  schedule: \"{schedule}\"\n"
        if max_versions is not None:
            body += f"  maxVersions: {max_versions}\n"
        if max_age:
            body += f"  maxAge: \"{max_age}\"\n"
        if match_labels:
            body += "  claimSelector:\n    matchLabels:\n"
            for k, v in match_labels.items():
                body += f"      {k}: \"{v}\"\n"
        self.logger.info(
            f"[K8sUtils] StorageBackupPolicy '{name}': schedule={schedule} "
            f"maxVersions={max_versions} maxAge={max_age} "
            f"selector={match_labels}")
        return self.apply_yaml(body, namespace=ns)

    def delete_storage_backup_policy(self, name: str,
                                     namespace: str | None = None):
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl delete {self.SBPOLICY_KIND} {name} -n {ns} "
            f"--ignore-not-found --wait=false")

    def _dump_backup_diagnostics(self, backup_name: str,
                                  namespace: str) -> None:
        """Log diagnostic info when a StorageBackup times out or fails."""
        self.logger.error(
            f"[backup-diag] StorageBackup '{backup_name}' did not reach Done. "
            f"Dumping diagnostics..."
        )
        # 1. kubectl describe the StorageBackup CRD
        try:
            out, _ = self._exec_kubectl(
                f"kubectl describe storagebackup {backup_name} -n {namespace}",
                supress_logs=True,
            )
            self.logger.error(f"[backup-diag] describe storagebackup:\n{out}")
        except Exception as e:
            self.logger.warning(f"[backup-diag] describe failed: {e}")

        # 2. Recent events in the namespace related to backup
        try:
            out, _ = self._exec_kubectl(
                f"kubectl get events -n {namespace} --sort-by=.lastTimestamp "
                f"--field-selector involvedObject.name={backup_name} "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(f"[backup-diag] events:\n{out}")
            else:
                self.logger.error("[backup-diag] No events found for StorageBackup")
        except Exception as e:
            self.logger.warning(f"[backup-diag] events query failed: {e}")

        # 3. admin-control pod logs (last 50 lines) — the operator that should reconcile
        try:
            out, _ = self._exec_kubectl(
                f"kubectl logs -n {namespace} -l app=simplyblock-admin-control "
                f"--tail=50 --all-containers 2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(
                    f"[backup-diag] admin-control logs (tail 50):\n{out}"
                )
        except Exception as e:
            self.logger.warning(f"[backup-diag] admin-control logs failed: {e}")

        # 4. tasks pod backup runner logs (last 50 lines)
        try:
            out, _ = self._exec_kubectl(
                f"kubectl logs -n {namespace} -l app=simplyblock-tasks "
                f"--tail=50 --all-containers 2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(
                    f"[backup-diag] tasks pod logs (tail 50):\n{out}"
                )
        except Exception as e:
            self.logger.warning(f"[backup-diag] tasks pod logs failed: {e}")

    def find_storage_backup_by_id(self, backup_id: str) -> tuple:
        """(name, namespace) of the StorageBackup holding *backup_id*.

        Searched across ALL namespaces on purpose. A backup taken on
        demand lives beside its claim, but one a policy took lives in the
        operator's namespace whatever the claim's -- so looking only where
        the claim is finds half of them.

        Returns (None, None) when nothing matches, which is the caller's
        cue that the id is a control-plane id with no Kubernetes object
        behind it: backups taken through sbcli, or through a policy on a
        cluster this operator did not import, have none.
        """
        for item in self.list_storage_backups(namespace="--all-namespaces"):
            st = item.get("status") or {}
            if str((st.get("backup") or {}).get("backupID") or "") == str(backup_id):
                m = item.get("metadata") or {}
                return m.get("name"), m.get("namespace")
        return None, None

    def create_backup_restore(self, name: str, backup_ref_name: str,
                              pvc_name: str, pvc_size: str = "",
                              cluster_name: str = "simplyblock-cluster",
                              storage_class: str | None = None,
                              target_pool: str | None = None,
                              namespace: str | None = None):
        """Restore a backup into a new claim. Now a StorageBackupOps.

        The signature is the old BackupRestore one and the call sites are
        unchanged, but the object written is the v1alpha2 kind. Three
        things do not map one-to-one and are handled here rather than at
        fourteen call sites:

        * ``backup_ref_name`` is passed a control-plane backup ID by every
          caller in this suite, while StorageBackupOps.backupRef names a
          StorageBackup OBJECT. An id is looked up; a name is used as
          given.
        * ``pvc_size`` and ``storage_class`` have no counterpart. The
          operator creates the claim itself, sized from the backup. They
          are accepted and ignored so the call sites keep compiling, and
          logged once so nobody thinks they took effect.
        * ``target_pool`` is now REQUIRED by the CRD. A restore without
          one is refused at admission rather than failing later.
        """
        ns = namespace or self.namespace
        if not target_pool:
            raise ValueError(
                f"restore '{name}': target_pool is required by "
                f"StorageBackupOps.spec.restore and has no default -- the "
                f"operator will not guess which pool to restore into")
        if pvc_size or storage_class:
            self.logger.info(
                "[K8sUtils] restore '%s': pvc_size=%r and storage_class=%r "
                "are ignored; the operator sizes the claim from the backup "
                "and places it with targetPool", name, pvc_size,
                storage_class)

        # An object of that name is the ordinary case and is checked first.
        # Every producer in this suite returns the StorageBackup's name, so
        # looking the control-plane uuid up first meant the miss branch ran
        # on every restore and logged that nothing carried the id and the
        # operation might not resolve -- on restores that then worked. Only
        # a reference that is NOT an object here is worth a word.
        backup_name, backup_ns = backup_ref_name, ns
        if self.get_resource_json(self.SB_KIND, backup_ref_name,
                                  namespace=ns):
            self.logger.info(
                "[K8sUtils] restore '%s': StorageBackup %s/%s",
                name, ns, backup_ref_name)
        else:
            found, found_ns = self.find_storage_backup_by_id(backup_ref_name)
            if found:
                backup_name, backup_ns = found, found_ns
                self.logger.info(
                    "[K8sUtils] restore '%s': backup id %s is StorageBackup "
                    "%s/%s", name, backup_ref_name, found_ns, found)
            else:
                self.logger.warning(
                    "[K8sUtils] restore '%s': %r is neither a StorageBackup "
                    "in %s nor the id of one. If it is a control-plane id "
                    "with no Kubernetes object behind it -- an sbcli backup, "
                    "say -- the operation will not resolve it.",
                    name, backup_ref_name, ns)

        return self.create_storage_backup_ops_restore(
            name=name, backup_name=backup_name, claim_name=pvc_name,
            target_pool=target_pool, backup_namespace=backup_ns,
            cluster_name=cluster_name, namespace=ns)

    def wait_backup_restore_done(self, name: str, timeout: int = 300,
                                 namespace: str | None = None) -> dict:
        """Wait for a restore. Now a StorageBackupOps, same signature."""
        return self.wait_storage_backup_ops_done(
            name, timeout=timeout, namespace=namespace)

    def delete_backup_restore(self, name: str, namespace: str | None = None):
        return self.delete_storage_backup_ops(name, namespace=namespace)

    def create_backup_import(self, name: str,
                              source_cluster_name: str,
                              source_backup_id: str,
                              target_cluster_name: str,
                              namespace: str | None = None):
        """Create a BackupImport CRD to import a backup from another cluster.

        The operator will create a corresponding StorageBackup on the target
        cluster and handle source-switching automatically.
        """
        ns = namespace or self.namespace
        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha1\n"
            f"kind: BackupImport\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  sourceClusterName: {source_cluster_name}\n"
            f"  sourceBackupID: {source_backup_id}\n"
            f"  targetClusterName: {target_cluster_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating BackupImport '{name}' "
            f"(source={source_cluster_name}/{source_backup_id} "
            f"-> target={target_cluster_name})"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_backup_import_done(self, name: str, timeout: int = 300,
                                 namespace: str | None = None) -> dict:
        """Poll until BackupImport phase is ``Done``.  Returns resource JSON.

        The status will contain ``storageBackupRef`` — the name of the
        StorageBackup CRD created on the target cluster.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self.get_resource_json("backupimport", name, namespace=ns)
            phase = (res.get("status", {}).get("phase") or "").lower()
            if phase == "done":
                self.logger.info(f"[K8sUtils] BackupImport '{name}' is Done")
                return res
            if phase == "failed":
                raise AssertionError(
                    f"BackupImport '{name}' failed: {res.get('status')}")
            self.logger.info(
                f"[K8sUtils] Waiting for BackupImport '{name}' "
                f"(phase={res.get('status', {}).get('phase', 'unknown')})"
            )
            time.sleep(10)
        raise TimeoutError(
            f"BackupImport '{name}' not Done within {timeout}s"
        )

    def get_backup_import_storage_backup_ref(self, name: str,
                                              namespace: str | None = None) -> str:
        """Return the storageBackupRef from a BackupImport's status."""
        ns = namespace or self.namespace
        res = self.get_resource_json("backupimport", name, namespace=ns)
        return res.get("status", {}).get("storageBackupRef", "")

    def delete_backup_import(self, name: str, namespace: str | None = None):
        """Delete a BackupImport CRD."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting BackupImport '{name}'")
        self.delete_resource("backupimport", name, namespace=ns)

    # ── BackupPolicy CRD operations ──────────────────────────────────────────

    #: The label a policy selects its claims by. The API has no opinion --
    #: claimSelector is a plain label selector and the published example
    #: uses "backup: nightly" -- so the suite fixes one key and puts the
    #: policy's own name in the value. That keeps attach/detach a one-line
    #: label edit and keeps two policies in one namespace from selecting
    #: each other's claims.
    BACKUP_POLICY_LABEL = "backup"

    def create_backup_policy(self, name: str,
                             cluster_name: str = "simplyblock-cluster",
                             max_versions: int = 0, max_age: str = "",
                             schedule: str = "", namespace: str | None = None):
        """Create a backup policy. Now a v1alpha2 StorageBackupPolicy.

        Same signature as the legacy BackupPolicy helper, so the call
        sites are unchanged, but three things about the new kind change
        what a test can do with it:

        * claims attach by LABEL, not by an annotation naming the policy.
          annotate_pvc_backup_policy() does that instead now.
        * ``schedule``, ``maxVersions`` and ``maxAge`` are IMMUTABLE. A
          different schedule needs a new policy; editing one is refused.
        * a policy selects claims in its own namespace only, which is the
          StorageCluster's namespace -- and the StorageBackups it produces
          are created in the operator's namespace whatever the claim's.

        An absent claimSelector selects NOTHING and is reported with a
        SelectorEmpty event, while ``{}`` selects every claim. The
        selector is therefore always written, so a policy created here is
        never accidentally the second of those.
        """
        ns = namespace or self.namespace
        return self.create_storage_backup_policy(
            name=name, cluster_name=cluster_name, schedule=schedule or None,
            max_versions=max_versions or None, max_age=max_age or None,
            match_labels={self.BACKUP_POLICY_LABEL: name}, namespace=ns)

    def annotate_pvc_backup_policy(self, pvc_name: str, policy_name: str,
                                   namespace: str | None = None):
        """Attach a claim to a policy. A LABEL now, despite the name.

        The name is kept so the call sites do not move. The legacy kind
        read an annotation naming the policy; StorageBackupPolicy selects
        on labels, so attaching is "kubectl label pvc <claim>
        backup=<policy>" and detaching is removing it. Existing backups
        survive both.
        """
        ns = namespace or self.namespace
        self.logger.info(
            f"[K8sUtils] attaching PVC '{pvc_name}' to policy "
            f"'{policy_name}' via label "
            f"{self.BACKUP_POLICY_LABEL}={policy_name}")
        return self._exec_kubectl(
            f"kubectl label pvc {pvc_name} -n {ns} "
            f"{self.BACKUP_POLICY_LABEL}={policy_name} --overwrite")

    def remove_pvc_backup_policy_annotation(self, pvc_name: str,
                                            namespace: str | None = None):
        """Detach a claim from its policy by removing the label."""
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl label pvc {pvc_name} -n {ns} "
            f"{self.BACKUP_POLICY_LABEL}- 2>/dev/null || true")

    def delete_backup_policy(self, name: str, namespace: str | None = None):
        return self.delete_storage_backup_policy(name, namespace=namespace)

    def storage_backup_policy_status(self, name: str,
                                     namespace: str | None = None) -> dict:
        """attachedClaims and lastBackupAt, which is how a test knows the
        policy actually picked a claim up rather than silently selecting
        nothing."""
        res = self.get_resource_json(self.SBPOLICY_KIND, name,
                                     namespace=namespace or self.namespace)
        st = res.get("status") or {}
        return {"phase": st.get("phase"),
                "attachedClaims": st.get("attachedClaims") or [],
                "lastBackupAt": st.get("lastBackupAt")}

    def create_utility_pod(self, pod_name: str, pvc_name: str,
                           mount_path: str = "/spdkvol",
                           namespace: str | None = None,
                           node_name: str | None = None,
                           node_selector: str | None = None):
        """Create an alpine utility pod that mounts a PVC for checksum operations.

        node_name: hard-pin the pod to this node via ``spec.nodeName``, bypassing
            the scheduler entirely (including any StorageClass allowedTopologies
            restriction). Used to deliberately test scheduling a pod onto a node
            outside a DHCHAP pool's allowedNodes. When set, the client-node
            affinity/toleration block below is skipped — nodeName already forces
            exact placement.
        node_selector: pin via ``spec.nodeSelector`` on
            ``kubernetes.io/hostname`` instead, which keeps the SCHEDULER in the
            loop. Required for a StorageClass using
            ``volumeBindingMode: WaitForFirstConsumer`` -- with nodeName the
            scheduler never runs, so the claim stays at "waiting for first
            consumer" and the pod never starts whether the node is allowed or
            not, making a negative assertion pass vacuously. Mutually exclusive
            with node_name.
        """
        ns = namespace or self.namespace
        node_name_line = f"  nodeName: {node_name}\n" if node_name else ""
        if node_selector and node_name:
            raise ValueError(
                "create_utility_pod: pass node_name OR node_selector, not both")
        if node_selector:
            node_name_line = (
                "  nodeSelector:\n"
                f"    kubernetes.io/hostname: {node_selector}\n")
        # Build tolerations + nodeAffinity to match FIO job scheduling
        tolerations_block = ""
        node_affinity_block = ""
        if not node_name and not node_selector and self.has_client_nodes():
            node_affinity_block = (
                "    nodeAffinity:\n"
                "      requiredDuringSchedulingIgnoredDuringExecution:\n"
                "        nodeSelectorTerms:\n"
                "        - matchExpressions:\n"
                "          - key: node-role.kubernetes.io/client\n"
                "            operator: Exists\n"
            )
            tolerations_block = (
                "  tolerations:\n"
                "  - key: \"node-role\"\n"
                "    operator: \"Equal\"\n"
                "    value: \"client\"\n"
                "    effect: \"NoSchedule\"\n"
            )
        affinity_block = ""
        if node_affinity_block:
            affinity_block = (
                f"  affinity:\n"
                f"{node_affinity_block}"
            )
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: Pod\n"
            f"metadata:\n"
            f"  name: {pod_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"{node_name_line}"
            f"{affinity_block}"
            f"{tolerations_block}"
            f"  securityContext:\n"
            f"    seLinuxOptions:\n"
            f"      type: spc_t\n"
            f"  containers:\n"
            f"  - name: alpine\n"
            f"    image: alpine:3\n"
            f"    imagePullPolicy: IfNotPresent\n"
            f"    command: [\"sleep\", \"3600\"]\n"
            f"    volumeMounts:\n"
            f"    - mountPath: {mount_path}\n"
            f"      name: data-volume\n"
            f"  volumes:\n"
            f"  - name: data-volume\n"
            f"    persistentVolumeClaim:\n"
            f"      claimName: {pvc_name}\n"
            f"  restartPolicy: Never\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating utility pod '{pod_name}' with PVC "
            f"'{pvc_name}' at {mount_path}"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_pod_running(self, pod_name: str, timeout: int = 300,
                         namespace: str | None = None) -> bool:
        """Wait until pod is ``Running``.  Returns True on success."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pod {pod_name} -n {ns} "
                f"-o jsonpath='{{.status.phase}}' 2>/dev/null || true",
                supress_logs=True,
            )
            phase = out.strip()
            if phase == "Running":
                self.logger.info(f"[K8sUtils] Pod '{pod_name}' is Running")
                return True
            if phase in ("Failed", "Error"):
                self._dump_pod_diagnostics(pod_name, ns, f"entered {phase}")
                raise RuntimeError(
                    f"Pod '{pod_name}' entered {phase} state")
            time.sleep(5)
        # Say why before the caller deletes it. A checksum pod that never
        # starts used to be reported as "not Running within 600s" and nothing
        # else, and the caller's retry deletes the pod immediately -- so the
        # events, the container's waiting reason and the mount state all went
        # with it. TestBackupSecurityLvol burned two attempts and 20 minutes
        # that way on 2026-10-20 and left no evidence at all; all the logs
        # hold is "getting running pods" ten times over.
        self._dump_pod_diagnostics(pod_name, ns, f"not Running within {timeout}s")
        raise TimeoutError(
            f"Pod '{pod_name}' not Running within {timeout}s"
        )

    def _dump_pod_diagnostics(self, pod_name: str, namespace: str,
                              what: str) -> None:
        """Why a pod is not Running, read while it still exists.

        The three ways a pod stalls without ever logging anything: it is not
        scheduled, its image will not pull, or its volume will not mount.
        describe carries all three, and the pod's own log carries the fourth
        case where it started and died. Every read is bounded and independent,
        so one failure does not cost the others.
        """
        self.logger.error(
            f"[pod-diag] Pod '{pod_name}' {what}. Dumping diagnostics...")

        def _show(label, cmd):
            try:
                out, _ = self._exec_kubectl(cmd, supress_logs=True)
                out = (out or "").strip()
                if out:
                    self.logger.error("[pod-diag] %s:\n%s", label, out)
                else:
                    self.logger.error("[pod-diag] %s: <empty>", label)
            except Exception as exc:                          # noqa: BLE001
                self.logger.warning(f"[pod-diag] {label} failed: {exc}")

        _show("pod", f"kubectl get pod {pod_name} -n {namespace} -o wide 2>&1")
        _show("detail", f"kubectl describe pod {pod_name} -n {namespace} 2>&1 | tail -45")
        _show("events", f"kubectl get events -n {namespace} --sort-by=.lastTimestamp "
                        f"2>&1 | grep -i {pod_name} | tail -20")
        _show("log", f"kubectl logs {pod_name} -n {namespace} --tail=40 "
                     f"--all-containers 2>&1")

    def csi_node_pod(self, node_name: str) -> tuple:
        """The CSI node-plugin pod on *node_name*, as (pod, container).

        It is the window onto a worker's own NVMe fabric.  The node plugin
        already mounts the host's ``/sys`` -- that is how it finds the
        devices it attaches -- so reading a subsystem's ANA state through
        it needs no privileged pod of our own, and nothing is created or
        scheduled to take a reading.

        Returns ``(None, None)`` rather than raising: a missing plugin pod
        makes a diagnostic unavailable, and a diagnostic must never be the
        thing that fails a run.
        """
        ns = self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pods -n {ns} --no-headers -o custom-columns="
            f"NAME:.metadata.name,NODE:.spec.nodeName,"
            f"PHASE:.status.phase 2>/dev/null || true",
            supress_logs=True,
        )
        pod = None
        for line in (out or "").splitlines():
            cols = line.split()
            if len(cols) < 3:
                continue
            name, node, phase = cols[0], cols[1], cols[2]
            low = name.lower()
            if node != node_name or phase != "Running":
                continue
            if "csi" in low and "node" in low:
                pod = name
                break
        if not pod:
            self.logger.warning(
                "[K8sUtils] no running CSI node pod on %s; the host's NVMe "
                "fabric cannot be read from here", node_name)
            return None, None
        # Pick the container that actually has the host mounts rather than
        # a registrar or liveness sidecar, which do not.
        out, _ = self._exec_kubectl(
            f"kubectl get pod {pod} -n {ns} -o jsonpath="
            f"'{{range .spec.containers[*]}}{{.name}}{{\"\\n\"}}{{end}}' "
            f"2>/dev/null || true",
            supress_logs=True,
        )
        names = [c.strip() for c in (out or "").splitlines() if c.strip()]
        container = next((c for c in names
                          if "csi" in c.lower() and "node" in c.lower()),
                         None) or next(
            (c for c in names if "registrar" not in c.lower()
             and "liveness" not in c.lower()), None)
        return pod, container

    def exec_in_pod_container(self, pod_name: str, container: str,
                              command: str, namespace: str | None = None,
                              timeout: int = 15) -> str:
        """Run *command* in one container of a pod. Returns stdout only.

        The default timeout is short because the caller for this is a
        periodic sampler: one that blocks stops sampling during whatever
        it was watching, and a gap in the samples reads as "nothing
        happened" rather than as a gap.
        """
        ns = namespace or self.namespace
        c = f"-c {container} " if container else ""
        out, _ = self._exec_kubectl(
            f"kubectl exec -n {ns} {pod_name} {c}-- sh -c "
            f"{shlex.quote(command)} 2>/dev/null || true",
            supress_logs=True, timeout=timeout,
        )
        return out or ""

    def exec_in_pod(self, pod_name: str, command: str,
                    namespace: str | None = None, timeout: int = 300) -> tuple:
        """Execute a command inside a running pod.  Returns (stdout, stderr).

        timeout is exposed because a raw-device FIO verify runs for minutes and
        the 300s default would cut it off mid-run, which reads as a failure of
        the storage rather than of the harness.
        """
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl exec {pod_name} -n {ns} -- "
            f"sh -c {shlex.quote(command)}",
            timeout=timeout
        )

    # ── Quiescing a FIO client before an outage that would isolate it ───
    #
    # cut_storage_network writes its DROP rules into the HOST's tables (the
    # SPDK pod is hostNetwork) with no port match, so the kernel nvme-tcp
    # initiator on that worker loses every path to every volume, whichever
    # storage node owns it. Judging the FIO pods that happen to be scheduled
    # there against a latency ceiling afterwards measures where the scheduler
    # put them -- run 40507342839 failed three volumes exactly that way. See
    # sbcli-rca-archive/k8s_rapid_client_isolated_by_its_own_outage_rca_20261027.md
    #
    # So the caller stops fio on that worker first. Not SIGSTOP: fio times a
    # synchronous IO across the pwrite() it is blocked in, so the stopped
    # seconds would be charged to that IO's latency -- the exact err=110 we
    # are avoiding. SIGTERM is fio's own clean stop, and it exits 128+15.

    #: Lines that exist only because we sent the signal.
    _FIO_SIGNAL_RE = re.compile(
        r"fio: terminating on signal \d+"
        r"|fio: job .*(?:aborted|interrupted)",
        re.IGNORECASE)

    #: A real failure of the workload, signal or no signal.
    FIO_IO_ERROR_MARKERS = (
        "io_u error", "verify failed", "bad magic header", "hdr_fail",
        "data mismatch", "checksum error", "input/output error",
    )
    _FIO_ERRNO_RE = re.compile(r"\berr=\s*([1-9]\d*)")

    def fio_io_errors_in(self, logs: str, source: str = "") -> list:
        """Real IO errors in a fio log, forgiving only our own SIGTERM.

        ``err=110`` is NOT forgiven. The client is stopped BEFORE the outage
        starts, so a latency breach recorded in this log happened while the
        cluster was still whole, and that is a finding in its own right
        rather than a consequence of the cut.
        """
        hits = []
        for line in (logs or "").splitlines():
            s = line.strip()
            if not s or self._FIO_SIGNAL_RE.search(s):
                continue
            low = s.lower()
            if (any(m in low for m in self.FIO_IO_ERROR_MARKERS)
                    or self._FIO_ERRNO_RE.search(low)):
                hits.append(f"{source}: {s[:160]}" if source else s[:160])
        return hits

    def stop_fio_in_job(self, job_name: str, namespace: str | None = None,
                        wait: int = 90) -> dict:
        """SIGTERM fio inside a Job's pod and hand back what it left behind.

        Returns ``{"pod", "node", "stopped", "logs", "reason"}``. ``stopped``
        is False when the pod could not be reached or fio was still running
        after *wait* seconds; the caller must read that as "do not cut this
        node", never as success.

        The log is read before the signal and comes back even when the
        stop fails, because it is the only copy anybody will ever judge: a
        terminated Job never reaches "succeeded", and the Job itself is
        replaced when the client resumes.
        """
        ns = namespace or self.namespace
        out = {"pod": "", "node": "", "stopped": False, "logs": "",
               "reason": ""}
        pod = self.get_job_pod_name(job_name, namespace=ns)
        if not pod:
            out["reason"] = f"no pod for job {job_name}"
            return out
        out["pod"] = pod
        out["node"] = self.get_pod_node_name(pod, namespace=ns)

        # Read it BEFORE the signal. A container that is already exiting can
        # refuse the read, and a Job with a non-zero backoffLimit replaces
        # the pod -- `kubectl logs` would then answer for a replacement that
        # has only just started, and the record we came for is gone.
        try:
            out["logs"] = self.get_pod_logs(pod, namespace=ns, tail=2000) or ""
        except Exception as exc:                          # noqa: BLE001
            out["reason"] = f"log read failed before stop: {str(exc)[:100]}"

        try:
            self.exec_in_pod(
                pod, "pkill -TERM -x fio || pkill -TERM fio || true",
                namespace=ns, timeout=30)
        except Exception as exc:                          # noqa: BLE001
            out["reason"] = f"could not exec into {pod}: {str(exc)[:140]}"
            return out

        deadline = time.time() + wait
        while time.time() < deadline:
            alive = ""
            try:
                alive = self.exec_in_pod_container(
                    pod, "",
                    "pgrep -x fio >/dev/null 2>&1 && echo RUNNING || echo GONE",
                    namespace=ns, timeout=15).strip()
            except Exception:                             # noqa: BLE001
                pass
            if alive.endswith("GONE"):
                out["stopped"] = True
                break
            # The pod leaving Running is the same answer by another route,
            # and it is the one we get once the container has exited and
            # exec has stopped working.
            if self.get_pod_status_detail(pod, namespace=ns).get("phase") in (
                    "Succeeded", "Failed"):
                out["stopped"] = True
                break
            time.sleep(3)
        else:
            out["reason"] = f"fio still running in {pod} after {wait}s"

        # Bonus read: fio writes its closing summary on the way out, so the
        # log is usually a little longer now. Keep whichever is longer, and
        # do not read at all if the Job has moved to a different pod -- that
        # one's log is about a run we never saw.
        if self.get_job_pod_name(job_name, namespace=ns) == pod:
            try:
                after = self.get_pod_logs(pod, namespace=ns, tail=2000) or ""
                if len(after) > len(out["logs"]):
                    out["logs"] = after
            except Exception:                             # noqa: BLE001
                pass
        if not out["logs"].strip():
            out["reason"] = (
                f"{out['reason']} (no log could be read from {pod})".strip())
        self.logger.info(
            "[K8sUtils] stop fio in %s on %s: %s%s", job_name,
            out["node"] or "?", "stopped" if out["stopped"] else "STILL UP",
            f" -- {out['reason']}" if out["reason"] else "")
        return out

    def find_files_in_pvc(self, pod_name: str,
                          mount_path: str = "/spdkvol",
                          namespace: str | None = None) -> list:
        """Find regular files in mount_path inside the pod."""
        out, _ = self.exec_in_pod(
            pod_name, f"find {mount_path} -maxdepth 2 -type f",
            namespace=namespace,
        )
        return [f.strip() for f in out.splitlines() if f.strip()]

    def generate_checksums_in_pvc(self, pod_name: str, files: list,
                                   namespace: str | None = None) -> dict:
        """Generate md5 checksums for files inside the pod.

        Returns ``{filepath: md5hash}`` dict.
        """
        if not files:
            return {}
        # Batch all files into a single md5sum call for efficiency
        file_list = " ".join(shlex.quote(f) for f in files)
        out, _ = self.exec_in_pod(
            pod_name, f"md5sum {file_list}",
            namespace=namespace,
        )
        checksums = {}
        for line in out.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                checksums[parts[1]] = parts[0]
        return checksums

    def delete_pod(self, pod_name: str, namespace: str | None = None,
                   wait: bool = False):
        """Delete a pod.

        When *wait* is True the call blocks until the pod is fully removed,
        preventing name-collision races when a new pod with the same name is
        created shortly after deletion.
        """
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting pod '{pod_name}'"
                         f"{' (waiting)' if wait else ''}")
        if wait:
            self._exec_kubectl(
                f"kubectl delete pod {pod_name} -n {ns} "
                f"--ignore-not-found --wait=true --timeout=60s"
            )
        else:
            self.delete_resource("pod", pod_name, namespace=ns)

    def operator_storage_class_name(self, pool_crd_name: str,
                                    cluster_cr_name: str | None = None,
                                    namespace: str | None = None) -> str:
        """Return the StorageClass the operator generated for a pool.

        Read from the pool, not derived. This built
        ``simplyblock-{namespace}-{clusterName}-{poolName}``, which is one
        segment longer than what the operator now writes -- DefaultStorageClassName
        (pool/assignment.go:87) returns ``"simplyblock-" + namespace + "-" +
        clusterName`` with no pool segment -- so every caller was handed a class
        that does not exist, and the PVCs, snapshots and clones that referenced
        it never bound.

        Deriving it at all was the mistake, and the operator says so: the name is
        derived "only because the operator has to choose one, and nothing reads
        it back: the pool records what was written in
        status.defaultStorageClassName". So that is what this reads. An authored
        class can be called anything, which no derivation could ever have found.

        Falls back to the current derivation only if the pool has no status yet,
        and says so, rather than returning a name nothing will match.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl -n {ns} get storagepool {pool_crd_name} "
            f"-o jsonpath='{{.status.defaultStorageClassName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        sc = (out or "").strip()
        if sc:
            return sc

        if not cluster_cr_name:
            out, _ = self._exec_kubectl(
                f"kubectl get storageclusters -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true",
                supress_logs=True,
            )
            names = [n.strip() for n in (out or "").strip().splitlines() if n.strip()]
            cluster_cr_name = names[0] if names else "simplyblock-cluster"
        fallback = f"simplyblock-{ns}-{cluster_cr_name}"
        self.logger.warning(
            f"[pool] StoragePool '{pool_crd_name}' has no "
            f"status.defaultStorageClassName yet; guessing '{fallback}'. If the "
            f"PVC does not bind, the pool had not been reconciled when this was "
            f"read."
        )
        return fallback

    def wait_storage_class_exists(self, sc_name: str, timeout: int = 300) -> bool:
        """Wait for the operator to generate *sc_name*."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get storageclass {sc_name} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true",
                supress_logs=True,
            )
            if (out or "").strip():
                self.logger.info(
                    f"[K8sUtils] operator StorageClass {sc_name!r} is present")
                return True
            time.sleep(5)
        self.logger.warning(
            f"[K8sUtils] operator StorageClass {sc_name!r} did not appear "
            f"within {timeout}s")
        return False

    def get_csi_topology_keys(self, node_name: str,
                              driver: str = "csi.simplyblock.io") -> list:
        """Return the topology keys the CSI node plugin registered on a node."""
        out, _ = self._exec_kubectl(
            f"kubectl get csinode {node_name} -o "
            f"jsonpath='{{.spec.drivers[?(@.name==\"{driver}\")].topologyKeys}}' "
            f"2>/dev/null || true",
            supress_logs=True,
        )
        raw = (out or "").strip()
        if not raw.startswith("["):
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []

    def restart_csi_node_driver(self, expect_topology_key: str | None = None,
                                expect_on_nodes: list | None = None,
                                timeout: int = 420) -> bool:
        """Re-register the CSI node driver so it picks up new node labels.

        The node plugin snapshots node labels as topology keys at REGISTRATION
        time only. A DHCHAP pool created afterwards labels its allowed nodes,
        but that label is not yet a topology domain -- so the operator's
        generated StorageClass, whose ``allowedTopologies`` keys off exactly
        that label, fails to provision with::

            ProvisioningFailed: topology map[topology.kubernetes.io/zone:...]
            from selected node "worker-0" is not in requisite:
            [map[simplyblock.io/pool....:allowed]]

        Restarting the daemonset makes every plugin re-register and report the
        pool label. Verified on OpenShift 2026-09-04: afterwards the allowed
        nodes carry the pool label as a topology key and non-allowed nodes do
        not.

        This is a workaround for a product gap, not a normal operation -- the
        operator ideally triggers the re-register itself after labelling, or
        omits allowedTopologies (dhchap_node_label alone already enforces at
        mount via the PV's nodeAffinity).
        """
        ns = self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get ds -n {ns} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true")
        names = [n.strip() for n in (out or "").splitlines() if n.strip()]
        ds = next((n for n in names
                   if "csi" in n.lower() and "node" in n.lower()), None)
        if not ds:
            self.logger.warning(
                f"[K8sUtils] no CSI node daemonset found in {ns}; "
                f"cannot re-register (daemonsets={names})")
            return False

        self.logger.info(f"[K8sUtils] restarting CSI node daemonset {ds!r}")
        self._exec_kubectl(f"kubectl rollout restart daemonset {ds} -n {ns}")
        self._exec_kubectl(
            f"kubectl rollout status daemonset {ds} -n {ns} --timeout={timeout}s")

        if not expect_topology_key:
            return True

        deadline = time.time() + 180
        pending = list(expect_on_nodes or [])
        while time.time() < deadline and pending:
            still = [n for n in pending
                     if expect_topology_key not in self.get_csi_topology_keys(n)]
            if not still:
                pending = []
                break
            pending = still
            time.sleep(5)
        if pending:
            self.logger.warning(
                f"[K8sUtils] after re-register, {expect_topology_key!r} is "
                f"still not a topology key on {pending}")
            return False
        self.logger.info(
            f"[K8sUtils] {expect_topology_key!r} is now a CSI topology key on "
            f"{expect_on_nodes}")
        return True

    def get_volume_attachments(self, pv_name: str | None = None) -> list:
        """Return VolumeAttachments, optionally filtered to a single PV.

        Each entry is ``{"name", "pv", "node", "attached"}``. VolumeAttachment
        is cluster-scoped, so no namespace applies.
        """
        out, _ = self._exec_kubectl(
            "kubectl get volumeattachments -o json 2>/dev/null || true",
            supress_logs=True,
        )
        if not (out or "").strip():
            return []
        try:
            items = json.loads(out).get("items", [])
        except (json.JSONDecodeError, AttributeError):
            return []
        result = []
        for va in items:
            spec = va.get("spec", {}) or {}
            pv = (spec.get("source", {}) or {}).get("persistentVolumeName", "")
            if pv_name and pv != pv_name:
                continue
            result.append({
                "name": (va.get("metadata", {}) or {}).get("name", ""),
                "pv": pv,
                "node": spec.get("nodeName", ""),
                "attached": bool((va.get("status", {}) or {}).get("attached")),
            })
        return result

    def wait_volume_detached(self, pv_name: str, timeout: int = 180) -> bool:
        """Block until no VolumeAttachment references *pv_name*.

        Deleting a pod does NOT detach its volume: the Pod object goes away
        while the VolumeAttachment survives until kubelet finishes unmounting
        and the CSI controller completes ``ControllerUnpublishVolume``. A new
        pod created against the same ReadWriteOnce claim in that window fails
        with ``FailedAttachVolume: Multi-Attach error ... already exclusively
        attached to one node``, which has nothing to do with what the test was
        trying to observe.

        ``delete_pod(wait=True)`` waits only for the Pod, so any caller that
        moves a volume between nodes has to wait for this as well. That
        knowledge previously lived inline in
        ``e2e_tests/upgrade_tests/k8s_major_upgrade.py`` and nowhere else.

        Returns True once detached, False on timeout. Deliberately does not
        force-delete the VolumeAttachment: removing one that is genuinely in
        use can strand the volume. Callers decide what a timeout means.
        """
        if not pv_name:
            return True
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            attachments = self.get_volume_attachments(pv_name)
            if not attachments:
                self.logger.info(
                    f"[K8sUtils] PV '{pv_name}' fully detached")
                return True
            last = attachments
            time.sleep(3)
        self.logger.warning(
            f"[K8sUtils] PV '{pv_name}' still has VolumeAttachment(s) after "
            f"{timeout}s: {last}. A pod created for this claim on another "
            f"node will hit 'Multi-Attach error'."
        )
        return False

    def delete_pod_and_wait_detached(self, pod_name: str, pvc_name: str | None = None,
                                     namespace: str | None = None,
                                     timeout: int = 180) -> bool:
        """Delete a pod and wait until its volume is genuinely detached.

        The safe way to hand a ReadWriteOnce volume from one node to the next.
        Returns True when the volume is detached (or there was nothing to wait
        for); False if the attachment outlived the timeout.
        """
        ns = namespace or self.namespace
        self.delete_pod(pod_name, namespace=ns, wait=True)
        if not pvc_name:
            return True
        pv_name = self.get_pvc_pv_name(pvc_name, namespace=ns)
        if not pv_name:
            return True
        return self.wait_volume_detached(pv_name, timeout=timeout)

    def wait_for_per_node_config(self, worker_node: str,
                                  configmap_name: str = "simplyblock-node-per-node-config",
                                  namespace: str | None = None,
                                  timeout: int = 120):
        """Wait until the per-node-config ConfigMap has an entry for *worker_node*.

        The operator updates this ConfigMap when it reconciles a StorageNode CR.
        The DaemonSet pod's ``node-env-writer`` init container reads the entry
        to set ``MAX_LVOL``, ``MAX_SIZE``, etc.  If the pod starts before the
        entry exists it gets ``MAX_LVOL=0`` and crashes.

        Args:
            worker_node: The K8s node name (e.g. ``worker-4.ocp.simplyblock.ai``).
            configmap_name: Name of the per-node-config ConfigMap.
            namespace: K8s namespace (defaults to ``self.namespace``).
            timeout: Max seconds to wait.
        """
        import time
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            cm = self.get_resource_json("configmap", configmap_name, namespace=ns)
            data = cm.get("data", {})
            if worker_node in data:
                self.logger.info(
                    f"[K8sUtils] per-node-config has entry for '{worker_node}': "
                    f"{data[worker_node][:120]}..."
                )
                return
            self.logger.info(
                f"[K8sUtils] Waiting for per-node-config entry for "
                f"'{worker_node}' (keys: {list(data.keys())})..."
            )
            time.sleep(10)
        self.logger.warning(
            f"[K8sUtils] per-node-config entry for '{worker_node}' not found "
            f"after {timeout}s — proceeding anyway (pod may still fail)"
        )

    def delete_storage_node_pods_on_worker(self, worker_node: str,
                                           namespace: str | None = None):
        """Delete storage-node DaemonSet pods running on a specific worker.

        Call this after the per-node-config ConfigMap has been updated for the
        worker (use ``wait_for_per_node_config`` first) so the DaemonSet
        recreates the pod with the correct configuration.
        """
        ns = namespace or self.namespace
        cmd = (
            f"kubectl get pods -n {ns} "
            f"--field-selector spec.nodeName={worker_node} "
            f"--no-headers -o custom-columns=NAME:.metadata.name"
        )
        out, _ = self._exec_kubectl(cmd, supress_logs=True)
        deleted = 0
        for line in (out or "").strip().splitlines():
            pod_name = line.strip()
            if not pod_name:
                continue
            if "simplyblock-storage-node-ds" in pod_name:
                self.logger.info(
                    f"[K8sUtils] Deleting stale storage-node pod "
                    f"'{pod_name}' on worker '{worker_node}'"
                )
                self._exec_kubectl(
                    f"kubectl delete pod {pod_name} -n {ns} "
                    f"--force --grace-period=0 --ignore-not-found"
                )
                deleted += 1
        if deleted:
            self.logger.info(
                f"[K8sUtils] Deleted {deleted} stale storage-node pod(s) "
                f"on worker '{worker_node}'"
            )
        else:
            self.logger.info(
                f"[K8sUtils] No stale storage-node pods found on "
                f"worker '{worker_node}'"
            )

    def verify_pvc_mount(self, pvc_name: str, namespace: str | None = None,
                         timeout: int = 120) -> tuple:
        """Create a temporary pod to verify a PVC is mountable.

        Returns ``(success: bool, message: str)``.
        The temporary utility pod is always cleaned up after verification.
        """
        import random
        import string
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        pod_name_safe = pvc_name[:40].lower().replace("_", "-")
        pod_name_v = f"mount-verify-{pod_name_safe}-{suffix}"
        ns = namespace or self.namespace
        try:
            self.create_utility_pod(pod_name_v, pvc_name, namespace=ns)
            self.wait_pod_running(pod_name_v, timeout=timeout, namespace=ns)
            out, _ = self.exec_in_pod(
                pod_name_v,
                "df -h /spdkvol && ls -la /spdkvol",
                namespace=ns,
            )
            return True, f"Mount OK: {out.strip()[:200]}"
        except TimeoutError as e:
            return False, f"Mount timeout: {e}"
        except RuntimeError as e:
            return False, f"Pod failed: {e}"
        except Exception as e:
            return False, f"Mount error: {e}"
        finally:
            try:
                self.delete_resource("pod", pod_name_v, namespace=ns)
            except Exception:
                pass


# ── K8s-native sbcli_utils replacement ──────────────────────────────────────


class K8sSbcliUtils:
    """
    Drop-in replacement for SbcliUtils in Kubernetes environments.

    All CLI calls are routed through ``kubectl exec`` into the
    simplyblock-admin-control pod via the provided K8sUtils instance.
    No REST API calls are made.

    Parameters
    ----------
    k8s : K8sUtils
        Connected K8sUtils instance.
    cluster_id : str
        Cluster UUID (used by commands that accept a cluster id).
    sbcli_cmd : str
        The CLI binary name inside the admin pod (default: ``sbcli``).
    """

    def __init__(self, k8s: K8sUtils, cluster_id: str, sbcli_cmd: str = "sbctl"):
        self.k8s = k8s
        self.cluster_id = cluster_id
        self.sbcli_cmd = sbcli_cmd
        self.logger = setup_logger(__name__)
        #: pools this object created, and so may delete. add_storage_pool
        #: hands back an existing pool whenever there is one, so holding a
        #: pool name is not the same as owning it.
        self._pools_created: set[str] = set()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _run(self, cmd: str) -> str:
        """Execute *cmd* in the admin pod and return stripped stdout."""
        out, _ = self.k8s.exec_sbcli(cmd)
        return out.strip()

    def _run_json(self, cmd: str):
        """Execute *cmd* in the admin pod and parse stdout as JSON."""
        raw = self._run(cmd)
        if not raw:
            self.logger.warning(f"[_run_json] Empty output from: {cmd}")
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            self.logger.warning(f"[_run_json] JSON parse error from: {cmd}\n  raw={raw[:200]}\n  error={e}")
            return []

    @staticmethod
    def _cli_output_is_error(stdout: str, stderr: str) -> bool:
        """Return True if a sbcli/kubectl-exec result indicates a failure.

        ``exec_sbcli`` discards the exit code, so failures surface either as a
        non-empty stderr (``command terminated with exit code N`` from kubectl,
        or the sbcli ``Error:`` line) or as an ``Error``/``Traceback`` string
        printed to stdout.  Mirrors how the REST ``SbcliUtils`` raises on a
        non-2xx response.
        """
        blob = f"{stdout or ''}\n{stderr or ''}"
        markers = (
            "command terminated with exit code",
            "Error:",
            "Traceback (most recent call last)",
            "usage:",  # argparse rejected the arguments
        )
        return any(m in blob for m in markers)

    def _raise_if_cli_error(self, stdout: str, stderr: str, context: str = ""):
        """Raise RuntimeError when a CLI result indicates failure."""
        if self._cli_output_is_error(stdout, stderr):
            detail = (stderr or "").strip() or (stdout or "").strip()
            raise RuntimeError(f"sbcli command failed ({context}): {detail}")

    # ── lvol methods ──────────────────────────────────────────────────────────

    def list_lvols(self):
        """Return ``{lvol_name: lvol_id}`` dict."""
        items = self._run_json(f"{self.sbcli_cmd} lvol list --json")
        return {item["Name"]: item["Id"] for item in items}

    def get_lvol_id(self, lvol_name):
        return self.list_lvols().get(lvol_name)

    def lvol_exists(self, lvol_name):
        return bool(self.get_lvol_id(lvol_name))

    def get_lvol_details(self, lvol_id):
        """Return ``[{uuid, lvol_name, node_id, nqn, status, ...}]``."""
        raw = self._run(f"{self.sbcli_cmd} lvol get {lvol_id} --json")
        data = self._json_or_empty(raw, f"lvol get {lvol_id}")
        if not data:
            return []
        return data if isinstance(data, list) else [data]

    def get_lvol_connect_str(self, lvol_name):
        """Return list of ``sudo nvme connect ...`` strings for the lvol.

        Injects ``--ctrl-loss-tmo -1`` so NVMe controllers never time out
        during a storage-node outage (matches bare-metal stress-test behaviour).
        """
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            self.logger.info(f"Lvol {lvol_name} does not exist. Exiting")
            return []
        out = self._run(f"{self.sbcli_cmd} lvol connect {lvol_id}")
        lines = [line for line in out.splitlines() if line.strip()]
        result = []
        for line in lines:
            # Replace existing --ctrl-loss-tmo <value> or --ctrl-loss-tmo=<value> with -1
            line = re.sub(r"--ctrl-loss-tmo[=\s]\S+", "--ctrl-loss-tmo -1", line)
            if "--ctrl-loss-tmo" not in line:
                line = line.rstrip() + " --ctrl-loss-tmo -1"
            result.append(line)
        return result

    def add_lvol(self, lvol_name, pool_name, size="256M", distr_ndcs=0, distr_npcs=0,
                 distr_bs=4096, distr_chunk_bs=4096, max_rw_iops=0, max_rw_mbytes=0,
                 max_r_mbytes=0, max_w_mbytes=0, host_id=None, retry=10,
                 crypto=False, fabric="tcp", cluster_id=None,
                 max_namespace_per_subsys=None, namespace=None):
        """Create an lvol via the CLI."""
        if self.lvol_exists(lvol_name):
            self.logger.info(f"LVOL {lvol_name} already exists. Skipping")
            return

        cmd = (
            f"{self.sbcli_cmd} -d lvol add"
            f" {shlex.quote(lvol_name)} {size} {shlex.quote(pool_name)}"
        )
        if host_id:
            cmd += f" --host-id {shlex.quote(host_id)}"
        if distr_ndcs and distr_npcs:
            cmd += f" --data-chunks-per-stripe {distr_ndcs} --parity-chunks-per-stripe {distr_npcs}"
        if fabric:
            cmd += f" --fabric {shlex.quote(fabric)}"
        if crypto:
            cmd += " --encrypt"
        # Namespace packing flags (parity with SbcliUtils REST body):
        #   max_namespace_per_subsys -> --max-namespace-per-subsys <N>
        #   namespace=True           -> --namespaced true
        # Without these the CLI creates an independent subsystem per lvol
        # (distinct NQN) instead of packing namespaces into a shared one.
        if max_namespace_per_subsys is not None:
            cmd += f" --max-namespace-per-subsys {int(max_namespace_per_subsys)}"
        if namespace:
            cmd += " --namespaced true"

        out, err = self.k8s.exec_sbcli(cmd)
        # CLI write ops do not raise on failure the way the REST client does;
        # surface an error so negative tests observe the expected exception.
        self._raise_if_cli_error(out, err, context=f"lvol add {lvol_name}")

    def delete_lvol(self, lvol_name, max_attempt=120, skip_error=False):
        """Delete lvol by name, retrying the delete command periodically
        if the lvol returns to online state (mirrors sbcli_utils behaviour)."""
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            if skip_error:
                self.logger.info(f"Lvol {lvol_name} not found. Continuing without delete.")
                return True
            raise Exception(f"No such Lvol {lvol_name} found!!")

        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol delete {lvol_id}")

        attempt = 0
        while attempt < max_attempt:
            lvols = self.list_lvols()
            if lvol_name not in lvols:
                self.logger.info(f"Lvol {lvol_name} deleted successfully!!")
                return True
            # Every 12 attempts, check status and retry delete if lvol is
            # back to online (e.g. delete failed during outage).
            if attempt > 0 and attempt % 12 == 0:
                try:
                    details = self.get_lvol_details(lvol_id=lvol_id)
                    cur_state = details[0]["status"] if details else "unknown"
                except Exception:
                    cur_state = "unknown"
                if cur_state == "online":
                    self.logger.info(f"Lvol {lvol_name} in online state. Retrying delete!")
                    self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol delete {lvol_id}")
            attempt += 1
            self.logger.info(f"Lvol {lvol_name} deletion in progress... ({attempt})")
            sleep_n_sec(5)

        if skip_error:
            return False
        raise Exception(f"Lvol {lvol_name} is not getting deleted!!")

    def delete_all_clones(self):
        """Delete all clone lvols (lvols with cloned_from_snap set).

        Must be called BEFORE delete_all_snapshots, because SPDK refuses
        to delete a snapshot that still has clones.
        """
        lvols = self.list_lvols()
        for name, lvol_id in lvols.items():
            details = self.get_lvol_details(lvol_id)
            # isinstance, not truthiness. get_lvol_details does not always
            # hand back a list of dicts -- a failed or empty read can come
            # back with a bool in the first slot, and `details[0].get(...)`
            # then raises "'bool' object has no attribute 'get'" out of
            # teardown, which is where it was seen: run 38248680886 lost
            # its clone cleanup twice that way. Teardown is the one place
            # that must not throw, because it runs after a failure and its
            # exception replaces the one worth reading.
            row = details[0] if isinstance(details, (list, tuple)) and details else None
            if isinstance(row, dict) and row.get("cloned_from_snap"):
                self.logger.info(f"Deleting clone lvol: {name}")
                try:
                    self.delete_lvol(lvol_name=name, skip_error=True)
                except Exception as e:
                    self.logger.warning(
                        f"Clone delete failed (continuing): {name}, err={e}"
                    )

    def delete_all_lvols(self):
        lvols = self.list_lvols()
        for name in list(lvols.keys()):
            self.logger.info(f"Deleting lvol: {name}")
            self.delete_lvol(lvol_name=name)

    def resize_lvol(self, lvol_id, new_size):
        out, err = self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol resize {lvol_id} {new_size}")
        self._raise_if_cli_error(out, err, context=f"lvol resize {lvol_id}")

    # ── storage node methods ──────────────────────────────────────────────────

    def get_storage_nodes(self):
        """Return ``{'results': [{uuid, mgmt_ip, status, is_secondary_node, ...}]}``."""
        items = self._run_json(f"{self.sbcli_cmd} sn list --json")
        results = []
        for item in items:
            uuid = item["UUID"]
            detail_raw = self._run(f"{self.sbcli_cmd} sn get {uuid}")
            detail = self._json_or_empty(detail_raw, f"sn get {uuid}")
            if detail:
                results.append(detail)
        return {"results": results}

    def _json_or_empty(self, raw, what):
        """Parse sbctl output that is normally JSON, tolerating that it is not.

        _run_json already does this for --json commands; these callers
        did not, and a control plane that answers with an empty string
        or a traceback is not an exceptional condition -- run
        20261014-184128 lost two whole classes to it. Node 0f28a2bb was
        stuck in_restart, `sbctl sn get` returned nothing, and the bare
        json.loads raised JSONDecodeError out of a status poll, which
        reads as a harness crash rather than as the node still being
        down. An empty list says "could not read it", which is what the
        callers already handle.
        """
        text = (raw or "").strip()
        if not text:
            self.logger.warning("[%s] empty output; treating as no result",
                                what)
            return []
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            self.logger.warning("[%s] not JSON (%s); raw=%r", what, exc,
                                text[:200])
            return []

    def get_storage_node_details(self, storage_node_id):
        """Return ``[{uuid, mgmt_ip, status, ...}]``."""
        raw = self._run(f"{self.sbcli_cmd} sn get {storage_node_id}")
        data = self._json_or_empty(raw, f"sn get {storage_node_id}")
        if not data:
            return []
        return data if isinstance(data, list) else [data]

    def get_management_nodes(self):
        """Return ``{'results': [{'mgmt_ip': ip, ...}]}`` via sbctl cp list."""
        items = self._run_json(f"{self.sbcli_cmd} cp list --json")
        results = []
        for item in items:
            results.append({
                "mgmt_ip": item.get("IP", ""),
                "uuid": item.get("UUID", ""),
                "hostname": item.get("Hostname", ""),
                "status": item.get("Status", ""),
            })
        return {"results": results}

    def get_all_nodes_ip(self):
        """Return ``(mgmt_node_ips, storage_node_ips)`` as lists of strings."""
        mgmt_data = self.get_management_nodes()
        mgmt_ips = [n["mgmt_ip"] for n in mgmt_data["results"]]

        sn_data = self.get_storage_nodes()
        sn_ips = [n["mgmt_ip"] for n in sn_data["results"]]

        return mgmt_ips, sn_ips

    def shutdown_node(self, node_uuid, expected_error_code=None, force=False):
        force_flag = " --force" if force else ""
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn shutdown {node_uuid}{force_flag}")

    def suspend_node(self, node_uuid, expected_error_code=None):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn suspend {node_uuid}")

    def restart_node(self, node_uuid, expected_error_code=None, force=False):
        force_flag = " --force" if force else ""
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn restart {node_uuid}{force_flag}")

    def wait_for_storage_node_status(self, node_id, status, timeout=60):
        """Wait for *node_id* to reach *status*.

        Careful with *timeout*: it counts POLLS, not seconds. Each pass costs
        an ``sbctl`` exec into the admin pod -- about 4.5s against this lab --
        plus the 1s sleep below, so ``timeout=600`` waits something closer to
        54 minutes than to 10. The message on the way out reports the real
        elapsed time so the two numbers cannot be confused again.

        Left as polls rather than quietly redefined: 161 call sites pass this,
        95 of them taking the default, and every one of those values was tuned
        against the behaviour as it is. Changing the unit here would shorten
        every wait in the suite roughly fivefold in one go.
        """
        started = time.time()
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        while timeout > 0:
            try:
                node_details = self.get_storage_node_details(node_id)
                actual_status = node_details[0]["status"]
                if actual_status in status_list:
                    return node_details[0]
                self.logger.info(
                    f"Expected Status: {status_list} / Actual Status: {actual_status}"
                )
            except (json.JSONDecodeError, IndexError, KeyError) as exc:
                # Transient failure — admin-control pod may be recycling.
                self.logger.warning(
                    f"[wait_for_storage_node_status] Transient error for "
                    f"{node_id}: {exc!r}, retrying..."
                )
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(
            f"Timed out waiting for node status, {node_id}, "
            f"Expected: {status_list}, Actual: {actual_status} "
            f"(waited {time.time() - started:.0f}s)"
        )

    def is_secondary_node(self, node_id):
        try:
            details = self.get_storage_node_details(node_id)
            return bool(details[0].get("is_secondary_node", False))
        except Exception:
            return False

    def get_node_without_lvols(self):
        """Return a single primary node UUID that has no lvols, or empty string."""
        nodes_with_lvols = self._nodes_with_lvols()
        for result in self.get_storage_nodes()["results"]:
            if not result.get("is_secondary_node") and result["uuid"] not in nodes_with_lvols:
                return result["uuid"]
        return ""

    def get_all_node_without_lvols(self):
        """Return all primary node UUIDs that have no lvols."""
        nodes_with_lvols = self._nodes_with_lvols()
        return [
            r["uuid"]
            for r in self.get_storage_nodes()["results"]
            if not r.get("is_secondary_node") and r["uuid"] not in nodes_with_lvols
        ]

    def _nodes_with_lvols(self):
        """Return set of node UUIDs that have at least one lvol."""
        nodes = set()
        for lvol_id in self.list_lvols().values():
            try:
                details = self.get_lvol_details(lvol_id)
                nodes.add(details[0].get("node_id"))
            except Exception:
                pass
        return nodes

    # ── pool methods ──────────────────────────────────────────────────────────

    def list_storage_pools(self):
        """Return ``{pool_name: pool_id}`` dict."""
        items = self._run_json(f"{self.sbcli_cmd} pool list --json")
        return {item["Name"]: item["UUID"] for item in items}

    def get_storage_pool_id(self, pool_name):
        return self.list_storage_pools().get(pool_name)

    def _storage_pool_crds(self, ns):
        """The StoragePool CRs in *ns*, or [] if they cannot be read.

        Unreadable is treated as none deliberately: the callers use this to
        decide whether a backend pool is reachable from k8s, and guessing yes
        on a failed kubectl is how a pool with no CR gets handed out again.
        """
        raw, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools -n {ns} -o json 2>/dev/null || true")
        if not (raw or "").strip():
            return []
        try:
            return json.loads(raw).get("items", []) or []
        except (json.JSONDecodeError, AttributeError, TypeError):
            return []

    def _pool_of_crd(self, crd, existing):
        """The backend pool a StoragePool CR reconciled into, or None.

        The operator sets the backend name, which "may differ from the CRD
        metadata.name" -- so the CR's own status is asked first and the name
        is only a fallback. A CR that has not reconciled into any pool
        ``sbcli pool list`` reports is not a pool anything can be provisioned
        from, and None says so rather than inventing one.
        """
        meta = crd.get("metadata") or {}
        status = crd.get("status") or {}
        for candidate in (status.get("poolName"), status.get("name"),
                          meta.get("name")):
            if candidate and candidate in existing:
                return candidate
        return None

    def _pools_with_crd(self, ns, existing):
        """*existing* narrowed to the pools a StoragePool CR backs, in order.

        Order is preserved because callers pick positionally and a stable pick
        keeps a shared pool shared across the classes in a run.
        """
        crds = self._storage_pool_crds(ns)
        backed = {name for name in
                  (self._pool_of_crd(crd, existing) for crd in crds)
                  if name}
        return {name: uuid for name, uuid in existing.items() if name in backed}

    def add_storage_pool(self, pool_name, cluster_id=None, max_rw_iops=0, max_rw_mbytes=0,
                         max_r_mbytes=0, max_w_mbytes=0, dhchap=False, allowed_nodes=None,
                         storage_class_parameters=None, dedicated=False):
        """Use an existing pool if any exist; only create via kubectl if none exist.

        Returns the actual pool name to use (may differ from *pool_name* if an
        existing pool with a different name was found).

        The operator creates pools from StoragePool CRDs. The pool name in the
        backend (visible via ``sbcli pool list``) is set by the operator and
        may differ from the CRD metadata.name.  This method waits for the
        operator to reconcile the pool so that the real pool name can be
        returned.
        """
        # 1. Check if sbcli already sees a pool
        existing = self.list_storage_pools()
        self.logger.info(f"[pool] existing pools (sbcli): {list(existing.keys())}")
        ns = self.k8s.namespace
        # Blind reuse is only safe for a caller that asked for nothing in
        # particular. A caller passing storage_class_parameters is asking for
        # a specific StorageClass shape (encryption, filesystem, ...), and
        # those are immutable once the class exists -- handing back an
        # arbitrary existing pool silently gives it the wrong one. That is how
        # a "non-DHCHAP pool" request came back as the DHCHAP pool, whose
        # StorageClass carries dhchap_node_label.
        # ...and only a pool the CSI driver can actually resolve. "Exists in
        # the control plane" is not the same as "usable from k8s": the driver
        # resolves a pool through its StoragePool CR, so a backend pool with
        # no CR in this namespace provisions nothing --
        #
        #   failed to resolve pool "encryption-pool":
        #   pool "encryption-pool" not found
        #
        # -- and every PVC the test makes stays Pending. Two pools on this lab
        # are exactly that. encryption-pool is created by the workflow only
        # when tls_enabled is true, but survives in the control plane after a
        # TLS run, so a later non-TLS run finds it listed with no CR behind
        # it; and simplyblock-cluster-default is the cluster's own pool, which
        # never had a CR at all. Both sort before simplyblock-pool, and the
        # pick below is positional, so the backup lane took encryption-pool on
        # every class.
        usable = self._pools_with_crd(ns, existing)
        if existing and not usable:
            self.logger.warning(
                "[pool] %d backend pool(s) exist but none has a StoragePool "
                "CR in '%s': %s. Creating one rather than handing back a pool "
                "the CSI driver cannot resolve.",
                len(existing), ns, list(existing))
        if (usable and not dedicated and not dhchap and not allowed_nodes
                and not storage_class_parameters):
            # The general-purpose pool by name, not whichever one the control
            # plane listed first. A caller on this path asked for nothing in
            # particular, and on a TLS run the first pool alphabetically is
            # encryption-pool -- a pool that exists to be bound to encrypted
            # StorageClasses. Handing it to every caller would make the pool a
            # test runs against depend on whether TLS happened to be on.
            actual = ("simplyblock-pool" if "simplyblock-pool" in usable
                      else next(iter(usable)))
            self.logger.info(f"[pool] Using existing pool '{actual}'")
            return actual

        # dedicated=True asks for a pool of this test's own outright. A test
        # that DELETES its pool needs one: reuse hands back the lab's shared
        # pool, and delete_storage_pool rightly declines to remove a pool the
        # caller did not create -- so the delete, and any recreate after it,
        # quietly do nothing and the case proves nothing.
        dedicated = bool(dedicated or dhchap or allowed_nodes
                         or storage_class_parameters)
        if existing and dedicated:
            # A caller with a specific dhchap/allowedNodes requirement must
            # NOT get an arbitrary existing pool handed back — reuse is only
            # safe here if that pool's own StoragePool CRD already has the
            # exact same dhchap + allowedNodes config. Other, non-DHCHAP
            # tests share pools freely via the blind-reuse path above; this
            # only narrows behavior for callers that actually asked for
            # security enforcement.
            wanted_nodes = sorted(allowed_nodes or [])
            crd_json, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepools -n {ns} -o json 2>/dev/null || true"
            )
            try:
                crds = json.loads(crd_json).get("items", []) if crd_json.strip() else []
            except (json.JSONDecodeError, AttributeError):
                crds = []
            matched = False
            for crd in crds:
                spec = crd.get("spec", {})
                # Against volumeDefaults, which is where both dhchap and
                # storageClassParameters went. Comparing the old names made
                # this match anything: spec.get("dhchap") and
                # spec.get("storageClassParameters") are absent on every
                # v1alpha2 pool, so the test reduced to {} == {} and True ==
                # True, and a caller asking for an encrypted DHCHAP pool was
                # handed whichever pool happened to exist.
                wanted_vd = {
                    k: _as_yaml_scalar(v) for k, v in
                    pool_volume_defaults(dhchap, storage_class_parameters).items()
                }
                have_vd = {
                    k: _as_yaml_scalar(v)
                    for k, v in (spec.get("volumeDefaults") or {}).items()
                }
                if (sorted(spec.get("allowedNodes", []) or []) == wanted_nodes
                        and have_vd == wanted_vd):
                    # The pool this CRD reconciled into, not whichever pool
                    # happens to sort first. Returning an arbitrary one threw
                    # away the match that was just made and handed a caller
                    # asking for a specific dhchap/allowedNodes shape a pool
                    # with some other shape -- the exact failure the matching
                    # above exists to prevent.
                    actual = self._pool_of_crd(crd, existing)
                    if actual is None:
                        continue
                    self.logger.info(
                        f"[pool] Existing CRD '{crd['metadata']['name']}' "
                        f"already matches requested dhchap={dhchap} "
                        f"allowedNodes={wanted_nodes} — reusing pool "
                        f"'{actual}'")
                    return actual
            if not matched:
                # HACK: suffix the pool name with a timestamp so it cannot
                # collide with (or be shadowed by) the pool other tests are
                # sharing. This exists only because the blind-reuse path
                # above is load-bearing for every non-DHCHAP caller — they
                # rely on being handed whatever pool already exists, so we
                # cannot simply delete leftovers. A cleaner design would be
                # one pool per test with no cross-test sharing at all; that
                # is a bigger change than this fix.
                pool_name = f"{pool_name}-{int(time.time()) % 10000}"
                self.logger.info(
                    f"[pool] No existing pool matches dhchap={dhchap} "
                    f"allowedNodes={wanted_nodes} — creating dedicated pool "
                    f"'{pool_name}'")

        k8s_resource_name = f"simplyblock-{pool_name.lower().replace('_', '-')}"

        # The operator derives a node label from this CRD name:
        #   simplyblock.io/pool.<ns>.<StorageCluster CR>.<CRD name>
        # A Kubernetes label key's name part (everything after the "/") is
        # capped at 63 chars, and the CSI driver writes that key into every
        # provisioned PV's nodeAffinity — so if it overflows, the PV is
        # rejected outright ("name part must be no more than 63 characters")
        # and the PVC never binds. Budget the CRD name against the real
        # namespace and cluster CR name and truncate rather than emit a name
        # that cannot be enforced.
        if dedicated:
            sc_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storageclusters -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
            )
            sc_names = [s.strip() for s in (sc_out or "").strip().splitlines() if s.strip()]
            cluster_cr = sc_names[0] if sc_names else "simplyblock-cluster"
            # len("pool.") + ns + "." + cluster_cr + "."
            fixed = 5 + len(ns) + 1 + len(cluster_cr) + 1
            budget = 63 - fixed
            if budget < 8:
                self.logger.warning(
                    f"[pool] namespace '{ns}' + cluster CR '{cluster_cr}' "
                    f"leave only {budget} chars for the pool label name — "
                    f"DHCHAP enforcement cannot be expressed as a label here")
            elif len(k8s_resource_name) > budget:
                truncated = k8s_resource_name[:budget].rstrip("-")
                self.logger.warning(
                    f"[pool] CRD name '{k8s_resource_name}' would make the "
                    f"operator's pool label exceed the 63-char limit "
                    f"({fixed + len(k8s_resource_name)} > 63) — truncating to "
                    f"'{truncated}'")
                k8s_resource_name = truncated
                pool_name = k8s_resource_name

        # 2. Check whether the CRD this call actually wants already exists.
        #    For a dedicated (dhchap/allowedNodes) request this MUST be
        #    scoped to our own resource name — checking "does any StoragePool
        #    CRD exist in the namespace" would find an unrelated leftover
        #    pool's CRD and skip creating ours entirely, then step 4 below
        #    would hand back whichever pool the operator lists first (often
        #    the unrelated one), silently dropping the dhchap/allowedNodes
        #    request. Non-dedicated calls keep the original "any CRD" check.
        if dedicated:
            crd_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepool {k8s_resource_name} -n {ns} "
                f"--no-headers -o custom-columns=NAME:.metadata.name "
                f"2>/dev/null || true"
            )
            existing_crds = [k8s_resource_name] if crd_out.strip() else []
        else:
            out, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepools -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
            )
            existing_crds = [r.strip() for r in out.strip().splitlines() if r.strip()]

        # 2b. If CRDs exist, check if they're stuck in Terminating.
        #     Previous test runs may have initiated deletion but finalizers
        #     blocked completion.  Wait up to 120s for them to disappear,
        #     then force-remove finalizers if still stuck.
        if existing_crds:
            term_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepools -n {ns} --no-headers "
                f"-o custom-columns="
                f"NAME:.metadata.name,DEL:.metadata.deletionTimestamp "
                f"2>/dev/null || true"
            )
            terminating = []
            for line in (term_out or "").strip().splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[1] != "<none>" and parts[0] in existing_crds:
                    terminating.append(parts[0])

            if terminating:
                self.logger.warning(
                    f"[pool] Found Terminating StoragePool CRDs: "
                    f"{terminating} — waiting for deletion to complete"
                )
                deadline = time.time() + 120
                while time.time() < deadline:
                    check_out, _ = self.k8s._exec_kubectl(
                        f"kubectl get storagepools -n {ns} --no-headers "
                        f"-o custom-columns=NAME:.metadata.name "
                        f"2>/dev/null || true"
                    )
                    remaining = [
                        r.strip() for r in
                        (check_out or "").strip().splitlines()
                        if r.strip() and r.strip() in existing_crds
                    ]
                    if not remaining:
                        self.logger.info(
                            "[pool] All Terminating CRDs deleted")
                        break
                    sleep_n_sec(5)
                else:
                    # Force-remove finalizers on stuck CRDs
                    self.logger.warning(
                        "[pool] CRDs still stuck after 120s — "
                        "removing finalizers to unblock deletion"
                    )
                    for crd_name in terminating:
                        try:
                            self.k8s._exec_kubectl(
                                f"kubectl patch storagepool {crd_name} "
                                f"-n {ns} --type merge "
                                f"-p '{{\"metadata\":{{\"finalizers\":[]}}}}'"
                            )
                            self.logger.info(
                                f"[pool] Removed finalizers from "
                                f"{crd_name}")
                        except Exception as e:
                            self.logger.warning(
                                f"[pool] Failed to patch {crd_name}: "
                                f"{e}")
                    sleep_n_sec(10)

                existing_crds = [c for c in existing_crds if c not in terminating]

        if not existing_crds:
            # 3. Create the CRD via kubectl apply.
            cid = cluster_id or self.cluster_id
            cluster_details = self.get_cluster_details(cluster_id=cid)
            # sbcli cluster get returns "cluster_name" (not "name")
            cluster_name = (
                cluster_details.get("cluster_name")
                or cluster_details.get("name")
                or cluster_details.get("Name", cid)
            )

            # Look up the StorageCluster CRD name from K8s to ensure
            # the StoragePool CRD references the correct CRD resource name.
            sc_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storageclusters -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
            )
            sc_names = [s.strip() for s in sc_out.strip().splitlines() if s.strip()]
            if sc_names:
                cluster_name = sc_names[0]
                self.logger.info(
                    f"[pool] Using StorageCluster CRD name '{cluster_name}' "
                    f"from K8s (found {len(sc_names)} CRD(s))"
                )
            else:
                self.logger.warning(
                    f"[pool] No StorageCluster CRDs found in namespace {ns}; "
                    f"falling back to cluster_name='{cluster_name}' from sbcli"
                )

            # v1alpha2. The older version needs the conversion webhook, which
            # these installs do not deploy, so a v1alpha1 apply fails outright
            # -- and the failure was invisible here, because existing_crds was
            # set to the name we had just tried rather than to what the cluster
            # holds. The run then waited the full 300s for a pool that had
            # never been created and reported "Operator may not have reconciled
            # the pool", which pointed at the operator instead of at the apply.
            #
            # Three fields moved. clusterName is clusterRef. dhchap and
            # storageClassParameters are both gone: what they carried is typed
            # fields under volumeDefaults, which the operator turns into the
            # pool's StorageClass. allowedNodes stays where it was.
            yaml_content = (
                f"apiVersion: storage.simplyblock.io/v1alpha2\n"
                f"kind: StoragePool\n"
                f"metadata:\n"
                f"  name: {k8s_resource_name}\n"
                f"  namespace: {ns}\n"
                f"spec:\n"
                f"  clusterRef: {cluster_name}\n"
            )
            if allowed_nodes:
                yaml_content += "  allowedNodes:\n"
                for node_name in allowed_nodes:
                    yaml_content += f"    - {node_name}\n"

            # volumeDefaults is immutable once set, because the StorageClass
            # parameters it produces are immutable in the Kubernetes API. A
            # caller wanting both plain and encrypted volumes still needs two
            # pools.
            defaults = pool_volume_defaults(dhchap, storage_class_parameters)
            if defaults:
                yaml_content += "  volumeDefaults:\n"
                for _k, _v in defaults.items():
                    yaml_content += f"    {_k}: {_as_yaml_scalar(_v)}\n"

            self.logger.info(
                f"[pool] Creating '{pool_name}' "
                f"(CRD={k8s_resource_name}, cluster={cluster_name}) via kubectl apply"
            )
            yaml_escaped = yaml_content.replace("'", "'\\''")
            self.k8s._exec_kubectl(f"echo '{yaml_escaped}' | kubectl apply -f -")

            # Read back what the cluster holds rather than asserting what we
            # sent. Setting this to the name we had just tried made a rejected
            # apply indistinguishable from a slow operator: the wait below
            # then blamed reconciliation for a CR that did not exist.
            readback = self.k8s._exec_kubectl(
                f"kubectl -n {ns} get storagepool {k8s_resource_name} "
                f"-o jsonpath='{{.metadata.name}}' 2>/dev/null") or ""
            if k8s_resource_name not in readback:
                raise RuntimeError(
                    f"[pool] StoragePool '{k8s_resource_name}' does not exist "
                    f"after apply, so the apply was rejected. The document "
                    f"sent was:\n{yaml_content}"
                    f"    kubectl -n {ns} get storagepool")
            existing_crds = [k8s_resource_name]
        else:
            self.logger.info(
                f"[pool] Found existing StoragePool CRD(s): {existing_crds} — "
                f"waiting for operator to reconcile"
            )

        # 4. Wait for operator to reconcile the StoragePool CRD into an actual
        #    pool visible via sbcli pool list. For a dedicated request, wait
        #    specifically for a pool name that wasn't already in `existing`
        #    at the start of this call — an unrelated pool that was already
        #    there is never a valid answer here, no matter how the dict
        #    orders. Use 300s timeout to handle slow reconciliation after
        #    pool deletion/recreation cycles.
        already_seen = set(existing.keys())
        for attempt in range(60):  # up to 300s
            pools = self.list_storage_pools()
            if dedicated:
                new_pools = [p for p in pools if p not in already_seen]
                if new_pools:
                    actual = new_pools[0]
                    self.logger.info(
                        f"[pool] Operator reconciled dedicated pool '{actual}' "
                        f"(attempt {attempt})"
                    )
                    self._pools_created.add(actual)
                    return actual
            elif pools:
                actual = next(iter(pools))
                self.logger.info(
                    f"[pool] Operator reconciled pool '{actual}' "
                    f"(attempt {attempt})"
                )
                self._pools_created.add(actual)
                return actual
            if attempt % 5 == 4:
                self.logger.info(
                    f"[pool] Still waiting for operator to reconcile "
                    f"StoragePool CRD(s) {existing_crds} (attempt {attempt})"
                )
            sleep_n_sec(5)

        # 5. Raise instead of silently returning a non-existent pool name.
        #    Returning the raw pool_name caused StorageClasses to reference
        #    pools that don't exist on the backend, leading to PVC bind failures.
        raise TimeoutError(
            f"[pool] Pool not visible in sbcli after 300s. "
            f"StoragePool CRD(s): {existing_crds}. "
            f"Operator may not have reconciled the pool."
        )

    def add_storage_pool_direct(self, pool_name, cluster_id=None, sbcli_cmd=None):
        """Create a pool directly via ``sbcli pool add`` (kubectl exec).

        Unlike ``add_storage_pool()``, this does NOT create a StoragePool CRD —
        it calls the CLI directly in the admin pod.  Use this for R25
        clusters that have no operator to reconcile StoragePool CRDs.

        sbcli_cmd: override the CLI binary name (e.g. "sbcli-dev" for R25).
                   Defaults to self.sbcli_cmd.

        Returns the pool name on success.
        """
        cli = sbcli_cmd or self.sbcli_cmd

        def _list_pools():
            items = self._run_json(f"{cli} pool list --json")
            return {item["Name"]: item["UUID"] for item in items}

        # 1. Check if sbcli already sees a pool
        existing = _list_pools()
        if existing:
            actual = next(iter(existing))
            self.logger.info(f"[pool] Using existing pool '{actual}'")
            return actual

        # 2. Create via CLI
        cid = cluster_id or self.cluster_id
        cmd = f"{cli} pool add {pool_name} {cid}"
        self.logger.info(f"[pool] Creating pool directly via CLI: {cmd}")
        out = self._run(cmd)
        self.logger.info(f"[pool] pool add output: {out}")

        # 3. Wait for pool to appear in pool list
        for attempt in range(30):  # up to 150s
            pools = _list_pools()
            if pools:
                actual = next(iter(pools))
                self.logger.info(
                    f"[pool] Pool '{actual}' visible after CLI create "
                    f"(attempt {attempt})"
                )
                return actual
            sleep_n_sec(5)

        raise TimeoutError(
            f"[pool] Pool '{pool_name}' not visible in sbcli after 150s "
            f"following direct CLI creation."
        )

    def pool_crd_exists(self, pool_name):
        """Check if a StoragePool CRD exists in K8s (with or without simplyblock- prefix).

        Returns True if the StoragePool CRD is found, False otherwise.
        """
        ns = self.k8s.namespace
        # Try with simplyblock- prefix first (add_storage_pool creates these)
        k8s_name = f"simplyblock-{pool_name.lower().replace('_', '-')}"
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools {k8s_name} -n {ns} "
            f"-o jsonpath='{{.metadata.name}}' 2>/dev/null || true"
        )
        if out.strip():
            return True
        # Try with exact pool_name (ensure_pool_exists creates these)
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools {pool_name} -n {ns} "
            f"-o jsonpath='{{.metadata.name}}' 2>/dev/null || true"
        )
        return bool(out.strip())

    def ensure_pool_exists(self, pool_name, cluster_id=None, encryption=False):
        """Verify a specific pool exists; create it via kubectl if missing.

        Unlike ``add_storage_pool`` (which reuses *any* existing pool), this
        method checks for a pool with exactly *pool_name* and only creates
        it when that specific pool is absent.

        Returns the pool name.
        """
        existing = self.list_storage_pools()
        if pool_name in existing:
            self.logger.info(f"[pool] Pool '{pool_name}' already exists")
            return pool_name

        # Pool does not exist — create it
        cid = cluster_id or self.cluster_id
        cluster_details = self.get_cluster_details(cluster_id=cid)
        # sbcli cluster get returns "cluster_name" (not "name")
        cluster_name = (
            cluster_details.get("cluster_name")
            or cluster_details.get("name")
            or cluster_details.get("Name", cid)
        )

        # Look up the StorageCluster CRD name from K8s to ensure
        # the StoragePool CRD references the correct CRD resource name.
        ns = self.k8s.namespace
        sc_out, _ = self.k8s._exec_kubectl(
            f"kubectl get storageclusters -n {ns} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
        )
        sc_names = [s.strip() for s in sc_out.strip().splitlines() if s.strip()]
        if sc_names:
            cluster_name = sc_names[0]
            self.logger.info(
                f"[pool] Using StorageCluster CRD name '{cluster_name}' "
                f"from K8s (found {len(sc_names)} CRD(s))"
            )
        else:
            self.logger.warning(
                f"[pool] No StorageCluster CRDs found in namespace {ns}; "
                f"falling back to cluster_name='{cluster_name}' from sbcli"
            )
        # v1alpha2, for the reason the other pool writer in this file states:
        # v1alpha1 needs a conversion webhook these installs do not deploy.
        #
        # The pool carries NO encryption setting. v1alpha2's spec is
        # allowedNodes/clusterRef/limits/volumeDefaults and volumeDefaults has
        # no encryption key, so the volumeDefaults.enableEncryption this used
        # to emit was rejected outright and failed every bring-up at
        # "Applying pools". Encryption is a StorageClass setting again,
        # attached to a pool by the
        # storage.simplyblock.io/{namespace,cluster,pool} labels -- see
        # create_storage_class. The encryption argument is kept so callers do
        # not have to change, and is logged rather than silently dropped.
        sc_params = ""
        if encryption:
            self.logger.info(
                "[pool] Pool %r requested with encryption=True. v1alpha2 "
                "StoragePool has no encryption field, so the pool is created "
                "plain; encryption comes from a StorageClass labelled to it. "
                "Use create_storage_class(encryption=True).", pool_name)

        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha2\n"
            f"kind: StoragePool\n"
            f"metadata:\n"
            f"  name: {pool_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  clusterRef: {cluster_name}\n"
            f"{sc_params}"
        )

        self.logger.info(
            f"[pool] Pool '{pool_name}' not found — creating "
            f"(cluster={cluster_name}, encryption={encryption}) via kubectl apply"
        )
        yaml_escaped = yaml_content.replace("'", "'\\''")
        self.k8s._exec_kubectl(f"echo '{yaml_escaped}' | kubectl apply -f -")

        # Wait up to 180s for the pool to become visible in sbcli
        for _ in range(36):
            pools = self.list_storage_pools()
            if pool_name in pools:
                self.logger.info(f"[pool] Pool '{pool_name}' is ready")
                return pool_name
            sleep_n_sec(5)
        raise RuntimeError(
            f"[pool] Pool '{pool_name}' not confirmed after kubectl apply "
            f"— operator did not reconcile the StoragePool CRD within 180s"
        )

    def add_host_to_pool(self, pool_id, host_nqn):
        """Run ``pool add-host <pool_id> <nqn>`` via kubectl exec.

        Registers a client NQN at pool level so it can connect to any
        DHCHAP-enabled volume in the pool.
        """
        out = self._run(f"{self.sbcli_cmd} pool add-host {pool_id} {host_nqn}")
        self.logger.info(f"[add_host_to_pool] pool={pool_id} nqn={host_nqn}: {out}")
        return out

    def remove_host_from_pool(self, pool_id, host_nqn):
        """Run ``pool remove-host <pool_id> <nqn>`` via kubectl exec."""
        out = self._run(f"{self.sbcli_cmd} pool remove-host {pool_id} {host_nqn}")
        self.logger.info(f"[remove_host_from_pool] pool={pool_id} nqn={host_nqn}: {out}")
        return out

    def disable_storage_pool(self, pool_name):
        """Set a pool's status to Inactive via ``pool disable <pool_id>``."""
        pool_id = self.get_storage_pool_id(pool_name)
        if not pool_id:
            raise RuntimeError(f"Pool {pool_name} not found; cannot disable")
        out, err = self.k8s.exec_sbcli(f"{self.sbcli_cmd} pool disable {pool_id}")
        self._raise_if_cli_error(out, err, context=f"pool disable {pool_name}")
        self.logger.info(f"[pool] Disabled pool '{pool_name}' ({pool_id})")
        return out

    def enable_storage_pool(self, pool_name):
        """Set a pool's status to Active via ``pool enable <pool_id>``."""
        pool_id = self.get_storage_pool_id(pool_name)
        if not pool_id:
            raise RuntimeError(f"Pool {pool_name} not found; cannot enable")
        out, err = self.k8s.exec_sbcli(f"{self.sbcli_cmd} pool enable {pool_id}")
        self._raise_if_cli_error(out, err, context=f"pool enable {pool_name}")
        self.logger.info(f"[pool] Enabled pool '{pool_name}' ({pool_id})")
        return out

    def delete_storage_pool(self, pool_name):
        """Delete a storage pool, if this object is the one that made it.

        add_storage_pool hands back an EXISTING pool whenever there is one,
        so the name a test holds is usually not a pool the test owns.
        Deleting it took the lab's shared simplyblock-pool out from under
        every later class: the 2026-10-17 gate run reused simplyblock-pool
        in one test, deleted it in that test's teardown, and the next test
        sat 300s on a PVC the CSI driver could not place --
        "failed to resolve pool".

        Only pools this object actually created are deletable. The caller is
        told rather than failed: tearing down a pool you share is not an
        error, it is simply not yours to do.
        """
        if pool_name not in self._pools_created:
            self.logger.info(
                "[pool] not deleting '%s': this object did not create it. "
                "Created here: %s", pool_name,
                sorted(self._pools_created) or "nothing")
            return
        self.logger.info(f"[pool] Deleting pool CRD '{pool_name}'")
        ns = self.k8s.namespace
        self.k8s._exec_kubectl(
            f"kubectl delete storagepools {pool_name} -n {ns} "
            f"--timeout=60s 2>/dev/null || true"
        )
        # Wait for THIS pool to go. The old test was "no pools at all left",
        # which on a cluster that always carries simplyblock-cluster-default
        # could never be true, so every delete ended on the warning below
        # whether it had worked or not.
        for _ in range(12):
            if pool_name not in self.list_storage_pools():
                self._pools_created.discard(pool_name)
                self.logger.info(f"[pool] Pool '{pool_name}' deleted")
                return
            sleep_n_sec(5)
        self.logger.warning(f"[pool] Pool '{pool_name}' may not be fully removed")

    def delete_all_storage_pools(self):
        """Delete the pools this object created. Not every pool there is.

        This used to delete every StoragePool CRD in the namespace, which on
        a shared lab means the cluster's own pools as well -- simplyblock-pool
        and encryption-pool are created once by the workflow and every class
        afterwards depends on them. One teardown removed them for the whole
        run.
        """
        ns = self.k8s.namespace
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools -n {ns} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
        )
        resources = [r.strip() for r in out.strip().splitlines() if r.strip()]
        skipped = [r for r in resources if r not in self._pools_created]
        if skipped:
            self.logger.info(
                "[pool] leaving %d pool(s) this object did not create: %s",
                len(skipped), ", ".join(sorted(skipped)))
        for res in resources:
            if res not in self._pools_created:
                continue
            self.logger.info(f"[pool] Deleting pool CRD '{res}'")
            self.k8s._exec_kubectl(
                f"kubectl delete storagepools {res} -n {ns} "
                f"--timeout=60s 2>/dev/null || true"
            )
            self._pools_created.discard(res)

    # ── cluster methods ──────────────────────────────────────────────────────

    def get_cluster_details(self, cluster_id=None):
        """Return cluster dict (includes ``status``, ``max_fault_tolerance``, etc.)."""
        cid = cluster_id or self.cluster_id
        raw = self._run(f"{self.sbcli_cmd} cluster get {cid}")
        return json.loads(raw)

    def get_cluster_tasks(self, cluster_id=None):
        """
        Return list of task dicts parsed from the ``cluster list-tasks`` table.

        Each dict contains: id, function_name, node_id, status,
        updated_at (ISO string), date (Unix timestamp int).

        Table columns: Task ID | Target ID | Function | Retry | Status | Result | Updated At
        Updated At format: "HH:MM:SS, DD/MM/YYYY"
        """
        cid = cluster_id or self.cluster_id
        out = self._run(f"{self.sbcli_cmd} cluster list-tasks {cid} --limit 0")
        tasks = []
        for line in out.splitlines():
            line = line.strip()
            # Skip border rows and header
            if not line or line.startswith("+") or "Task ID" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # Expect: ['', task_id, target_id, function, retry, status, result, updated_at, '']
            if len(parts) < 8:
                continue
            task_id = parts[1]
            target_id = parts[2]
            function_name = parts[3]
            status = parts[5]
            updated_at_raw = parts[7]

            # Skip rows that don't look like UUIDs
            if not task_id or len(task_id) != 36 or task_id.count("-") != 4:
                continue

            # Extract node_id from "NodeID:<uuid>" or leave None
            node_id = None
            if target_id.startswith("NodeID:"):
                node_id = target_id[len("NodeID:"):]

            # Parse "HH:MM:SS, DD/MM/YYYY" → ISO string + Unix timestamp
            date_ts = 0
            iso_str = updated_at_raw
            try:
                dt = datetime.strptime(updated_at_raw, "%H:%M:%S, %d/%m/%Y")
                dt = dt.replace(tzinfo=UTC)
                iso_str = dt.isoformat()
                date_ts = int(dt.timestamp())
            except Exception:
                pass

            tasks.append({
                "id": task_id,
                "function_name": function_name,
                "node_id": node_id,
                "status": status,
                "updated_at": iso_str,
                "date": date_ts,
            })
        return tasks

    def get_cluster_logs(self, cluster_id=None):
        """Return list of cluster log dicts (each has ``Message``, etc.)."""
        cid = cluster_id or self.cluster_id
        return self._run_json(f"{self.sbcli_cmd} cluster get-logs {cid} --json --limit 0")

    def get_cluster_status(self, cluster_id=None):
        """Return cluster status dict."""
        details = self.get_cluster_details(cluster_id)
        return details

    def list_migration_tasks(self, cluster_id=None):
        """Return raw task list (same shape as ``get_cluster_tasks``)."""
        cid = cluster_id or self.cluster_id
        tasks = self.get_cluster_tasks(cid)
        return {"results": tasks}

    # ── device / node capacity methods ────────────────────────────────────────

    def get_device_details(self, storage_node_id):
        """Return list of storage-device dicts for a storage node.

        `sbctl sn list-devices --json` does not return a list. It returns
        {"Storage Devices": [...], "JM Devices": [...]}, so returning it raw
        made every caller iterate the DICT KEYS -- two strings -- and anything
        doing dev.get(...) died with "'str' object has no attribute 'get'".

        Unwrapped to the storage devices so this matches the docker side, where
        /device/list/<id> returns the data devices only. The JM devices are
        available separately rather than silently mixed in, since a caller
        counting devices means data devices.
        """
        data = self._run_json(
            f"{self.sbcli_cmd} sn list-devices {storage_node_id} --json"
        )
        self.logger.info(f"Device Details: {data}")
        if isinstance(data, dict):
            return data.get("Storage Devices") or []
        return data or []

    def get_jm_device_details(self, storage_node_id):
        """Return the JM (journal) devices for a storage node."""
        data = self._run_json(
            f"{self.sbcli_cmd} sn list-devices {storage_node_id} --json"
        )
        if isinstance(data, dict):
            return data.get("JM Devices") or []
        return []

    def get_device_capacity(self, device_id):
        """Return capacity records for a device.

        ``sbctl sn get-capacity-device`` does not support ``--json``,
        so we parse the table output.
        """
        out = self._run(f"{self.sbcli_cmd} sn get-capacity-device {device_id}")
        records = []
        headers = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+"):
                continue
            parts = [p.strip() for p in line.split("|")]
            parts = [p for p in parts if p]
            if not headers:
                headers = [h.lower().replace(" ", "_") for h in parts]
                continue
            if len(parts) == len(headers):
                records.append(dict(zip(headers, parts)))
        return records

    def get_node_capacity(self, node_id, history=None):
        """Return capacity records for a storage node.

        ``sbctl sn get-capacity`` does not support ``--json``,
        so we parse the table output.
        """
        cmd = f"{self.sbcli_cmd} sn get-capacity {node_id}"
        if history:
            cmd += f" --history {history}"
        out = self._run(cmd)
        records = []
        headers = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+"):
                continue
            parts = [p.strip() for p in line.split("|")]
            parts = [p for p in parts if p]
            if not headers:
                headers = [h.lower().replace(" ", "_") for h in parts]
                continue
            if len(parts) == len(headers):
                records.append(dict(zip(headers, parts)))
        return records

    # ── pool methods ──────────────────────────────────────────────────────────

    def get_pool_by_id(self, pool_id):
        """Return pool dict for the given pool id."""
        data = self._run_json(f"{self.sbcli_cmd} pool get {pool_id} --json")
        return data

    def get_io_stats(self, cluster_id=None, time_duration=None):
        """
        Fetch last 10 minutes of I/O stats and return a single averaged dict so
        that ``validate_io_stats`` can assert read_io + write_io > 0 over the window.

        Keys: date, read_bytes, write_bytes, read_io, write_io.
        """
        _UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4}

        def _parse_bytes(val):
            """Convert human-readable size string (e.g. '108.8 MiB') to bytes."""
            try:
                parts = val.split()
                num = float(parts[0])
                unit = parts[1].lower() if len(parts) > 1 else "b"
                return num * _UNITS.get(unit, 1)
            except Exception:
                return 0.0

        def _parse_int(val):
            try:
                return int(val)
            except Exception:
                return 0

        cid = cluster_id or self.cluster_id
        out = self._run(f"{self.sbcli_cmd} cluster get-io-stats {cid} --history 10m")
        rows = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+") or "Date" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # ['', date, read_speed, read_iops, read_lat, write_speed, write_iops, write_lat, '']
            if len(parts) < 8:
                continue
            rows.append({
                "date": parts[1],
                "read_bytes": _parse_bytes(parts[2]),
                "write_bytes": _parse_bytes(parts[5]),
                "read_io": _parse_int(parts[3]),
                "write_io": _parse_int(parts[6]),
            })

        if not rows:
            return []

        n = len(rows)
        avg = {
            "date": f"avg({rows[0]['date']} … {rows[-1]['date']})",
            "read_bytes": sum(r["read_bytes"] for r in rows) / n,
            "write_bytes": sum(r["write_bytes"] for r in rows) / n,
            "read_io": sum(r["read_io"] for r in rows) / n,
            "write_io": sum(r["write_io"] for r in rows) / n,
        }
        self.logger.info(f"[io_stats] {n} samples averaged: {avg}")
        return [avg]

    def get_cluster_capacity(self):
        """Return list of capacity records (each has ``date``, ``size_used``, etc.)."""
        raw = self._run(f"{self.sbcli_cmd} cluster get-capacity {self.cluster_id} --json")
        return json.loads(raw)

    def wait_for_cluster_status(self, cluster_id=None, status="active", timeout=60):
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        while timeout > 0:
            cluster_details = self.get_cluster_details(cluster_id=cluster_id)
            actual_status = cluster_details.get("status")
            if actual_status in status_list:
                return cluster_details
            self.logger.info(f"Expected Status: {status_list} / Actual Status: {actual_status}")
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(
            f"Timed out waiting for cluster status, {cluster_id or self.cluster_id}, "
            f"Expected: {status_list}, Actual: {actual_status}"
        )

    def all_expected_status(self, value_dict, expected_status):
        value_match = []
        for key, value in value_dict.items():
            self.logger.info(f"Entity: {key}, Expected: {expected_status}, Actual: {value}")
            value_match.append(value in expected_status)
        self.logger.info(f"Value: {value_match}")
        return all(value_match)

    # ── snapshot methods ──────────────────────────────────────────────────────

    def add_snapshot(self, lvol_id: str, snapshot_name: str, retry: int = 10):
        out, err = self.k8s.exec_sbcli(
            f"{self.sbcli_cmd} -d snapshot add {lvol_id} {shlex.quote(snapshot_name)}"
        )
        self._raise_if_cli_error(out, err, context=f"snapshot add {snapshot_name}")
        self.wait_for_snapshot(snapshot_name, present=True, timeout=60)

    def list_snapshots(self):
        """Parse snapshot list table output → ``{snap_name: snap_uuid}``.

        Table columns: | UUID | BDdev UUID | BlobID | Name | Size | BDev | Node ID | LVol ID | ...
        """
        out = self._run(f"{self.sbcli_cmd} snapshot list")
        result = {}
        for line in out.splitlines():
            parts = [p.strip() for p in line.split("|")]
            # parts[0]='' parts[1]=UUID parts[2]=BDdev UUID parts[3]=BlobID parts[4]=Name ...
            if len(parts) > 4:
                uuid_candidate = parts[1]
                name_candidate = parts[4]
                # UUID is a 36-char hyphenated string
                if (
                    len(uuid_candidate) == 36
                    and uuid_candidate.count("-") == 4
                    and name_candidate
                ):
                    result[name_candidate] = uuid_candidate
        return result

    def get_snapshot_id(self, snap_name: str):
        return self.list_snapshots().get(snap_name)

    def wait_for_snapshot(self, snap_name: str, present: bool = True, timeout: int = 60):
        """Poll until snap_name appears (present=True) or disappears (present=False)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            exists = snap_name in self.list_snapshots()
            if exists == present:
                return
            state = "appear" if present else "disappear"
            self.logger.info(f"[wait_for_snapshot] Waiting for '{snap_name}' to {state}...")
            time.sleep(3)
        state = "appear" if present else "disappear"
        raise TimeoutError(f"[wait_for_snapshot] '{snap_name}' did not {state} within {timeout}s")

    def delete_snapshot(self, snap_name: str | None = None, snap_id: str | None = None,
                        max_attempt: int = 60, skip_error: bool = False):
        if not snap_id:
            if not snap_name:
                raise ValueError("delete_snapshot requires snap_name or snap_id")
            snap_id = self.get_snapshot_id(snap_name)
        if not snap_id:
            if skip_error:
                self.logger.info(f"Snapshot not found (skip_error=True). snap_name={snap_name}")
                return
            raise Exception(f"Snapshot not found. snap_name={snap_name}")

        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d snapshot delete {snap_id}")

        resolve_name = snap_name or next(
            (k for k, v in self.list_snapshots().items() if v == snap_id), None
        )
        # Wait for it to disappear, retrying the delete command periodically
        attempt = 0
        while attempt < max_attempt:
            cur = self.list_snapshots()
            gone = True
            if resolve_name and resolve_name in cur:
                gone = False
            elif not resolve_name and snap_id in cur.values():
                gone = False
            if gone:
                self.logger.info(f"Snapshot {snap_name or snap_id} deleted successfully!")
                return
            if attempt > 0 and attempt % 12 == 0:
                self.logger.info(f"Snapshot {snap_name or snap_id} still present. Retrying delete!")
                self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d snapshot delete {snap_id}")
            attempt += 1
            sleep_n_sec(5)

        if skip_error:
            self.logger.warning(f"Snapshot {snap_name or snap_id} not deleted after {max_attempt} attempts")
            return
        raise Exception(f"Snapshot did not get deleted in time. snap_name={snap_name}, snap_id={snap_id}")

    def delete_all_snapshots(self):
        for snap_name in list(self.list_snapshots().keys()):
            try:
                self.delete_snapshot(snap_name=snap_name, skip_error=True)
            except Exception as e:
                self.logger.info(f"Snapshot delete failed (continuing): {snap_name}, err={e}")

    def add_clone(self, snapshot_id: str, clone_name: str):
        """Create a clone lvol from snapshot_id and wait for it to appear in lvol list."""
        out, err = self.k8s.exec_sbcli(
            f"{self.sbcli_cmd} -d snapshot clone {snapshot_id} {shlex.quote(clone_name)}"
        )
        self._raise_if_cli_error(out, err, context=f"snapshot clone {clone_name}")
        # Poll until the clone appears in lvol list
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.get_lvol_id(clone_name):
                self.logger.info(f"[add_clone] '{clone_name}' is now listed.")
                return out, err
            self.logger.info(f"[add_clone] Waiting for '{clone_name}' to appear in lvol list...")
            time.sleep(3)
        raise TimeoutError(f"[add_clone] '{clone_name}' did not appear in lvol list within 60s")

    # ── task / balancing methods ──────────────────────────────────────────────

    def get_task_subtasks(self, task_id: str) -> list:
        """
        Return list of subtask dicts for the given master task_id.

        Parses the output of ``cluster get-subtasks <task_id>`` which has
        8 data columns::

            | Task ID | Node ID | Distrib | Function | Retry | Status | Result | Updated At |

        Each dict contains: id, node_id, distrib, function_name, retry, status,
        result, updated_at.
        """
        try:
            out = self._run(f"{self.sbcli_cmd} cluster get-subtasks {task_id}")
        except Exception as e:
            self.logger.warning(f"[get_task_subtasks] Failed to fetch subtasks for {task_id}: {e}")
            return []

        subtasks = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+") or "Task ID" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # get-subtasks table layout (8 data columns):
            # ['', task_id, node_id, distrib, function, retry, status, result, updated_at, '']
            if len(parts) < 9:
                continue
            sub_id = parts[1]
            if not sub_id or len(sub_id) != 36 or sub_id.count("-") != 4:
                continue
            subtasks.append({
                "id": sub_id,
                "node_id": parts[2],
                "distrib": parts[3],
                "function_name": parts[4],
                "retry": parts[5],
                "status": parts[6],
                "result": parts[7],
                "updated_at": parts[8] if len(parts) > 8 else "",
            })
        return subtasks

    def _wait_for_balancing_subtasks(self, node_id: str, timeout: int = 600) -> None:
        """
        After a node comes back online, find the latest ``balancing_on_restart``
        master task and poll its subtasks until all are ``done``.

        Polls every 15 s for up to *timeout* seconds (default 10 min).
        Logs a warning (does not raise) if the timeout is reached so the test
        can continue to the health-check step.
        """
        self.logger.info(
            f"[balancing] Waiting for balancing_on_restart subtasks after node {node_id} recovery."
        )
        tasks = self.get_cluster_tasks(self.cluster_id)
        balancing_tasks = [t for t in tasks if "balancing_on" in t.get("function_name", "")]

        if not balancing_tasks:
            self.logger.info("[balancing] No balancing_on_restart tasks found. Skipping subtask check.")
            return

        # Use the most recently updated balancing task
        latest_task = max(balancing_tasks, key=lambda t: t["date"])
        task_id = latest_task["id"]
        self.logger.info(
            f"[balancing] Latest balancing task: {task_id} status={latest_task['status']}"
        )

        if latest_task["status"] == "done":
            self.logger.info(f"[balancing] Task {task_id} is already done.")
            return

        deadline = time.time() + timeout
        while time.time() < deadline:
            subtasks = self.get_task_subtasks(task_id)
            if not subtasks:
                self.logger.info(f"[balancing] No subtasks returned for {task_id} yet. Waiting 15s…")
                time.sleep(15)
                continue

            # Build status breakdown
            status_counts = {}
            for st in subtasks:
                s = st.get("status", "unknown")
                status_counts[s] = status_counts.get(s, 0) + 1
            total = len(subtasks)
            done_count = status_counts.get("done", 0)

            self.logger.info(
                f"[balancing] Task {task_id}: {done_count}/{total} subtasks done. "
                f"status_map: {status_counts}"
            )

            # Log individual non-done subtasks for debugging
            non_done = [st for st in subtasks if st.get("status") != "done"]
            for st in non_done:
                self.logger.info(
                    f"[balancing]   subtask {st['id'][:8]}… "
                    f"distrib={st.get('distrib', '?')} "
                    f"status={st.get('status', '?')} "
                    f"retry={st.get('retry', '?')} "
                    f"node={st.get('node_id', '?')[:8]}…"
                )

            if done_count == total:
                self.logger.info(f"[balancing] All {total} subtasks done for task {task_id}.")
                return

            time.sleep(15)

        self.logger.warning(
            f"[balancing] Timed out after {timeout}s waiting for subtasks of task {task_id}. "
            f"Proceeding to health-check anyway."
        )

    def wait_for_health_status(self, node_id, status, timeout=60, device_id=None,
                               wait_for_balancing=True):
        """
        K8s equivalent of SbcliUtils.wait_for_health_status.

        Before checking the node's ``health_check`` field this method first
        waits for all ``balancing_on_restart`` subtasks to complete (up to
        10 minutes), then polls the node health flag until it matches *status*.

        Pass ``wait_for_balancing=False`` to skip that first step and poll the
        health flag straight away. ``balancing_on_restart`` *is* data migration,
        so a test whose purpose is to fire the next outage while migration is
        still in flight must not call this with the wait enabled -- doing so
        drains migration on every iteration and quietly defeats the test. The
        docker equivalent (SbcliUtils.wait_for_health_status) has no such step,
        which is why this only bites on k8s.

        The ``device_id`` branch is not supported in K8s mode (no REST API);
        a warning is logged and the method returns None if device_id is given.
        """
        if device_id:
            self.logger.warning(
                "[K8s] wait_for_health_status: device_id branch not supported in K8s mode. "
                "Skipping device health check."
            )
            return None

        # Step 1: wait for balancing_on_restart subtasks to finish
        if wait_for_balancing:
            self._wait_for_balancing_subtasks(node_id, timeout=600)
        else:
            self.logger.info(
                f"[health_check] node={node_id}: skipping the balancing_on_restart "
                f"wait at the caller's request"
            )

        # Step 2: poll node health_check flag
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        node_details = None
        while timeout > 0:
            node_details = self.get_storage_node_details(node_id)
            actual_status = node_details[0].get("health_check")
            self.logger.info(
                f"[health_check] node={node_id} expected={status_list} actual={actual_status}"
            )
            if actual_status in status_list:
                return node_details[0]
            sleep_n_sec(1)
            timeout -= 1

        # Mirror sbcli_utils: if waiting for False and node is not offline, assert True
        if node_details and False in status_list and node_details[0].get("status") != "offline":
            assert actual_status is True, "Health Status not True for node not in offline state"
            return node_details[0]

        raise TimeoutError(
            f"Timed out waiting for health_check, node_id={node_id}, "
            f"Expected: {status_list}, Actual: {actual_status}"
        )
