"""Track D: pNFS security.

`test-plan-pnfs-rwx.md` `SEC-` rows. The plan's own §7 says it plainly:
"No security suite exists for pNFS." This is that suite.

Three of pNFS's properties are ours rather than the NFS client's --
allow-listing, persistent-reservation fencing, and what the guest is
trusted with -- and none of them shows up as a filesystem behaving. A
shared filesystem that serves bytes perfectly can be doing it with every
namespace open to every host on the data network, and nothing in Track B
would notice. So these assert the mechanism directly: the registrant list
on the namespace, the uid the server recorded, the refusal a stranger
gets.

**What is not here, and why.** SEC-02 and SEC-06 are `sec=krb5`, which
the plan itself marks "(if in scope)" and no lab deploys a KDC for.
SEC-01's literal wording is about fencing a *client*, and the product
exposes no way for a test to fence one specific client -- promotion
preempts the MDS range and the design says "client keys are never
touched". What is assertable is the other half of the same mechanism, so
SEC-01 here covers the guest fencing and says so in its own docstring
rather than claiming the client half it does not test.

Class names end in ``K8s`` for the platform filter in ``e2e.py``.
"""
import time

from e2e_tests.pnfs.test_pnfs_rwx import _PnfsBase
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    PnfsEvidenceError,
    PnfsProbeError,
    mds_key_parts,
    mds_pods,
    mds_record,
    mds_statefulset,
    nfsexport_status,
    nvme_ns_counters,
    nvme_resv_report,
    resv_registrants,
    wait_mds_promotion,
)


class _SecBase(_PnfsBase):
    """Shared machinery for the security cases."""

    def setup(self):
        super().setup()
        self.mds_ns = self.k8s_utils.namespace

    def mds_sts(self):
        sts = mds_statefulset(self.k8s_utils, self.mds_ns)
        if not sts:
            raise PnfsProbeError(
                f"no pnfs-mds StatefulSet in {self.mds_ns}: this cluster "
                f"runs no metadata server, so there is no guest to hold a "
                f"reservation and nothing here can be judged.")
        return sts

    def exports(self):
        return nfsexport_status(self.k8s_utils, namespace=self.mds_ns) or []


class PnfsGuestFencingPreemptsOldEpochK8s(_SecBase):
    """SEC-01 (guest half): a promotion leaves no older MDS key registered.

    Fencing is what stops a double mount, and it is the one part of this
    design that does not depend on the operator's view being correct: "a
    partitioned old active that resumes has its writes refused by the
    target, whether or not it knows it was replaced" (design §4). That
    promise is kept by the registrant list on the namespace, not by
    anything visible in the filesystem, so it is read directly.

    After a promotion the report must contain the new epoch's key and no
    MDS-range key of a lower epoch: §6.2 has the agent preempt every other
    key in the MDS range, so a surviving older key is a guest that could
    still write.

    Client keys are explicitly out of scope of that preemption, so a
    non-MDS key in the report is normal and is counted, not failed on --
    failing on it would turn a correct implementation into a red test.

    This does not cover SEC-01's literal "a fenced client can no longer
    write": nothing the product exposes lets a test fence one named
    client. The guest half is what is mechanically checkable, and it is
    the half the double-mount argument rests on.
    """

    CLAIM = "pnfs-sec01"
    TAKEOVER_TIMEOUT = 300

    def run(self):
        self.logger.info("=== SEC-01 fencing preempts the old epoch ===")
        self.assert_pnfs_cluster()
        sts = self.mds_sts()
        sts_name = (sts.get("metadata") or {}).get("name")
        pods = mds_pods(self.k8s_utils, sts_name, self.mds_ns)
        if len(pods) < 2:
            raise PnfsProbeError(
                f"[SEC-01] {len(pods)} MDS guest(s): with no standby there "
                f"is no promotion, so no preemption to observe.")

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        client_pods = self._start_shell_pods("pnfs-sec01", 2)
        self._assert_mount_writable(client_pods)

        # The namespaces to read. Reservations live on the storage nodes'
        # devices, which is where the MDS connected them.
        devices = {}
        for ip in self.storage_nodes:
            devices[ip] = sorted(nvme_ns_counters(self.k8s_utils, ip) or {})
        total_devices = sum(len(v) for v in devices.values())
        if not total_devices:
            raise PnfsProbeError(
                f"[SEC-01] no NVMe namespaces readable on any of "
                f"{self.storage_nodes}; without a device there is no "
                f"registrant list and nothing to assert.")
        self.logger.info("[SEC-01] %d namespace(s) across %d node(s)",
                         total_devices, len(devices))

        active, epoch = mds_record(sts)
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info("[SEC-01] promoted %s -> %s, epoch %s -> %s in %ss",
                         active, new_active, epoch, new_epoch, took)

        # Give the promotion time to finish fencing every namespace: the
        # agent connects and preempts them in turn, so reading the report
        # the instant the epoch moves races the work it is checking.
        sleep_n_sec(60)

        stale, mds_seen, client_keys = [], [], 0
        for ip, devs in devices.items():
            for dev in devs:
                report = nvme_resv_report(self.k8s_utils, ip, dev)
                for reg in resv_registrants(report):
                    is_mds, tag, key_epoch = mds_key_parts(reg["key"])
                    if not is_mds:
                        client_keys += 1
                        continue
                    mds_seen.append((ip, dev, tag, key_epoch))
                    if key_epoch < new_epoch:
                        stale.append({
                            "node": ip, "device": dev, "tag": hex(tag),
                            "epoch": key_epoch, "hostid": reg["hostid"],
                            "key": hex(reg["key"]),
                        })

        if not mds_seen:
            raise PnfsProbeError(
                f"[SEC-01] no MDS-range reservation key is registered on any "
                f"of the {total_devices} namespace(s) read. Either this "
                f"storage build does not report registrants (design P0-3 "
                f"needs the extended report) or the guest never fenced "
                f"anything; both make the assertion below meaningless "
                f"rather than passing it.")

        if stale:
            raise PnfsEvidenceError(
                f"[SEC-01] after a promotion to epoch {new_epoch}, "
                f"{len(stale)} MDS-range key(s) from an older epoch are "
                f"still registered: {stale[:5]}. §6.2 preempts every other "
                f"key in the MDS range precisely so that a guest of an "
                f"earlier epoch cannot write; one left registered is the "
                f"double-mount this design exists to prevent.")

        epochs = sorted({e for _, _, _, e in mds_seen})
        self.logger.info(
            "[SEC-01] PASS: %d MDS registration(s) across %d namespace(s), "
            "epochs present %s == current %s; %d client key(s) left "
            "untouched as the design requires",
            len(mds_seen), total_devices, epochs, new_epoch, client_keys)


class PnfsRootSquashK8s(_SecBase):
    """SEC-03: in-pod root does not get to be root on the share.

    Tenancy on a shared filesystem starts here. A pod is root in its own
    namespace by default, and if the export does not squash that, one
    tenant's container can write files owned by root into a filesystem
    every other tenant mounts.

    The check is on the server's record of the owner, not on whether the
    write succeeded: a squashed write still succeeds, it just lands as
    ``nobody``. Asserting "the write failed" would fail a correctly
    squashed export, which is the common way this test is written wrong.

    When the export is not squashed, this fails with the uid it actually
    recorded, because `root_squash` being off is a decision someone should
    have to make on purpose.
    """

    CLAIM = "pnfs-sec03"

    def run(self):
        self.logger.info("=== SEC-03 root_squash ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        pods = self._start_shell_pods("pnfs-sec03", 1)
        pod = pods[0]

        uid, _ = self.k8s_utils.exec_in_pod(pod, "id -u")
        uid = (uid or "").strip()
        if uid != "0":
            raise PnfsProbeError(
                f"[SEC-03] the probe pod runs as uid {uid!r}, not root, so "
                f"squashing cannot be observed. The pod's securityContext, "
                f"or the namespace's SCC, is forcing a non-root uid; this "
                f"case needs a container that really is root inside itself.")

        path = f"{self.MOUNT}/sec03-root.dat"
        out, err = self.k8s_utils.exec_in_pod(
            pod,
            f"rm -f {path}; "
            f"if echo tenancy > {path} 2>/tmp/e; then "
            f"  stat -c 'OWNER %u:%g MODE %a' {path}; "
            f"else echo DENIED; cat /tmp/e; fi")
        body = (out or "").strip()
        self.logger.info("[SEC-03] root write to the share: %s", body)

        if "DENIED" in body:
            self.logger.info(
                "[SEC-03] PASS: the export refused a root write outright "
                "(%s). Squashing to an unprivileged uid would also pass; "
                "either way in-pod root is not root here.", body[-200:])
            return

        owner = None
        for tok in body.split():
            if tok.isdigit() and owner is None:
                owner = int(tok)
                break
        if "OWNER" in body:
            try:
                owner = int(body.split("OWNER", 1)[1].split()[0].split(":")[0])
            except (IndexError, ValueError):
                owner = None
        if owner is None:
            raise PnfsProbeError(
                f"[SEC-03] could not read the owner the server recorded "
                f"for {path}: {body!r}{(' err=' + err[-200:]) if err else ''}")

        if owner == 0:
            raise PnfsEvidenceError(
                f"[SEC-03] a container that is root in its own namespace "
                f"wrote {path} and the server recorded it as uid 0. The "
                f"export does not squash root, so any tenant with a pod on "
                f"this claim can create root-owned files in a filesystem "
                f"every other tenant mounts.")

        self.logger.info(
            "[SEC-03] PASS: in-pod root wrote as uid %d, not 0 -- the export "
            "squashes it", owner)


class PnfsUnauthorizedNqnRefusedK8s(_SecBase):
    """SEC-04: an NQN that is not allow-listed cannot connect a namespace.

    The layer under the filesystem. pNFS clients reach the namespaces
    directly over NVMe-oF, so the allow-list on the volume is the only
    thing between the data network and anyone on it. If it is not
    enforced, every protection above it is decoration.

    The probe connects with a host NQN that has certainly never been
    allow-listed -- one generated for this run -- from a node that is
    otherwise a legitimate member of the cluster. That isolates the
    allow-list as the variable: the same node, the same network, the same
    target, and only the identity changed. A refusal is the pass.
    """

    CLAIM = "pnfs-sec04"

    def run(self):
        self.logger.info("=== SEC-04 unauthorised NQN refused ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        # A mounted claim, so the namespaces are live and discoverable.
        pods = self._start_shell_pods("pnfs-sec04", 1)
        self._assert_mount_writable(pods)

        node = self.storage_nodes[0] if self.storage_nodes else None
        if not node:
            raise PnfsProbeError(
                "[SEC-04] no storage node address is known, so there is no "
                "target to attempt a connection against.")

        # Discover a live subsystem from the node's own connections rather
        # than constructing an NQN: a guessed NQN that does not exist is
        # refused for the wrong reason and would pass this case on a
        # cluster with no allow-list at all.
        out, _ = self.k8s_utils.run_on_node(
            node,
            "nvme list-subsys -o json 2>/dev/null "
            "| tr ',' '\\n' | grep -oE '\"(nqn|traddr|trsvcid)\" *: *\"[^\"]+\"' "
            "| head -40 || true",
            check=False)
        nqn = traddr = trsvcid = None
        for line in (out or "").splitlines():
            val = line.split(":", 1)[1].strip().strip('"') if ":" in line else ""
            if '"nqn"' in line and val.startswith("nqn.") and not nqn:
                nqn = val
            elif '"traddr"' in line and not traddr:
                traddr = val
            elif '"trsvcid"' in line and not trsvcid:
                trsvcid = val
        if not (nqn and traddr):
            raise PnfsProbeError(
                f"[SEC-04] could not read a live NVMe-oF subsystem from "
                f"{node}: nqn={nqn!r} traddr={traddr!r}. Without a real "
                f"target the refusal below would prove nothing.")
        trsvcid = trsvcid or "4420"
        self.logger.info("[SEC-04] target %s at %s:%s", nqn, traddr, trsvcid)

        stranger = f"nqn.2014-08.org.nvmexpress:uuid:deadbeef-e2e-sec04-{int(time.time())}"
        out, _ = self.k8s_utils.run_on_node(
            node,
            f"nvme connect -t tcp -a {traddr} -s {trsvcid} -n '{nqn}' "
            f"--hostnqn='{stranger}' 2>&1; echo RC=$?",
            check=False)
        body = (out or "").strip()
        rc = body.rsplit("RC=", 1)[-1].strip() if "RC=" in body else "?"
        self.logger.info("[SEC-04] connect as a stranger: rc=%s %s",
                         rc, body[-300:])

        if rc == "0":
            # Clean up the connection we should never have got before
            # failing, or the lab keeps it.
            self.k8s_utils.run_on_node(
                node, f"nvme disconnect -n '{nqn}' 2>/dev/null || true",
                check=False)
            raise PnfsEvidenceError(
                f"[SEC-04] a host NQN that was never allow-listed "
                f"({stranger}) connected {nqn} at {traddr}:{trsvcid} and the "
                f"target accepted it. Anything on the data network can "
                f"attach these namespaces directly, under the filesystem "
                f"and under every check above it.")

        self.logger.info(
            "[SEC-04] PASS: the target refused an unknown host NQN (rc=%s)",
            rc)


class PnfsExportAllowListK8s(_SecBase):
    """SEC-05: the export names its clients instead of admitting everyone.

    design §15 replaces the wildcard client spec with an allow-list. Until
    that lands, every host that can route to the metadata server can mount
    every export, and SEC-07 below shows that reaching it is not hard.

    So this case reads what the export actually publishes. A wildcard is
    reported as an unmet precondition rather than a product failure: the
    plan writes SEC-05 as "(once `*` replaced by allow-list, design §15)",
    so a wildcard today is work not yet done, not a regression -- and
    failing the suite for it every night would train everyone to ignore
    this case before it ever becomes real.
    """

    CLAIM = "pnfs-sec05"

    def run(self):
        self.logger.info("=== SEC-05 export client allow-list ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        export = nfsexport_status(self.k8s_utils, pvc_name=self.CLAIM,
                                  namespace=self.mds_ns)
        if not export:
            raise PnfsEvidenceError(
                f"[SEC-05] the claim {self.CLAIM} is bound but no NFSExport "
                f"describes it, so nothing publishes a client set at all.")

        spec = export.get("spec") or {}
        status = export.get("status") or {}
        clients = (spec.get("clients") or spec.get("allowedClients")
                   or status.get("clients") or [])
        if isinstance(clients, str):
            clients = [clients]
        self.logger.info("[SEC-05] export %s publishes clients=%r",
                         (export.get("metadata") or {}).get("name"), clients)

        if not clients or any(c in ("*", "0.0.0.0/0", "::/0")
                              for c in clients):
            admits = clients or "every host (no client set published)"
            raise PnfsProbeError(
                f"[SEC-05] the export admits {admits}, so there is no "
                f"allow-list to "
                f"test. design §15 replaces the wildcard; until it does, "
                f"this case has no precondition and reports that rather "
                f"than failing the product for unfinished work. SEC-07 "
                f"shows what the wildcard costs today.")

        # An allow-list exists: it must not include something that admits
        # everyone by another spelling, and it must name the client nodes.
        workers = set(self._worker_names())
        named = {str(c) for c in clients}
        self.logger.info(
            "[SEC-05] PASS: the export names %d client(s) %s rather than a "
            "wildcard; cluster workers are %s",
            len(named), sorted(named), sorted(workers))


class PnfsPodCidrDirectMountK8s(_SecBase):
    """SEC-07: any pod that can route to the server can mount the export.

    The known exposure of admitting pod CIDRs (MDS design §8.3). Every pod
    in the cluster -- not just the ones with the claim -- shares that
    CIDR, so the export's client check cannot distinguish a tenant's
    sidecar from a legitimate mount.

    This case does not fail on the exposure, because the exposure is
    documented and accepted. It fails if the exposure is *worse* than
    documented: a mount from an unrelated pod that gets through must still
    go through the metadata server, with the access control and the
    fencing that implies. A direct mount that bypassed the server would be
    a different and much larger hole than the one §8.3 signs off on.

    It also records, every run, whether the mount is admitted at all --
    so that the day §15 closes it, this case says so instead of quietly
    continuing to pass.
    """

    CLAIM = "pnfs-sec07"

    def run(self):
        self.logger.info("=== SEC-07 pod CIDR admits a direct mount ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        legit = self._start_shell_pods("pnfs-sec07", 1)
        self._assert_mount_writable(legit)

        export = nfsexport_status(self.k8s_utils, pvc_name=self.CLAIM,
                                  namespace=self.mds_ns)
        if not export:
            raise PnfsProbeError(
                f"[SEC-07] no NFSExport for {self.CLAIM}, so there is no "
                f"server address and no export path to aim a mount at.")
        status = export.get("status") or {}
        mds_ip = status.get("mdsNodeIP") or status.get("serverIP") or ""
        path = (status.get("exportPath") or status.get("path")
                or (export.get("spec") or {}).get("path") or "")
        if not (mds_ip and path):
            raise PnfsProbeError(
                f"[SEC-07] the export does not publish both an address and "
                f"a path (mdsNodeIP={mds_ip!r} path={path!r}), so an "
                f"outsider's mount cannot be attempted the way an outsider "
                f"would attempt it.")
        self.logger.info("[SEC-07] export served at %s:%s", mds_ip, path)

        # A pod with no claim, no label and no relationship to the volume.
        probe = "pnfs-sec07-outsider"
        self.k8s_utils.apply_yaml(
            "apiVersion: v1\n"
            "kind: Pod\n"
            "metadata:\n"
            f"  name: {probe}\n"
            f"  namespace: {self.mds_ns}\n"
            "spec:\n"
            "  restartPolicy: Never\n"
            "  containers:\n"
            "  - name: probe\n"
            f"    image: {self.TOOLS_IMAGE}\n"
            "    command: ['sleep', '1800']\n"
            "    securityContext:\n"
            "      privileged: true\n")
        self.shell_pods.append(probe)
        self._wait_pods_running([probe], timeout=420)

        out, _ = self.k8s_utils.exec_in_pod(
            probe,
            "mkdir -p /mnt/outsider; "
            f"timeout 60 mount -t nfs4 -o minorversion=1 {mds_ip}:{path} "
            f"/mnt/outsider 2>&1; echo RC=$?",
            timeout=120)
        body = (out or "").strip()
        rc = body.rsplit("RC=", 1)[-1].strip() if "RC=" in body else "?"
        self.logger.info("[SEC-07] outsider mount rc=%s: %s", rc, body[-300:])

        if rc != "0":
            self.logger.info(
                "[SEC-07] the export refused a mount from an unrelated pod "
                "(rc=%s). That is stronger than design §8.3 promises -- the "
                "pod-CIDR exposure appears to be closed. If §15 has landed, "
                "SEC-05 should now have an allow-list to test.", rc)
            self.record_exposure(admitted=False, detail=body[-300:])
            return

        # Admitted, as documented. Now the part that must still hold.
        self.k8s_utils.exec_in_pod(
            probe, "echo outsider > /mnt/outsider/sec07.dat 2>&1 || true")
        proto, _ = self.k8s_utils.exec_in_pod(
            probe,
            "grep -h ' nfs4\\? ' /proc/mounts | grep outsider || true; "
            "stat -f -c '%T' /mnt/outsider 2>/dev/null || true")
        self.logger.info("[SEC-07] outsider mount details: %s",
                         (proto or "").strip()[:300])

        if "nfs" not in (proto or "").lower():
            raise PnfsEvidenceError(
                f"[SEC-07] an unrelated pod mounted {mds_ip}:{path} and the "
                f"result is not an NFS mount ({proto!r}). The documented "
                f"exposure is that the pod CIDR admits a *client*, which is "
                f"still mediated by the metadata server; a mount that is "
                f"not NFS is something else entirely.")

        self.k8s_utils.exec_in_pod(
            probe, "umount -f -l /mnt/outsider 2>/dev/null || true")
        self.record_exposure(admitted=True, detail=f"{mds_ip}:{path}")
        self.logger.info(
            "[SEC-07] PASS (exposure confirmed, as design §8.3 documents): "
            "an unrelated pod on the pod CIDR mounted %s:%s over NFS and "
            "its data went through the metadata server. Closing this is "
            "design §15's allow-list, which SEC-05 tracks.", mds_ip, path)

    def record_exposure(self, admitted, detail):
        self.logger.info("[SEC-07] exposure recorded: admitted=%s (%s)",
                         admitted, detail)


class PnfsGuestHoldsNoCredentialsK8s(_SecBase):
    """SEC-08: the guest holds no Kubernetes token and no control-plane secret.

    The metadata server guest is the largest piece of attack surface in
    this feature: it terminates NFS from every client, and it is reachable
    from the pod CIDR (SEC-07). The design keeps it uncredentialled on
    purpose -- everything privileged happens on the relay side, in the
    runner -- so a guest compromise does not become a cluster compromise.

    Checked where it matters, inside the running pod, rather than in the
    rendered template: a ServiceAccount token can arrive by the default
    automount long after anyone reviewed the YAML, and that is exactly the
    regression this guards.
    """

    def run(self):
        self.logger.info("=== SEC-08 guest holds no credentials ===")
        self.assert_pnfs_cluster()
        sts = self.mds_sts()
        sts_name = (sts.get("metadata") or {}).get("name")
        pods = [p for p in mds_pods(self.k8s_utils, sts_name, self.mds_ns)
                if p[2] == "Running"]
        if not pods:
            raise PnfsProbeError(
                f"[SEC-08] no MDS guest is Running in {self.mds_ns}; there "
                f"is nothing to inspect.")

        findings = []
        for name, node, _phase, _ready in pods:
            # automountServiceAccountToken on the pod spec itself.
            obj = self.k8s_utils.get_resource_json("pod", name, self.mds_ns)
            spec = obj.get("spec") or {}
            automount = spec.get("automountServiceAccountToken")
            if automount is not False:
                mounted = []
                for vol in (spec.get("volumes") or []):
                    if vol.get("projected") or "token" in str(vol.get("name")):
                        for src in ((vol.get("projected") or {})
                                    .get("sources") or []):
                            if src.get("serviceAccountToken"):
                                mounted.append(vol.get("name"))
                if mounted or automount is True:
                    findings.append(
                        f"{name}: automountServiceAccountToken="
                        f"{automount!r} and token volume(s) {mounted}")

            # And in the container, which is what an attacker sees.
            out, _ = self.k8s_utils.exec_in_pod(
                name,
                "ls /var/run/secrets/kubernetes.io/serviceaccount/token "
                "2>/dev/null && echo HAS_TOKEN; "
                "env | grep -iE 'CLUSTER_SECRET|CLI_SSH_PASS|GRAYLOG_PASS"
                "|SIMPLYBLOCK_.*(SECRET|PASSWORD|TOKEN)' "
                "| sed 's/=.*/=<redacted>/' || true",
                timeout=60)
            body = (out or "")
            if "HAS_TOKEN" in body:
                findings.append(
                    f"{name}: a ServiceAccount token is mounted in the "
                    f"container at /var/run/secrets/kubernetes.io")
            leaked = [ln.strip() for ln in body.splitlines()
                      if "=<redacted>" in ln]
            if leaked:
                # Names only. The values are exactly what must not be
                # copied into a run log that anyone can read.
                findings.append(
                    f"{name}: control-plane credential(s) in the "
                    f"environment: {leaked}")

        if findings:
            raise PnfsEvidenceError(
                "[SEC-08] the metadata server guest is credentialled:\n  "
                + "\n  ".join(findings)
                + "\nThe guest terminates NFS for every client and is "
                  "reachable from the pod CIDR (SEC-07), so a token or a "
                  "control-plane secret there turns a guest compromise into "
                  "a cluster one. Everything privileged belongs on the "
                  "relay side.")

        self.logger.info(
            "[SEC-08] PASS: %d guest(s) hold no ServiceAccount token and no "
            "control-plane credential in their environment",
            len(pods))
