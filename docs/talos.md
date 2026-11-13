Followed this guide to install TalOS: 
https://www.talos.dev/v1.10/introduction/getting-started/

Generated image using: 
https://factory.talos.dev/?platform=metal&target=metal


Before installing the storage cluster, the following additional preparation needs to be done. 

```
Kubectl create namespace simplyblock
kubectl label namespace simplyblock \
  pod-security.kubernetes.io/enforce=privileged \
  pod-security.kubernetes.io/enforce-version=latest \
  pod-security.kubernetes.io/warn=privileged \
  pod-security.kubernetes.io/warn-version=latest \
  pod-security.kubernetes.io/audit=privileged \
  pod-security.kubernetes.io/audit-version=latest \
  --overwrite
```

## Huge pages and the kubelet on Talos: two modes

On other distributions the storage node prepares its worker itself: a privileged
Job writes `nr_hugepages` per NUMA node, edits the kubelet's configuration file
and restarts the kubelet. The Talos host is immutable and has no shell, so both
settings live in the **machine config**, and there are two ways to get them
there. The StorageCluster's `storageNodes.talosMachineConfig` picks one; the
operator passes it to the node agent as `TALOS_MACHINE_CONFIG`.

| Mode | `talosMachineConfig` | Who writes the machine config | Talos API credentials in the namespace |
|------|----------------------|-------------------------------|----------------------------------------|
| Pre-provisioned (**default**) | `Preprovisioned` or unset | You, before the node is added | None |
| Managed | `Managed` | The node, through the Talos API, while it is added | An `os:admin` Secret (see below) |

### Where the mode is set

The mode is a field of the deployment document, `spec.cluster.talosMachineConfig`
on the `ClusterDeploymentConfig`, next to the cluster's name and sizing. The
operator copies it to `spec.storageNodes.talosMachineConfig` of the
StorageCluster it creates, and from there to every node agent as
`TALOS_MACHINE_CONFIG`. Unset means `Preprovisioned`. The document's CRD accepts
the field only when `spec.environment` is `Talos`; on any other environment it
is refused at `kubectl apply`.

```yaml
apiVersion: storage.simplyblock.io/v1alpha2
kind: ClusterDeploymentConfig
spec:
  environment: Talos
  cluster:
    name: mycluster
    talosMachineConfig: Managed      # Preprovisioned (default) | Managed
    enableCpuTopology: true          # default true on Talos
    enableKubeletConfiguration: true # default true on Talos
```

The two flags below the mode decide whether the kubelet's CPU topology is part
of it at all (see the next section); a Talos document turns both on unless it
states otherwise. A document produced by the operator's discovery run states
none of the three, so a discovered Talos cluster is pre-provisioned with the
kubelet included.

The mode can be changed on a running cluster by patching the StorageCluster:

```
kubectl -n simplyblock patch storagecluster mycluster --type=merge \
  -p '{"spec":{"storageNodes":{"talosMachineConfig":"Managed"}}}'
```

The node agents roll with the new value and apply it on the next node add or
restart; nodes that are online stay as they are.

In both modes the node first reads what the worker already has: the host's
huge-page pool (`/proc/meminfo`), the kubelet's advertised `hugepages-2Mi`
capacity, and the kubelet's live configuration (`nodes/<name>/proxy/configz`).
A worker that already carries everything is left alone in either mode; a plain
node restart never restarts the kubelet. The modes differ only in what happens
on a shortfall.

The huge-page pool is always part of this: the SPDK pod cannot start without
it. The kubelet's CPU topology (`cpuManagerPolicy: static`, the reserved CPUs,
the topology manager) is part of it only when the deployment asks for it --
`storageNodes.enableCpuTopology` *and* `storageNodes.enableKubeletConfiguration`
both on (the node agent's `CPU_TOPOLOGY_ENABLED=true` with
`SKIP_KUBELET_CONFIGURATION=false`). A Talos deployment turns both on by
default; a document that states either flag off keeps the node away from the
kubelet in both modes -- nothing is verified, nothing is applied, and the
kubelet is then yours to configure (or to leave alone).

The node detects Talos by itself, from what the kubelet reports about its host
in the Node object (`status.nodeInfo.osImage`, e.g. `Talos (v1.12.7)`), so no
flag is needed for that. `TALOS_CLUSTER=true|false` on the node agent overrides
the detection; the operator sets it from `spec.environment: Talos`.

### Pre-provisioned (default)

The node applies nothing. If the pool or the kubelet settings are short, the
add fails and the message carries the exact machine-config patch to apply,
sized for that worker, in the form `talosctl -n <node ip> patch mc -p
@patch.yaml` takes. No `os:admin` grant exists anywhere, which is what makes
this the default: the Talos API is never reachable from the cluster.

Put both settings in the machine config before the worker is added. Per-NUMA
huge pages, 2 MiB pages, through `machine.sysfs` (dotted paths under `/sys`;
`node0`, `node1`, ... one entry per NUMA node that will hold an SPDK process):

```yaml
machine:
  sysfs:
    devices.system.node.node0.hugepages.hugepages-2048kB.nr_hugepages: "6943"
  kubelet:
    extraConfig:
      cpuManagerPolicy: static
      topologyManagerPolicy: single-numa-node
      topologyManagerScope: pod
      reservedSystemCPUs: "0,1"
```

The page count is what the node sizes for itself: `huge_page_memory` of each
node config on the worker (`sbctl sn get`), plus 1 GB headroom per node config,
in 2 MiB pages, per NUMA node. The refusal message prints it, so one way to size
a fleet is to add the first worker without the patch, read the numbers off the
failure, and roll them out. A pool set through `machine.sysctls`
`vm.nr_hugepages` is honoured as well, as long as it covers the SPDK pod's
request (`spdk_mem` rounded to 2 MiB plus 512 MiB per pod); do not set both.

The refusal message carries this YAML; nothing writes a file for you (the host
has no shell, and the node agent's filesystem is not yours). A refusal for a
worker whose pool is short looks like this in the node agent's log (or, with
`\n` in place of the line breaks, in the HTTP body the control plane logs):

```
Talos node 'worker-3': host provides 4000 MiB of huge pages but the SPDK pod
requests 6088 MiB (hugepages-2Mi), i.e. at least 3044 2 MiB pages, and the
machine config is pre-provisioned (TALOS_MACHINE_CONFIG=preprovisioned). Save
the YAML below as patch.yaml and apply it to the worker with 'talosctl -n <node
ip> patch mc -p @patch.yaml', then restart its kubelet with 'talosctl -n <node
ip> service kubelet restart' so it advertises the pool, or set
storageNodes.talosMachineConfig: Managed on the StorageCluster to let the node
apply it through the Talos API:
machine:
  sysfs:
    devices.system.node.node0.hugepages.hugepages-2048kB.nr_hugepages: '3423'
```

The file is the lines from `machine:` to the end, nothing else:

```yaml title="patch.yaml"
machine:
  sysfs:
    devices.system.node.node0.hugepages.hugepages-2048kB.nr_hugepages: '3423'
```

Save it where you run `talosctl` and apply it, then restart the kubelet so it
advertises the new pool:

```
talosctl -n <node ip> patch mc --mode=no-reboot -p @patch.yaml
talosctl -n <node ip> service kubelet restart
```

The count is simplyblock's own sizing for the worker and replaces whatever the
key held; a pool kept for another consumer has to be added to it by hand (the
node verifies only that the total covers the SPDK pod). The same patch works
inline, as JSON after `-p`, which saves the file when rolling it out to a fleet
of identical workers:

```
talosctl -n <node ip> patch mc --mode=no-reboot \
  -p '{"machine":{"sysfs":{"devices.system.node.node0.hugepages.hugepages-2048kB.nr_hugepages":"3423"}}}'
```

The kubelet refusal has the same shape, with `machine.kubelet.extraConfig` in
place of `machine.sysfs`; the operator retries the add on its own once the
patch is in.

Both settings take effect without a reboot; Talos restarts the kubelet itself
for the `extraConfig` change. Two things to know:

* **The kubelet reads its huge-page capacity at start.** Talos writes
  `machine.sysfs` after boot, racing the kubelet, so after a reboot the kubelet
  can advertise zero or a partial pool while the host has it all (seen
  2026-10-25). The add then fails with "the kubelet advertises less"; the fix is
  `talosctl -n <node ip> service kubelet restart`. A reservation on the kernel
  command line (`hugepagesz=2M hugepages=<total>` in
  `machine.install.extraKernelArgs`, applied through an upgrade/reboot) exists
  before the kubelet does and has no such race; it is the better choice for a
  production fleet.
* **A kubelet that ran under another CPU-manager policy refuses to start over
  its checkpoint** (`configured policy differs from state checkpoint policy`).
  Put the `extraConfig` block in the machine config at install time, before the
  first boot, or remove `/var/lib/kubelet/cpu_manager_state` on the worker
  first (a privileged pod with that path mounted; the host has no shell).

`reservedSystemCPUs` is the StorageCluster's `reservedSystemCPU` (default
`0,1`), and the node compares the live kubelet configuration against it, so
the two must agree. The kubelet step runs under the same switches as elsewhere
(`enableCpuTopology`; the operator enables the kubelet configuration on Talos).
Without `nodes/proxy get` on the storage-node role the live configuration
cannot be read and the step is skipped with a warning.

### Managed

With `storageNodes.talosMachineConfig: Managed` the node writes the same two
settings itself, as Jobs running `talosctl` against the worker's own `apid`,
and only on a shortfall:

1. If the host's pool is short: patch the per-NUMA counts into `machine.sysfs`
   (applied live, no reboot) and wait until the pool shows in `/proc/meminfo`.
   Talos writes the kernel parameter asynchronously, a few seconds after the
   patch is accepted.
2. If the kubelet's `hugepages-2Mi` capacity is short: restart the kubelet and
   wait until it advertises the pool. The two steps are separate because a
   restart that races the write boots the kubelet seeing zero and the SPDK pod
   never schedules (seen on the first Talos deployment, 2026-10-16).
3. If the kubelet lacks the CPU topology settings: a privileged Job with
   `/var/lib/kubelet` mounted removes the CPU-manager checkpoint (the SPDK pod
   is down while this runs, and the kubelet rebuilds the state from the pods it
   finds), then a `talosctl` Job patches `machine.kubelet.extraConfig`; Talos
   restarts the kubelet and the add waits for the node to be Ready again.

The add fails with the numbers if a wait runs out. What `talosctl` printed is
copied into the node agent's log before a completed Job is deleted; a failed Job
is left in place with its pod log.

Managed mode adds simplyblock's pages on top of the worker's own, as the node
does on every other host. The first time it sees a worker it records the pages
each NUMA node already carries as the user's baseline, and the patch sets each
NUMA node to that baseline plus simplyblock's count; a pool the worker already
had is never taken for the SPDK pod, even one large enough for it, and a manual
increase made later is absorbed into the baseline. The pool is never shrunk.
The record (`hugepages_baseline_node<N>` and `hugepages_sb_node<N>`) lives in
`/var/simplyblock` on the worker, which Talos keeps across reboots -- it has to,
because the machine config re-applies the whole pool on boot and a record lost
with it would count simplyblock's pages as the user's on the next add. A worker
whose pool was put in place by hand and that is then switched to Managed should
have that pool set to `"0"` in its machine config first (or the record written
as `0`), or Managed sizes a second allocation on top of the first. Pre-provisioned
mode keeps no record: the pool is yours, and the node only verifies that it
covers the SPDK pod.

Managed mode does not remove the cluster-admin step: allowing the Talos API to
be reached from the simplyblock namespace is a control-plane machine-config
change only the cluster's operator can make. It saves the per-worker sizing and
patching.

#### Prerequisite for Managed: Talos API access from Kubernetes

The Jobs authenticate to `apid` with credentials Talos issues to a
`talos.dev/v1alpha1 ServiceAccount` in our namespace
([Talos API access from Kubernetes](https://docs.siderolabs.com/kubernetes-guides/advanced-guides/talos-api-access-from-k8s)).
Patching the machine config needs the `os:admin` role. Two steps, both on the
cluster side:

1. Enable the feature on the **control plane** machines, allowing `os:admin` for
   the simplyblock namespace. Talos 1.14 and later:
   ```yaml
   apiVersion: v1alpha1
   kind: KubeTalosAPIAccessConfig
   allowedRoles:
     - os:admin
   allowedKubernetesNamespaces:
     - simplyblock
   ```
   Earlier Talos:
   ```yaml
   machine:
     features:
       kubernetesTalosAPIAccess:
         enabled: true
         allowedRoles:
           - os:admin
         allowedKubernetesNamespaces:
           - simplyblock
   ```
   Apply with `talosctl -n <control plane ip> patch mc -p @patch.yaml`.

2. Create the Talos ServiceAccount in the namespace. Talos then issues a Secret
   of the same name holding the client config the Job mounts:
   ```yaml
   apiVersion: talos.dev/v1alpha1
   kind: ServiceAccount
   metadata:
     name: talos-api-access-talos-secrets
     namespace: simplyblock
   spec:
     roles:
       - os:admin
   ```

`os:admin` lets anything that can mount that Secret reconfigure the node. Keep
the namespace to simplyblock's own workloads. The node checks for the Secret
before creating a Job that mounts it (a missing Secret would otherwise be a Job
pod stuck in ContainerCreating for the whole Job timeout) and fails the add
with these two steps when it is absent.

### Which CPUs are reserved

`reservedSystemCPUs` is the StorageCluster's `reservedSystemCPU` (default `0,1`),
passed to the node as `RESERVED_SYSTEM_CPUS`. It is not derived from the SPDK
core layout. The static CPU manager picks the SPDK pod's exclusive cores from
the non-reserved CPUs, any of them, and the restart path follows that pick; the
only way to force the pick would be to reserve everything but the SPDK cores,
and the kubelet also sizes the node's allocatable CPU as capacity minus the
reserved list, so the SPDK pod and every other pod with a CPU request would
then have to fit into the SPDK core count (worker-4 went from 22 to 4
allocatable and the pod sat Pending on "Insufficient cpu", 2026-10-21). Size
the reserved set with that in mind: every reserved CPU is one less schedulable
CPU on the node, even though normal pods still run on the reserved CPUs.

The exception to "normal pods still run there" is the `strict-cpu-reservation`
policy option, which keeps every pod off the reserved CPUs; a kubelet that has
it on is refused with a message naming it. The SPDK pod's proxy container
requests a fractional CPU (requests equal to limits, so the pod stays
Guaranteed) and runs in the shared pool; only the SPDK container holds
exclusive cores, so a changed `reservedSystemCPU` or a hyperthreaded host with
`full-pcpus-only` never costs a second core per pod.

Core isolation (`isolcpus`, `nohz_full`, `rcu_nocbs`) is not applied on Talos in
either mode: those are kernel arguments, which on Talos live in
`machine.install.extraKernelArgs` and take effect only through an upgrade and
reboot. Set them in the machine config yourself if you need them.

### Knobs on the node agent

| Env | Default | Meaning |
|-----|---------|---------|
| `TALOS_MACHINE_CONFIG` | `preprovisioned` | `preprovisioned`: verify only, fail the add with the patch to apply. `managed`: apply through the Talos API. Set by the operator from `storageNodes.talosMachineConfig`. |
| `TALOS_CLUSTER` | unset (detect) | Override the Talos detection: `true` forces the Talos path, `false` the host-shell one. |
| `TALOSCTL_IMAGE` | `ghcr.io/siderolabs/talosctl:v1.14.3` | Image the managed-mode Jobs run; pin to the cluster's Talos minor, mirror it for air-gapped sites. |
| `TALOS_API_SECRET` | `talos-api-access-talos-secrets` | Name of the ServiceAccount / Secret above (managed mode). |
| `CPU_TOPOLOGY_ENABLED` | `true` on Talos | Whether the kubelet's CPU topology is part of what the node verifies (pre-provisioned) or applies (managed). Set by the operator from `storageNodes.enableCpuTopology`. |
| `SKIP_KUBELET_CONFIGURATION` | `false` on Talos | `true` keeps the node away from the kubelet even with the CPU topology on: the huge pages are still verified or applied, the kubelet is left as it is. Set by the operator from `storageNodes.enableKubeletConfiguration` (inverted). |

Wipe Disks:
* `talosctl wipe disk --nodes $NODEID nvme1n1`
