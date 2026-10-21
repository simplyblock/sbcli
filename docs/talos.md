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

## Huge pages

On other distributions the storage node reserves its huge pages while it is
added: a privileged Job writes `nr_hugepages` per NUMA node and restarts the
kubelet. The Talos host is immutable and has no shell, so on Talos the node does
the same through the **Talos API** instead, as two Jobs running `talosctl`
against the node's own `apid`:

1. If the host's pool is short: patch the per-NUMA counts into `machine.sysfs`
   (applied live, no reboot) and wait until the pool shows in `/proc/meminfo`.
   Talos writes the kernel parameter asynchronously, a few seconds after the
   patch is accepted.
2. If the kubelet's `hugepages-2Mi` capacity is short: restart the kubelet and
   wait until it advertises the pool. The kubelet reads huge-page capacity at
   start, which is why the two steps are separate: a restart that races the
   write boots the kubelet seeing zero and the SPDK pod never schedules (seen on
   the first Talos deployment, 2026-10-16).

Each step is skipped when its half already holds, so a plain node restart with
the pool in place touches nothing, and a node whose kubelet merely lags the host
gets only the restart. The add fails with the numbers if either wait runs out.
What `talosctl` printed is copied into the node agent's log before a completed
Job is deleted; a failed Job is left in place with its pod log.

The node detects Talos by itself, from what the kubelet reports about its host
in the Node object (`status.nodeInfo.osImage`, e.g. `Talos (v1.12.7)`), so no
flag is needed. `TALOS_CLUSTER=true|false` on the node agent overrides the
detection (a Talos derivative whose osImage says something else, or forcing the
managed path); the operator sets it from `spec.environment: Talos` but it is
not required.

## Kubelet CPU topology

On other distributions the CPU-topology Job edits the kubelet's config file
(`cpuManagerPolicy: static`, `reservedSystemCPUs`, `topologyManagerPolicy:
single-numa-node`, `topologyManagerScope: pod`) and restarts the kubelet. On
Talos the same keys go into `machine.kubelet.extraConfig` through a `talosctl`
Job; Talos applies it without a reboot and restarts the kubelet itself, and the
add waits for the node to be Ready again. Before patching, the node reads the
kubelet's live configuration through the API server's node proxy
(`nodes/<name>/proxy/configz`) and skips the patch when the settings are
already in place, so a plain node restart never restarts the kubelet. That read
needs `nodes/proxy get` on the storage-node role; without it the step is skipped
with a warning rather than restarting the kubelet on every restart. The step
runs under the same switches as elsewhere (`enableCpuTopology`, and not
`skipKubeletConfiguration`).

Right before the patch, a privileged Job with `/var/lib/kubelet` mounted removes
the kubelet's CPU-manager checkpoint (`cpu_manager_state`). The kubelet refuses
to start over a checkpoint written under another policy, or one whose
assignments overlap the new reserved set; the vanilla Job removes the file
before its restart, and Talos has no shell to do that from. The SPDK pod is
down while this runs, and the kubelet rebuilds the state from the pods it finds.

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

Core isolation (`isolcpus`, `nohz_full`, `rcu_nocbs`) is not applied on Talos:
those are kernel arguments, which on Talos live in
`machine.install.extraKernelArgs` and take effect only through an upgrade and
reboot. Set them in the machine config yourself if you need them.

### Prerequisite: Talos API access from Kubernetes

The Job authenticates to `apid` with credentials Talos issues to a
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
the namespace to simplyblock's own workloads.

### Knobs on the node agent

| Env | Default | Meaning |
|-----|---------|---------|
| `TALOS_CLUSTER` | unset (detect) | Override the Talos detection: `true` forces the Talos path, `false` the managed one. |
| `TALOSCTL_IMAGE` | `ghcr.io/siderolabs/talosctl:v1.14.3` | Image the Job runs; pin to the cluster's Talos minor, mirror it for air-gapped sites. |
| `TALOS_API_SECRET` | `talos-api-access-talos-secrets` | Name of the ServiceAccount / Secret above. |

### Sizing, if you want to pre-provision anyway

The node sizes the pool from its own configuration (`huge_page_memory`, see
`sbctl sn get`, plus 1 GB headroom per node config, in 2 MiB pages per NUMA
node) and writes it to
`machine.sysfs` `devices.system.node.node<N>.hugepages.hugepages-2048kB.nr_hugepages`.
A pool you set yourself through `machine.sysctls` `vm.nr_hugepages` is honoured
when it already covers the SPDK pod's request (`spdk_mem` rounded to 2 MiB plus
512 MiB per pod); if it is short the node adds its per-NUMA sysfs entries on top.
Do not set both to different values: the per-NUMA sysfs entries and the global
sysctl are applied independently and the last writer wins.

Create a machine config patch with the contents below and save as patch.yaml.
`1024` pages is 2 GiB and only enough for a small test node.
```
cat > patch.yaml <<'EOF'
machine:
  sysctls:
    vm.nr_hugepages: "1024"
EOF

talosctl -e <endpoint ip/hostname> -n <node ip/hostname> patch mc -p @patch.yaml
```

Wipe Disks:
* `talosctl wipe disk --nodes $NODEID nvme1n1`

