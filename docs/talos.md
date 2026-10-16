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
the same through the **Talos API** instead: a Job running `talosctl` against the
node's own `apid` patches the per-NUMA counts into `machine.sysfs` (applied
live, no reboot) and restarts the kubelet so the node's `hugepages-2Mi` capacity
follows. The add waits for both the host and the kubelet to show the pool before
it creates the SPDK pod, and fails with the numbers if they do not. A pool that
already covers the request is left alone, so a plain node restart never restarts
the kubelet.

State Talos in the deployment: `spec.environment: Talos` on the
ClusterDeploymentConfig, which the operator passes to the node agent as
`TALOS_CLUSTER=true` (the way OpenShift is passed as `OPENSHIFT_CLUSTER`). A
stated Talos node also skips the CPU-topology / core-isolation Jobs, which
`nsenter` into the host; kubelet and kernel arguments come from the machine
config on Talos. Without the flag the node still recognises Talos from the
kernel version string inside the init Job and skips the host write, but then
nothing reserves the pages and the pod stays Pending on
`Insufficient hugepages-2Mi`.

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
| `TALOS_CLUSTER` | `false` | The host is Talos: reserve huge pages through the Talos API, skip host-touching Jobs. |
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

