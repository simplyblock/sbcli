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

Huge pages are a prerequisite on Talos. On other distributions the storage node
reserves them itself while it is added (it writes `nr_hugepages` and restarts the
kubelet from a privileged Job); the Talos host is immutable and has no shell, so
the node applies nothing to it and instead verifies the pool the machine config
provides. State Talos in the deployment (`spec.environment: Talos` on the
ClusterDeploymentConfig, which the operator passes to the node agent as
`TALOS_CLUSTER=true`, the same way OpenShift is passed as `OPENSHIFT_CLUSTER`).
A Talos node then:

* skips the `nr_hugepages` write, the kubelet restart and the CPU-topology /
  core-isolation Jobs (kubelet and kernel args come from the machine config);
* fails the add before creating anything when the host's huge-page pool is
  smaller than the SPDK pod will request, naming the `vm.nr_hugepages` value to
  set. Each SPDK pod requests its huge-page memory (the node's `spdk_mem`, see
  `sbctl sn get`) rounded to 2 MiB plus 512 MiB; a host that runs one pod per
  NUMA node needs the sum.

Create a machine config patch with the contents below and save as patch.yaml.
`1024` pages is 2 GiB and only enough for a small test node; size it to the
message above or to `spdk_mem + 512 MiB` per SPDK pod.
```
cat > patch.yaml <<'EOF'
machine:
  sysctls:
    vm.nr_hugepages: "1024"
EOF

talosctl -e <endpoint ip/hostname> -n <node ip/hostname> patch mc -p @patch.yaml
```

Without the flag the node still recognises Talos from the kernel version string
inside the Job, as a fallback; the flag is what makes the behaviour explicit and
adds the pool check.

Wipe Disks:
* `talosctl wipe disk --nodes $NODEID nvme1n1`

