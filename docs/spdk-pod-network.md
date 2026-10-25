# SPDK pod on the pod network (Multus attachment)

By default the SPDK pod of a storage node runs with `hostNetwork: true`: NVMe-oF
listens on a host NIC (`--data-nics`) and the RPC proxy answers on the node IP.
Set a Multus `NetworkAttachmentDefinition` and the pod runs on the pod network
instead, with the attachment as its data interface (`net1`).

```
+---------------------- worker -----------------------+
| host kernel: enp1s0 10.10.10.145  <- CSI nvme connect|
| SPDK pod (no hostNetwork)                            |
|   eth0  10.244.x.y   primary CNI, RPC proxy          |
|   net1  10.10.10.220 Multus attachment, NVMe-oF      |
+------------------------------------------------------+
```

## Enabling it

Prerequisites on the cluster: Multus, an IPAM (whereabouts or static), the
reference CNI plugin the attachment uses (`host-device`, `sriov`, `macvlan`,
`bridge`), and an attachment in the storage namespace, for example

```yaml
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: storage-vf, namespace: simplyblock}
spec:
  config: |
    {"cniVersion": "0.3.1", "name": "storage-vf", "type": "host-device",
     "device": "enp2s0",
     "ipam": {"type": "whereabouts", "range": "10.10.10.0/24",
              "range_start": "10.10.10.220", "range_end": "10.10.10.239"}}
```

Then point the storage-node agent at it: env `SPDK_NETWORK_ATTACHMENT=storage-vf`
on the agent DaemonSet (the operator's knob), or `network_attachment` in the
`spdk_process_start` request. The value is recorded on the node, so a node added
on the pod network restarts on it whatever the agent env says later.

## What changes

| | hostNetwork (default) | pod network |
|---|---|---|
| NVMe-oF listeners | every `--data-nics` host NIC | the attachment's address (`data_nics` = `net1`) |
| RPC proxy | node IP (`mgmt_ip`) or the per-node proxy DNS name under TLS | pod IP (`pod_ip`) or the same DNS name, which the operator resolves to the pod IP |
| Proxy bind address | node IP | `0.0.0.0` inside the pod |
| Agent API, init Job | unchanged: the agent stays hostNetwork | unchanged |
| Clients | host-kernel initiator on the host's own storage NIC | unchanged: the host keeps its NIC |

The agent waits for Multus to wire the attachment (up to 3 minutes), then reports
`pod_ip`, `data_ip` and `data_ifname` from the pod's
`k8s.v1.cni.cncf.io/network-status` annotation; the control plane stores them on
the node. A restart re-reads them, since the IPAM may hand the new pod another
address.

## Limits

- TCP only. RDMA over a Multus interface is not supported; a cluster whose fabric
  disables TCP refuses the node.
- The attachment's addresses must be reachable from every worker's host kernel,
  including the node that runs the pod (macvlan and ipvlan isolate the parent;
  use `host-device` on a VF, `sriov`, or `bridge`).
- Host-level port fencing (`tasks_runner_port_allow`) acts on the host network
  namespace and does not reach a pod-networked SPDK.
