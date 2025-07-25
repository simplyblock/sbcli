# SimplyBlock Management Node on Kubernetes

This guide explains how to deploy the SimplyBlock Management Node on a Kubernetes cluster using the provided bootstrap scripts.

---

## Prerequisites

- A Linux host or VM with access to the required bootstrap scripts:
  - `./bootstrap-k3s.sh`
  - `./bootstrap-cluster.sh`
- At least one Kubernetes master node (can also act as a worker node) and optionally one or more additional worker nodes for HA management.
- SSH access to all nodes.

---

## Step-by-Step Guide

### 1. Bootstrap the Kubernetes Cluster with Worker Node Support

Run the following command to deploy a K3s-based Kubernetes cluster with support for storage worker nodes:

```bash
./bootstrap-k3s.sh --k8s-snode
```

### 2. Copy Kubeconfig to Management Worker Nodes

Once the cluster is bootstrapped, copy the generated kubeconfig file ``(~/.kube/config or the one output by K3s)`` to the worker nodes that will run the Management Node and update the ip form 127.0.0.1 to ``<mgmt-worker-node-ip>``:

```bash
scp /etc/rancher/k3s/k3s.yaml <mgmt-worker-node>:/home/<user>/.kube/config
```

### 3. Deploy the Cluster in Kubernetes Mode
Now run the bootstrap cluster script
```bash
./bootstrap-cluster.sh --mode kubernetes
```

### 4. Verification
You can verify that the Management Node is running by checking the pods in the namespace (e.g., simplyblock):

```bash
kubectl get pods -n simplyblock
```
