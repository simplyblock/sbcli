# SimplyBlock Management Node on Kubernetes

This guide explains how to deploy the SimplyBlock Management Node on a Kubernetes cluster using the provided bootstrap scripts.

---

## Prerequisites

- A Linux host or VM with access to the required bootstrap scripts:
  - `./bootstrap-k3s.sh`
  - `./bootstrap-cluster.sh`
- Access to the kubernetes Cluster

---

## Step-by-Step Guide

### 1. Bootstrap the Kubernetes Cluster with Worker Node Support

Run the following command to deploy a K3s-based Kubernetes cluster with support for storage worker nodes:

```bash
./bootstrap-k3s.sh --k8s-snode
```

### 2. Prepare an Administrative Host

Once the cluster is bootstrapped, copy the generated kubeconfig file (~/.kube/config or the one output by K3s) to a Linux host where you will perform SimplyBlock cluster administrative tasks. Also, update the IP in the kubeconfig file from 127.0.0.1 to ``<mgmt-worker-node-ip>``:

```bash
scp /etc/rancher/k3s/k3s.yaml <admin-host>:/home/<user>/.kube/config
```

> **Important:** The administrative host must be a **Linux machine** that:
>
> - Has access to the Kubernetes `kubeconfig` file  
> - Can reach the Kubernetes **worker nodes over the network**, including the following ports:
>   - `6443` for Kubernetes API server  
>   - `4500` for FoundationDB  
>   - `80` and `443` for HTTP and HTTPS access (if applicable)  
> - Is used for managing and operating the SimplyBlock cluster (not necessarily running the Management Node itself)

Install SimplyBlock CLI and FoundationDB Client
On the administrative host, install the following tools:

SimplyBlock CLI (sbctl):

```bash
pip install sbctl
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

List the Bootstrapped cluster.

```bash
sbctl cluster list
```
