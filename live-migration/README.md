### Installation

make sure that nested virtualzation is enabled. 

### packages
```
yum install -y tar wget git

```
#### Install the operator
```
export VERSION=$(curl -s https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)
echo $VERSION
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-operator.yaml"
```

CRDs

```
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-cr.yaml"
```

### install Krew

```
(
  set -x; cd "$(mktemp -d)" &&
  OS="$(uname | tr '[:upper:]' '[:lower:]')" &&
  ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')" &&
  KREW="krew-${OS}_${ARCH}" &&
  curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz" &&
  tar zxvf "${KREW}.tar.gz" &&
  ./"${KREW}" install krew
)
```

### Install VirtCTL 

```
VERSION=$(kubectl get kubevirt.kubevirt.io/kubevirt -n kubevirt -o=jsonpath="{.status.observedKubeVirtVersion}")
ARCH=$(uname -s | tr A-Z a-z)-$(uname -m | sed 's/x86_64/amd64/') || windows-amd64.exe
echo ${ARCH}
curl -L -o virtctl https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/virtctl-${VERSION}-${ARCH}
chmod +x virtctl
sudo install virtctl /usr/local/bin
```

install krew 
```
kubectl krew install virt
```

verify if all the components are installed

```
kubectl get kubevirt.kubevirt.io/kubevirt -n kubevirt -o=jsonpath="{.status.phase}"
```

### Enable Live migration

Check if live migration feature is enabled
```
kubectl get kubevirt kubevirt -n kubevirt -o jsonpath='{.spec.configuration.developerConfiguration.featureGates}'
```

enable live migration
```
kubectl patch kubevirt kubevirt -n kubevirt --type=merge -p '{"spec":{"configuration":{"developerConfiguration":{"featureGates":["LiveMigration"]}}}}'
```

Wait for the KubeVirt components to restart and become ready after applying the patch.
```
kubectl rollout status -n kubevirt deployment/virt-controller
kubectl rollout status -n kubevirt daemonset/virt-handler
```

For Live migration to work, Ensure your StorageClass supports ReadWriteMany (RWX). In case of Simplyblock, ReadWriteMany is supported only on `Block` volumeMode.

### Other Debug tips

On which Node is my VM running on
```
kubectl get vmis postgres-vm -o wide
```

### How to use virtctl cli? 

virtctl binary utility is used for:
* Serial and graphical console access. 
* Starting and stopping VirtualMachineInstances
* Live migrating VirtualMachineInstances and canceling live migrations
* Uploading virtual machine disk images
