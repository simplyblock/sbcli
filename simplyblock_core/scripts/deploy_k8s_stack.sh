#!/usr/bin/env bash

set -ex -o pipefail

export CLI_SSH_PASS=$1
export CLUSTER_IP=$2
export SIMPLYBLOCK_DOCKER_IMAGE=$3

export SIMPLYBLOCK_REPOSITORY="${SIMPLYBLOCK_DOCKER_IMAGE%%:*}"
export SIMPLYBLOCK_TAG="${SIMPLYBLOCK_DOCKER_IMAGE##*:}"

export GRAYLOG_ROOT_PASSWORD_SHA2=$4
export GRAYLOG_PASSWORD_SECRET="is6SP2EdWg0NdmVGv6CEp5hRHNL7BKVMFem4t9pouMqDQnHwXMSomas1qcbKSt5yISr8eBHv4Y7Dbswhyz84Ut0TW6kqsiPs"

export CLUSTER_SECRET=$5
export CLUSTER_ID=$6
export LOG_DELETION_INTERVAL=$7
export RETENTION_PERIOD=$8
export LOG_LEVEL=$9
export GRAFANA_ENDPOINT=${10}
export CONTACT_POINT=${11}
export K8S_NAMESPACE=${12}
export DISABLE_MONITORING=${13}
export TLS_SECRET=${14}
export INGRESS_HOST_SOURCE=${15}
export DNS_NAME=${16}
export DIR="$(dirname "$(realpath "$0")")"

if [[ "$LOG_DELETION_INTERVAL" == *d ]]; then
   export MAX_NUMBER_OF_INDICES=${LOG_DELETION_INTERVAL%d}
elif [[ "$LOG_DELETION_INTERVAL" == *h || "$LOG_DELETION_INTERVAL" == *m ]]; then
   export MAX_NUMBER_OF_INDICES=1
else
    echo "Invalid LOG_DELETION_INTERVAL format. Please use a value ending in 'd', 'h', or 'm'."
    exit 1
fi

if [[ "${DISABLE_MONITORING,,}" == "false" ]]; then
   export ENABLE_MONITORING=true
else
  export ENABLE_MONITORING=false
fi

if [[ "${INGRESS_HOST_SOURCE}" == "hostip" ]]; then
  export USE_HOST=true
  export USE_DNS=false
  export SERVICE_TYPE="ClusterIP"
elif [[ "${INGRESS_HOST_SOURCE}" == "loadbalancer" ]]; then
  export USE_HOST=false
  export USE_DNS=false
  export SERVICE_TYPE="LoadBalancer"
else
  export USE_HOST=false
  export USE_DNS=true 
  export SERVICE_TYPE="LoadBalancer"
fi


envsubst < "$DIR"/charts/values-template.yaml > "$DIR"/charts/values.yaml

# HELM=$(which helm)
# KUBECTL=$(which kubectl)
# if [[ -z "$HELM" ]]; then
#   echo "helm not found in PATH"
#   exit 1
# fi

# if [[ -z "$KUBECTL" ]]; then
#   echo "kubectl not found in PATH"
#   exit 1
# fi

rm -rf "$DIR"/charts/charts "$DIR"/charts/Chart.lock "$DIR"/charts/requirements.lock

/usr/local/bin/helm dependency build "$DIR"/charts/

/usr/local/bin/helm upgrade --install sbcli "$DIR"/charts/ \
  --namespace $K8S_NAMESPACE \
  --create-namespace

for kind in ds deploy sts; do
  /usr/local/bin/kubectl get $kind -n "$K8S_NAMESPACE" -o name \
  | xargs -r -n1 /usr/local/bin/kubectl rollout status -n "$K8S_NAMESPACE" --timeout=500s
done
