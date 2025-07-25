#!/usr/bin/env bash

set -ex -o pipefail

is_ip_address() {
    local ip="$1"

    if echo "$ip" | grep -E -q '^([0-9]{1,3}\.){3}[0-9]{1,3}$'; then
        IFS='.' read -r -a parts <<< "$ip"
        for part in "${parts[@]}"; do
            if [ "$part" -lt 0 ] || [ "$part" -gt 255 ]; then
                echo "false"
                return
            fi
        done
        echo "true"
        return
    fi

    if echo "$ip" | grep -E -q '^([0-9a-fA-F]{0,4}:){1,7}[0-9a-fA-F]{0,4}$'; then
        echo "true"
        return
    fi

    echo "false"
}

export CLI_SSH_PASS=$1
export CLUSTER_IP=$2
export SIMPLYBLOCK_DOCKER_IMAGE=$3

export SIMPLYBLOCK_REPOSITORY="${SIMPLYBLOCK_DOCKER_IMAGE%%:*}"
export SIMPLYBLOCK_TAG="${SIMPLYBLOCK_DOCKER_IMAGE##*:}"

export CONTROLCENTER_REPOSITORY="https://hub.docker.com/r/simplyblock/controlcenter"
export CONTROLCENTER_TAG="latest"

export GRAYLOG_ROOT_PASSWORD_SHA2=$4
export GRAYLOG_PASSWORD_SECRET="is6SP2EdWg0NdmVGv6CEp5hRHNL7BKVMFem4t9pouMqDQnHwXMSomas1qcbKSt5yISr8eBHv4Y7Dbswhyz84Ut0TW6kqsiPs"

export CLUSTER_SECRET=$5
export CLUSTER_ID=$6
export LOG_DELETION_INTERVAL=$7
export RETENTION_PERIOD=$8
export LOG_LEVEL=$9
export GRAFANA_ENDPOINT=${10}
export CONTACT_POINT=${11}
export DB_CONNECTION=${12}
export K8S_NAMESPACE=${13}
export DISABLE_MONITORING=${14}
export CONTROLCENTER_REFRESH_TOKEN_SECRET=${15}
export DIR="$(dirname "$(realpath "$0")")"
export FDB_CLUSTER_FILE_CONTENTS=${DB_CONNECTION}


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

if is_ip_address "$CLUSTER_IP"; then
  export USE_HOST=true
  export SERVICE_TYPE="ClusterIP"
else
  export USE_HOST=false
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

/usr/local/bin/helm dependency build "$DIR"/charts/

/usr/local/bin/helm upgrade --install sbcli "$DIR"/charts/ \
  --namespace $K8S_NAMESPACE \
  --create-namespace

/usr/local/bin/kubectl wait --for=condition=Ready pod --all --namespace $K8S_NAMESPACE --timeout=300s

