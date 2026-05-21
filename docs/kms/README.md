# Sample values for installing an external KMS
Both openbao and HCP vault are compatible, install either.
They require an existing cert-manager installation, and a mTLS simplyblock deployment.

## Installation
### Openbao
```
helm repo add openbao https://openbao.github.io/openbao-helm
helm install openbao openbao/openbao -n vault --create-namespace -f ./openbao-values.yaml
```

### Hashicorp Vault
For hashicorp vault, the certificates need to be created separately.
The files present in this repository assume the `vault` namespace,
if this changes it needs to be adapted in the `vault-certificates.yaml`
file, both the metadata and the dnsNames.

```
helm repo add hashicorp https://helm.releases.hashicorp.com
kubectl create namespace vault
kubectl apply -f ./vault-certificate.yaml
helm install vault hashicorp/vault -n vault -f ./vault-values.yaml
```

## Initialization
The vault needs to be initialized and unsealed before configuring it.
To interact with this, exec into the pod and set the corresponding environment variables. This may be either:
```
kubectl -n vault exec openbao-0 -- env BAO_ADDR=https://openbao.vault:8200/ bao
# or
kubectl -n vault exec vault-0 -- env VAULT_ADDR=https://vault.vault:8200/ vault
```

Using this prefix initialize the vault, noting the root token and unseal keys:
```
$prefix operator init
$prefix operator unseal $key1
$prefix operator unseal $key2
$prefix operator unseal $key3
```

## Configuration
Configuring the vault requires the token:
```
kubectl -n vault exec -it openbao-0 -- env BAO_ADDR=https://openbao.vault:8200/ BAO_TOKEN=$token CLI=bao sh
# or
kubectl -n vault exec -it vault-0 -- env VAULT_ADDR=https://vault.vault:8200/ VAULT_TOKEN=$token CLI=vault sh
```

Using this shell, configure the vault. Note that the certificate path needs to be adapted depending on Openbao/HCP-vault:
```
# Configure auth
$CLI policy write simplyblock-webappapi-policy - <<EOF
path "simplyblock/transit/keys/*" {
  capabilities = ["create", "update", "read", "delete"]
}

path "simplyblock/transit/datakey/plaintext/*" {
  capabilities = ["create", "update"]
}

path "simplyblock/transit/datakey/wrapped/*" {
  capabilities = ["create", "update"]
}

path "simplyblock/transit/encrypt/*" {
  capabilities = ["create", "update"]
}

path "simplyblock/transit/decrypt/*" {
  capabilities = ["create", "update"]
}

path "simplyblock/kv/*" {
  capabilities = ["create", "read", "update", "delete"]
}
EOF
$CLI auth enable cert
$CLI write auth/cert/certs/simplyblock-webappapi \
    certificate=@/{openbao,vault}/tls/ca.crt \
    allowed_dns_sans="simplyblock-webappapi" \
    token_policies=simplyblock-webappapi-policy \
    token_ttl=10m \
    token_max_ttl=30m

# Enable components
$CLI secrets enable -path=simplyblock/transit transit
$CLI secrets enable -path=simplyblock/kv kv
```
