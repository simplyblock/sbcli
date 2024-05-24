#!/bin/bash

spdk_deploy_dir="spdkDeploy"

if [ -d "$spdk_deploy_dir" ]; then
    rm -rf "$spdk_deploy_dir"
fi

mkdir "$spdk_deploy_dir"

git clone https://github.com/simplyblock-io/simplyBlockDeploy.git "$spdk_deploy_dir"

namespace="simplyblock-cluster-e2e"
sbcli_pkg="sbcli"
spdk_image="simplyblock/spdk:faster-bdev-startup-latest"

terraform destroy --auto-approve

terraform init

terraform workspace select -or-create "$namespace"

terraform plan

terraform apply -var mgmt_nodes=0 -var storage_nodes=0 -var extra_nodes=0 --auto-approve

terraform apply -var mgmt_nodes=1 -var storage_nodes=3 -var extra_nodes=1 \
                -var mgmt_nodes_instance_type="m6i.xlarge" -var storage_nodes_instance_type="i3en.2xlarge" \
                -var extra_nodes_instance_type="m6i.large" -var sbcli_pkg="$sbcli_pkg" \
                -var volumes_per_storage_nodes=0 \
                --auto-approve

terraform output -json > outputs.json


bootstrap_path="$spdk_deploy_path/bootstrap-cluster.sh" 
chmod +x "$bootstrap_path"

"$bootstrap_path" --memory 16g  --cpu-mask 0x3 \
                  --sbcli-cmd "$sbcli_pkg" --spdk-image "$spdk_image" \
                  --iobuf_large_pool_count 16384 --iobuf_small_pool_count 131072 \
                  --log-del-interval 300m --metrics-retention-period 2h


date=$(date +"%Y-%m-%d_%H:%M:%S")
new_filename="pytest_output_${date}.log"
directory_name="pytest_${date}_output"
mkdir $directory_name

python -m pip install -r requirements.txt

python -m pytest -s --directory "$directory_name" | tee $directory_name/$new_filename

# python send_email.py --sender "raunak@simplyblock.io" --receiver "ronakjalan98@gmail.com" --subject "Test Subject" --body "Test Body" --attachment_dir "$directory_name" --parse-logs