#!/bin/bash

spdk_deploy_dir="simplyBlockDeploy"

if [ -d "$spdk_deploy_dir" ]; then
    rm -rf "$spdk_deploy_dir"
fi

mkdir "$spdk_deploy_dir"

git clone https://github.com/simplyblock-io/simplyBlockDeploy.git "$spdk_deploy_dir"

bootstrap_path="$spdk_deploy_path/bootstrap-cluster.sh"
chmod +x "$bootstrap_path"

"$bootstrap_path"

date=$(date +"%Y-%m-%d_%H:%M:%S")
new_filename="pytest_output_${date}.log"
directory_name="pytest_${date}_output"
mkdir $directory_name

python -m pip install -r requirements.txt

python -m pytest -s --directory "$directory_name" | tee $directory_name/$new_filename

# python send_email.py --sender "raunak@simplyblock.io" --receiver "ronakjalan98@gmail.com" --subject "Test Subject" --body "Test Body" --attachment_dir "$directory_name" --parse-logs

### destroy the cluster after the tests are done
# terraform destroy --auto-approve
