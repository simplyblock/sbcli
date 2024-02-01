#!/bin/bash

script_dir=$(cd "$(dirname "$0")" && pwd)

# Function to display script usage
usage() {
    echo "Usage: $0"
    echo "          [--aws-access-key <aws_access_key>]"
    echo "          [--aws-secret-key <aws_secret_key>]"
    echo "          [--aws-region <aws_region>]"
    echo "          [--management-image-id <management_image_id>]"
    echo "          [--management-instances <number_of_management_instances>]"
    echo "          [--management-instance-type <management_instance_type>]"
    echo "          [--storage-image-id <storage_image_id>]"
    echo "          [--storage-instances <number_of_storage_instances>]"
    echo "          [--storage-instance-type <storage_instance_type>]"
    echo "          [--storage-instance-nvme-count <storage_instance_nvme_count>]"
    echo "          [--client-image-id <client_image_id>]"
    echo "          [--client-instances <number_of_client_instances>]"
    echo "          [--client-instance-type <client_instance_type>]"
    echo "          [--private-key-name <private_key_name>]"
    echo "          [--private-key-path <private_key_path>]"
    echo "          [--env-name <env_name>]"
    echo "          [--automation-tag <automation_tag>]"
    echo "    <management_image_id> defaults to ami-03cbad7144aeda3eb"
    echo "    <number_of_management_instances> defaults to 1"
    echo "    <management_instance_type> defaults to m6i.xlarge"
    echo "    <storage_image_id> defaults to ami-03cbad7144aeda3eb"
    echo "    <number_of_storage_instances> defaults to 1"
    echo "    <storage_instance_type> defaults to i3en.xlarge"
    echo "    <storage_instance_nvme_count> number of nvmes per storage node, this overrides the storage-instance-type"
    echo "                                  in case both are provided"
    echo "    <client_image_id> defaults to ami-03cbad7144aeda3eb"
    echo "    <number_of_client_instances> defaults to 1"
    echo "    <client_instance_type> defaults to m6i.xlarge"
    echo "    <private_key_name> keyname in aws to use"
    echo "    <private_key_path> local path to the keyname"
    echo "    <env_name> name of the created env, used to tag aws resources created by this session of the automation, defaults to nightly"
    echo "    <automation_tag> used as a general tag to tag all resources ever created by automation, defaults to automation_spawn"
    exit 1
}

parse_args(){
    # Parse command-line arguments
    while [[ $# -gt 0 ]]; do
        key="$1"

        case $key in
            --aws-access-key)
                aws_access_key="$2"
                shift
                shift
                ;;
            --aws-secret-key)
                aws_secret_key="$2"
                shift
                shift
                ;;
            --aws-region)
                aws_region="$2"
                shift
                shift
                ;;
            --management-instances)
                management_instances="$2"
                shift
                shift
                ;;
            --storage-instances)
                storage_instances="$2"
                shift
                shift
                ;;
            --client-instances)
                client_instances="$2"
                shift
                shift
                ;;
            --management-image-id)
                management_image_id="$2"
                shift
                shift
                ;;
            --storage-image-id)
                storage_image_id="$2"
                shift
                shift
                ;;
            --client-image-id)
                client_image_id="$2"
                shift
                shift
                ;;
            --management-instance-type)
                management_instance_type="$2"
                shift
                shift
                ;;
            --storage-instance-type)
                storage_instance_type="$2"
                shift
                shift
                ;;
            --storage-instance-nvme-count)
                storage_instance_nvme_count="$2"
                shift
                shift
                ;;
            --client-instance-type)
                client_instance_type="$2"
                shift
                shift
                ;;
            --private-key-name)
                private_key_name="$2"
                shift
                shift
                ;;
            --private-key-path)
                private_key_path="$2"
                shift
                shift
                ;;
            --env-name)
                env_name="$2"
                shift
                shift
                ;;
            --automation-tag)
                automation_tag="$2"
                shift
                shift
                ;;
            --placement-group-strategy)
                placement_group_strategy="$2"
                shift
                shift
                ;;
            --placement-group-name)
                placement_group_name="$2"
                shift
                shift
                ;;
            --vpc-subnet-id)
                vpc_subnet_id="$2"
                shift
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                echo "Invalid argument: $1"
                usage
                return 1
                ;;
        esac
    done
}

set_defaults(){
    # Set default values for optional arguments
    aws_access_key=${aws_access_key:-''}
    aws_secret_key=${aws_secret_key:-''}
    aws_region=${aws_region:-''}

    management_image_id=${management_image_id:-"ami-03cbad7144aeda3eb"}
    storage_image_id=${storage_image_id:-"ami-03cbad7144aeda3eb"}
    client_image_id=${client_image_id:-"ami-03cbad7144aeda3eb"}

    management_instances=${management_instances:-1}
    storage_instances=${storage_instances:-1}
    client_instances=${client_instances:-1}

    management_instance_type=${management_instance_type:-"m6i.xlarge"}
    storage_instance_type=${storage_instance_type:-"i3en.xlarge"}
    client_instance_type=${client_instance_type:-"m6i.large"}

    storage_instance_nvme_count=${storage_instance_nvme_count:-""}

    private_key_name=${private_key_name:-"my_key_pair"}
    private_key_path=${private_key_path:-"${script_dir}/${private_key_name}.pem"}

    env_name=${env_name:-"nightly"}
    automation_tag=${automation_tag:-"automation_spawn"}

    if [ "${placement_group_strategy}" = "None" ]
    then
      placement_group_name=""
    fi
    if [ "${vpc_subnet_id}" = "None" ]
    then
      vpc_subnet_id=""
    fi

}

check_aws_access(){
    if [[ -z "${aws_access_key}" ]] || [[ -z "${aws_secret_key}" ]] || [[ -z "${aws_region}" ]];then
        not_provided_flags=""
        provided_flags=""

        if [[ -z "${aws_access_key}" ]];then
            not_provided_flags+="--aws-access-key "
        else
            provided_flags+="--aws-access-key "
        fi

        if [[ -z "${aws_secret_key}" ]];then
            not_provided_flags+="--aws-secret-key "
        else
            provided_flags+="--aws-secret-key "
        fi

        if [[ -z "${aws_region}" ]];then
            not_provided_flags+="--aws-region "
        else
            provided_flags+="---aws-region "
        fi

        echo "flags ${provided_flags} were provided without ${not_provided_flags}, all flags must be set together!"
        echo "using system saved credentials instead"
    else
        export AWS_ACCESS_KEY_ID="${aws_access_key}"
        export AWS_SECRET_ACCESS_KEY="${aws_secret_key}"
        export AWS_REGION="${aws_region}"
    fi

    if ! aws sts get-caller-identity; then
        echo 'AWS credentials are not set, Please check credentials configurations!'
        return 1
    fi
}

create_inventory(){
    cat <<EOF > "${script_dir}/inventories/verification.aws_ec2.yaml"
# This file will be overriden by init.sh, only modify if the init is not needed any more
plugin: amazon.aws.aws_ec2

filters:
  tag:env_name: ${env_name}
  tag:automation_tag: ${automation_tag}

groups:
  storage: "'storage' in tags.node_type"
  client: "'client' in tags.node_type"
  management: "'management' in tags.node_type"
  rhel: "'rhel' in tags.os"
  centos: "'centos' in tags.os"
  ubuntu: "'ubuntu' in tags.os"
  debian: "'debian' in tags.os"
  oci: "'oci' in tags.os"

compose:
  ansible_host: public_ip_address
EOF
}

set_passowrd(){
  ansible-playbook "${script_dir}/playbooks/init/set_password.yaml" -vv \
    -e "env_name=${env_name}" \
    -e "automation_tag=${automation_tag}" \
    -e "default_key_pair_name=${private_key_name}" \
    -e "default_key_pair_path=${private_key_path}"

}

run_init_playbook(){
    ansible-playbook "${script_dir}/playbooks/init/init.yaml" -vvv \
    -e "env_name=${env_name}" \
    -e "automation_tag=${automation_tag}" \
    -e "default_security_group_name=MySecurityGroup" \
    -e "default_key_pair_name=${private_key_name}" \
    -e "default_key_pair_path=${private_key_path}" \
    -e "management_image_id=${management_image_id}" \
    -e "storage_image_id=${storage_image_id}" \
    -e "client_image_id=${client_image_id}" \
    -e "management_instances=${management_instances}" \
    -e "storage_instances=${storage_instances}" \
    -e "client_instances=${client_instances}" \
    -e "management_instance_type=${management_instance_type}" \
    -e "storage_instance_type=${storage_instance_type}" \
    -e "client_instance_type=${client_instance_type}" \
    -e "storage_instance_nvme_count=${storage_instance_nvme_count}" \
    -e "placement_group_strategy=${placement_group_strategy}" \
    -e "placement_group_name=${placement_group_name}" \
    -e "vpc_subnet_id=${vpc_subnet_id}"
}

main(){
    if ! parse_args $@; then
        return 1
    fi

    set_defaults

    if ! check_aws_access; then
        return 1
    fi

    if ! run_init_playbook; then
        return 1
    fi

    if ! create_inventory; then
        return 1
    fi
    if ! set_passowrd; then
        return 1
    fi
}

main $@
