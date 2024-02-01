#!/bin/bash

script_dir=$(cd "$(dirname "$0")" && pwd)

# Function to display script usage
usage() {
    echo "Usage: $0"
    echo "          [--aws-access-key <aws_access_key>]"
    echo "          [--aws-secret-key <aws_secret_key>]"
    echo "          [--aws-region <aws_region>]"
    echo "          [--management-instances <management_instance_names_to_delete>]"
    echo "          [--storage-instances <storage_instance_names_to_delete>]"
    echo "          [--client-instances <client_instance_names_to_delete>]"
    echo "          [--private-key-name <private_key_name>]"
    echo "          [--env-name <env_name>]"
    echo "          [--automation-tag <automation_tag>]"
    echo "    <management_instance_names_to_delete> csv list of instances to delete, defaults to 'all'"
    echo "    <storage_instance_names_to_delete> csv list of instances to delete, defaults to 'all'"
    echo "    <client_instance_names_to_delete> csv list of instances to delete, defaults to 'all'"
    echo "    <private_key_name> keyname to delete, default to none"
    echo "    <env_name> name of the env_name tag to match, this must be present on the resources to delete, defaults to nightly"
    echo "    <automation_tag> name of the automation_tag tag to match, this must be present on the resources to delete, defaults to automation_spawn"
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
            --private-key-name)
                private_key_name="$2"
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

    management_instances=${management_instances:-'all'}
    storage_instances=${storage_instances:-'all'}
    client_instances=${client_instances:-'all'}

    private_key_name=${private_key_name:-""}

    env_name=${env_name:-"nightly"}
    automation_tag=${automation_tag:-"automation_spawn"}
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

run_terminate_playbook(){
    ansible-playbook "${script_dir}/playbooks/terminate/terminate.yaml" -vv \
    -e "env_name=${env_name}" \
    -e "automation_tag=${automation_tag}" \
    -e "key_pair_name_to_delete=${private_key_name}" \
    -e "management_instances_to_delete=${management_instances}" \
    -e "storage_instances_to_delete=${storage_instances}" \
    -e "client_instances_to_delete=${client_instances}"
}

main(){
    if ! parse_args $@; then
        return 1
    fi

    set_defaults

    if ! check_aws_access; then
        return 1
    fi

    if ! run_terminate_playbook; then
        return 1
    fi
}

main $@
