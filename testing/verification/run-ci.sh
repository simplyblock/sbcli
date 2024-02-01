#!/bin/bash

script_dir=$(cd "$(dirname "$0")" && pwd)

# Function to display script usage
usage() {
    echo "Usage: $0"
    echo "          [--aws-access-key <aws_access_key>]"
    echo "          [--aws-secret-key <aws_secret_key>]"
    echo "          [--aws-region <aws_region>]"
    echo "          [--test <test_scenario>]"
    echo "          <test_scenario> test scenario to run on of"
    echo "          - empty"
    exit 1
}

parse_args(){
    # Parse command-line arguments
    while [[ $# -gt 0 ]]; do
        key="$1"

        case $key in
            --test)
                test_scenario="$2"
                shift
                shift
                ;;
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
            --ha-type)
                ha_type="$2"
                shift
                shift
                ;;
            --cluster-type)
                cluster_type="$2"
                shift
                shift
                ;;
            --branch)
                branch="$2"
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
            --storage-instances)
                storage_instances="$2"
                shift
                shift
                ;;
            --management-instances)
                management_instances="$2"
                shift
                shift
                ;;
            --placement-group-strategy)
                placement_group_strategy="$2"
                shift
                shift
                ;;
            --vpc-subnet-id)
                vpc_subnet_id="$2"
                shift
                shift
                ;;
            --image-ami)
                image_ami="$2"
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
    cluster_type=${cluster_type:-"none"}
    test_scenario=${test_scenario:-"empty"}
    management_instances=${management_instances:-1}
    storage_instances=${storage_instances:-1}
    storage_instance_type=${storage_instance_type:-'i3en.xlarge'}
    management_instance_type=${management_instance_type:-'m6i.xlarge'}
    client_instances=1


    case ${cluster_type} in
      "single")
        storage_instances=1
        ;;
      "multi")
        storage_instances=4
        ;;
      "HA")
        management_instances=3
        storage_instances=5
        ;;
      *)
        echo ""
        ;;
    esac
}

run_empty_scenario(){
    rc=0

    export ANSIBLE_COLLECTIONS_PATHS=${ANSIBLE_COLLECTIONS_PATHS}:/root/.ansible/collections
    echo "ANSIBLE_COLLECTIONS_PATHS: ${ANSIBLE_COLLECTIONS_PATHS}"

    run_id=${RANDOM}
    env_name="ci-${run_id}"
    key_name="${env_name}-key"
    key_name_path="${script_dir}/${env_name}.pem"

    bash ${script_dir}/init.sh \
      --aws-access-key "${aws_access_key}" \
      --aws-secret-key "${aws_secret_key}" \
      --aws-region "${aws_region}" \
      --management-instances ${management_instances} \
      --storage-instances ${storage_instances} \
      --client-instances ${client_instances} \
      --private-key-name "${key_name}" \
      --private-key-path "${key_name_path}" \
      --env-name "${env_name}"

    ((rc=rc + $?))

    bash ${script_dir}/terminate.sh \
      --aws-access-key "${aws_access_key}" \
      --aws-secret-key "${aws_secret_key}" \
      --aws-region "${aws_region}" \
      --private-key-name "${key_name}" \
      --env-name "${env_name}"

    ((rc=rc + $?))

    return ${rc}
}

run_prepare_distrib_cluster_scenario(){
    rc=0

    export ANSIBLE_COLLECTIONS_PATHS=${ANSIBLE_COLLECTIONS_PATHS}:/root/.ansible/collections
    echo "ANSIBLE_COLLECTIONS_PATHS: ${ANSIBLE_COLLECTIONS_PATHS}"
    export AWS_ACCESS_KEY_ID="${aws_access_key}"
    export AWS_SECRET_ACCESS_KEY="${aws_secret_key}"
    export AWS_REGION="${aws_region}"

    run_id=${RANDOM}
    env_name="ci-${run_id}"
    key_name="${env_name}-key"
    key_name_path="${script_dir}/${env_name}.pem"

    bash ${script_dir}/init.sh \
      --aws-access-key "${aws_access_key}" \
      --aws-secret-key "${aws_secret_key}" \
      --aws-region "${aws_region}" \
      --management-instances ${management_instances} \
      --storage-instances ${storage_instances} \
      --client-instances ${client_instances} \
      --private-key-name "${key_name}" \
      --private-key-path "${key_name_path}" \
      --env-name "${env_name}"

    ((rc=rc + $?))
    automation_tag=${automation_tag:-"automation_spawn"}
    ansible-playbook "${script_dir}/playbooks/unit_tests/prepare_distrib.yaml" -vv \
    -e "branch=${branch}" \
    -e "env_name=${env_name}" \
    -e "automation_tag=${automation_tag}"\
    -e "default_key_pair_name=${key_name}" \
    -e "default_key_pair_path=${key_name_path}"


    return ${rc}
}


run_prepare_sbcli_cluster_scenario(){
    rc=0

    export ANSIBLE_COLLECTIONS_PATHS=${ANSIBLE_COLLECTIONS_PATHS}:/root/.ansible/collections
    echo "ANSIBLE_COLLECTIONS_PATHS: ${ANSIBLE_COLLECTIONS_PATHS}"
    export AWS_ACCESS_KEY_ID="${aws_access_key}"
    export AWS_SECRET_ACCESS_KEY="${aws_secret_key}"
    export AWS_REGION="${aws_region}"

    run_id=${RANDOM}
    env_name="ci-${run_id}"
    key_name="${env_name}-key"
    key_name_path="${script_dir}/${env_name}.pem"
    placement_group_name="placement_group-${run_id}"
    bash ${script_dir}/init.sh \
      --aws-access-key "${aws_access_key}" \
      --aws-secret-key "${aws_secret_key}" \
      --aws-region "${aws_region}" \
      --vpc-subnet-id "${vpc_subnet_id}" \
      --placement-group-strategy "${placement_group_strategy}" \
      --placement-group-name "${placement_group_name}" \
      --storage-instance-type ${storage_instance_type} \
      --management-instance-type ${management_instance_type} \
      --management-instances ${management_instances} \
      --storage-instances ${storage_instances} \
      --client-instances 1 \
      --private-key-name "${key_name}" \
      --private-key-path "${key_name_path}" \
      --management-image-id "${image_ami}" \
      --storage-image-id "${image_ami}" \
      --client-image-id "${image_ami}" \
      --env-name "${env_name}"

    ((rc=rc + $?))
    automation_tag=${automation_tag:-"automation_spawn"}
    ansible-playbook "${script_dir}/playbooks/unit_tests/prepare_sbcli.yaml" \
    -e "branch=${branch}" \
    -e "env_name=${env_name}" \
    -e "automation_tag=${automation_tag}"\
    -e "default_key_pair_name=${key_name}" \
    -e "default_key_pair_path=${key_name_path}" \
    -e "ha_type=${ha_type}"

    ((rc=rc + $?))

    #ansible-playbook "${script_dir}/playbooks/unit_tests/init_test.yaml"

    #((rc=rc + $?))
    return ${rc}
}
main(){
    if ! parse_args $@; then
        return 1
    fi

    set_defaults

    if ! run_${test_scenario}_scenario; then
        return 1
    fi
}

main $@
