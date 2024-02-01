#!/bin/bash

# Get the absolute path of the script's directory
script_dir="$(cd "$(dirname "$0")" && pwd)"
root_dir="${script_dir}/.."

# Default values
function set_defaults {
    requirements_file="${root_dir}/requirements.txt"
    verbose=false
}

# Help function
function display_help {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -r, --requirements_file    The python requirements file to install"
    echo "  -v, --verbose              Enable verbose mode"
    echo "  -h, --help                 Display this help message"
}

# Function to parse arguments
function parse_arguments {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -r|--requirements_file)
                requirements_file="$2"
                shift 2
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            -h|--help)
                display_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                display_help
                exit 1
                ;;
        esac
    done
}

function install_python_dependencies {
    if [[ ! -f "$requirements_file" ]]; then
        echo "Error: Requirements file '$requirements_file' not found."
        return 1
    fi

    echo ensuring pip is installed

    wget https://bootstrap.pypa.io/get-pip.py

    python3 get-pip.py

    echo "Installing Python packages from '$requirements_file'..."
    python3 -m pip install --force --no-cache --upgrade -r "$requirements_file"

    if [[ $? -eq 0 ]]; then
        echo "Installation successful."
        return 0
    else
        echo "Installation failed."
        return 1
    fi
}

function install_distro_package {
    echo "Installing distro packages"
    if $(is_ubuntu);then
        apt-get update
        apt-get -y install curl wget unzip python3 less openssh-client jq
    elif $(is_rhel);then
        yum install install curl wget unzip python3 less openssh-clients jq
    fi
}

function is_ubuntu {
    test "$(get_distro_name)" == "ubuntu"
}

function is_rhel {
    test "$(get_distro_name)" == "rhel"
}

function get_distro_name {
    grep -oP '^ID=\K[^"]+' "/etc/os-release"
}

function install_aws {
    echo "Installing awscli"

    if which aws;then
        echo "aws is already installed"
        return 0
    fi

    if ! curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip";then
        return 1
    fi

    if ! unzip /tmp/awscliv2.zip -d /tmp;then
        return 1
    fi

    pushd /tmp

    if ! ./aws/install; then
        return 1
    fi

    popd

    which aws
}

function install_ansible_collections() {
  ansible-galaxy collection install amazon.aws --force
  ansible-galaxy collection install community.docker --force
  ansible-galaxy collection install community.aws --force
}

create_ansile_config(){
    mkdir -p /etc/ansible
    cat <<EOF > /etc/ansible/ansible.cfg
[defaults]
inventory = ${root_dir}/inventories/
roles_path = ${root_dir}/roles/
timeout = 999
[ssh_connection]
timeout = 999
retries = 10
[persistent_connection]
connect_timeout = 900
command_timeout = 900
EOF
}

# Main function
function main {
    rc=0

    set_defaults
    parse_arguments "$@"
    install_distro_package
    ((rc=rc + $?))
    install_python_dependencies
    ((rc=rc + $?))
    install_aws
    ((rc=rc + $?))
    install_ansible_collections
    ((rc=rc + $?))
    create_ansile_config
    ((rc=rc + $?))

    return ${rc}
}

# Call the main function
main "$@"
