#!/usr/bin/env python
# encoding: utf-8
import logging
import boto3
import time
from botocore.exceptions import ClientError


from flask import Blueprint
from flask import request

from simplyblock_web import utils

from simplyblock_core import kv_store
from simplyblock_core.models.deployer import Deployer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("deployer", __name__)
db_controller = kv_store.DBController()


## Terraform variables
document_name = 'AWS-RunShellScript'
output_key_prefix = 'ssm-output'

# TODO: take from os env vars
bucket_name = 'simplyblock-tfengine-logs-2c874a4e4e'

# intialise clients
ssm = boto3.client('ssm', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')


def get_instance_tf_engine_instance_id():
    tag_value = 'tfengine'
    tag_key = 'Name'

    ec2 = boto3.client('ec2', region_name='us-east-1')
    response = ec2.describe_instances(
        Filters=[
            {
                'Name': f'tag:{tag_key}',
                'Values': [tag_value]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['running']
            }
        ]
    )

    # Extract instance IDs
    instance_ids = [
        instance['InstanceId']
        for reservation in response['Reservations']
        for instance in reservation['Instances']
    ]
    return instance_ids

def check_command_status(ssm_client, command_id, instance_id):
    # time.sleep(1) # wait for a second to avoid: An error occurred (InvocationDoesNotExist)
    try:
        response = ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id
        )
        return response['Status']
    except Exception as e:
        print(f"Exeception {e}")
        return "Exeception"


def wait_for_s3_object(s3_client, bucket, key, timeout=300, interval=5):
    elapsed_time = 0

    while elapsed_time < timeout:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                time.sleep(interval)
                elapsed_time += interval
            else:
                raise e
    return False

def display_logs(command_id, instance_id, status):
    output_s3_key = f'{output_key_prefix}/{command_id}/{instance_id}/awsrunShellScript/0.awsrunShellScript/stdout'
    error_s3_key = f'{output_key_prefix}/{command_id}/{instance_id}/awsrunShellScript/0.awsrunShellScript/stderr'

    # if success get STDOUT. else get STDERR
    if status == 'Success':
        wait_for_s3_object(s3, bucket_name, output_s3_key)
        try:
            stdout_object = s3.get_object(Bucket=bucket_name, Key=output_s3_key)
            stdout_content = stdout_object['Body'].read().decode('utf-8')
            print('Output:')
            print(stdout_content)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                print('No object found - returning empty')
    else:
        wait_for_s3_object(s3, bucket_name, error_s3_key)
        try:
            stderr_object = s3.get_object(Bucket=bucket_name, Key=error_s3_key)
            stderr_content = stderr_object['Body'].read().decode('utf-8')
            print('Error:')
            print(stderr_content)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                print('No object found - returning empty')

def wait_for_status(command_id, instance_id):
    status = 'Pending'
    while status not in ['Success', 'Failed', 'Cancelled', 'TimedOut']:
        status = check_command_status(ssm, command_id, instance_id)
        print(f'Current status: {status}')
        if status in ['Success', 'Failed', 'Cancelled', 'TimedOut']:
            break
        time.sleep(5)  # Wait for 5 seconds before checking the status again

    return status

def update_cluster(TFSTATE_BUCKET, TFSTATE_KEY, TFSTATE_REGION, TF_WORKSPACE, storage_nodes, mgmt_nodes, availability_zone, aws_region):
    instance_ids = get_instance_tf_engine_instance_id()
    if len(instance_ids) == 0:
        # wait for a min and try again before returning error on the API
        print('no instance IDs')
        return

    commands = [
        f"""
        ECR_REGION="us-east-1"
        ECR_ACCOUNT_ID="565979732541"
        ECR_REPOSITORY_NAME="simplyblockdeploy"
        ECR_IMAGE_TAG="latest"

        docker pull $ECR_ACCOUNT_ID.dkr.ecr.$ECR_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:$ECR_IMAGE_TAG
        docker volume create terraform
        docker run --rm -v terraform:/app -w /app $ECR_ACCOUNT_ID.dkr.ecr.$ECR_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:$ECR_IMAGE_TAG \
            init -reconfigure -input=false \
                    -backend-config='bucket={TFSTATE_BUCKET}' \
                    -backend-config='key={TFSTATE_KEY}' \
                    -backend-config='region={TFSTATE_REGION}'

        docker run --rm -v terraform:/app -e TF_LOG=DEBUG -w /app $ECR_ACCOUNT_ID.dkr.ecr.$ECR_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:$ECR_IMAGE_TAG \
            workspace select {TF_WORKSPACE}

        docker run --rm -v terraform:/app -w /app $ECR_ACCOUNT_ID.dkr.ecr.$ECR_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:$ECR_IMAGE_TAG \
            plan -var mgmt_nodes={mgmt_nodes} -var storage_nodes={storage_nodes} -var az={availability_zone} -var region={aws_region}

        docker run --rm -v terraform:/app -w /app $ECR_ACCOUNT_ID.dkr.ecr.$ECR_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:$ECR_IMAGE_TAG \
         apply -var mgmt_nodes={mgmt_nodes} -var storage_nodes={storage_nodes} -var az={availability_zone} -var region={aws_region} --auto-approve
        """
    ]

    # Send command with S3 output parameters
    response = ssm.send_command(
        InstanceIds=instance_ids,
        DocumentName=document_name,
        Parameters={
            'commands': commands
        },
        OutputS3BucketName=bucket_name,
        OutputS3KeyPrefix=output_key_prefix
    )

    command_id = response['Command']['CommandId']
    print(f'Command ID: {command_id}')
    status = wait_for_status(command_id, instance_ids[0])

    # Get Logs
    display_logs(command_id, instance_ids[0], status)


@bp.route('/deployer', methods=['GET'], defaults={'uuid': None})
@bp.route('/deployer/<string:uuid>', methods=['GET'])
def list_deployer(uuid):
    deployers_list = []
    if uuid:
        dpl = db_controller.get_deployer_by_id(uuid)
        if dpl:
            deployers_list.append(dpl)
        else:
            return utils.get_response_error(f"Deployer not found: {uuid}", 404)
    else:
        dpls = db_controller.get_deployers()
        if dpls:
            deployers_list.extend(dpls)

    data = []
    for deployer in deployers_list:
        d = deployer.get_clean_dict()
        d['status_code'] = deployer.get_status_code()
        data.append(d)
    return utils.get_response(data)


def validate_tf_vars(dpl_data):
    if 'cluster_id' not in dpl_data:
        return "missing required param: cluster_id"
    if 'region' not in dpl_data:
        return "missing required param: region"
    if 'az' not in dpl_data:
        return "missing required param: az"
    if 'sbcli_cmd' not in dpl_data:
        return "missing required param: sbcli_cmd"
    if 'sbcli_pkg_version' not in dpl_data:
        return "missing required param: sbcli_pkg_version"
    if 'mgmt_nodes' not in dpl_data:
        return "missing required param: mgmt_nodes"
    if 'storage_nodes' not in dpl_data:
        return "missing required param: storage_nodes"
    if 'extra_nodes' not in dpl_data:
        return "missing required param: extra_nodes"
    if 'mgmt_nodes_instance_type' not in dpl_data:
        return "missing required param: mgmt_nodes_instance_type"
    if 'storage_nodes_instance_type' not in dpl_data:
        return "missing required param: storage_nodes_instance_type"
    if 'extra_nodes_instance_type' not in dpl_data:
        return "missing required param: extra_nodes_instance_type"
    if 'storage_nodes_ebs_size1' not in dpl_data:
        return "missing required param: storage_nodes_ebs_size1"
    if 'storage_nodes_ebs_size2' not in dpl_data:
        return "missing required param: storage_nodes_ebs_size2"
    if 'volumes_per_storage_nodes' not in dpl_data:
        return "missing required param: volumes_per_storage_nodes"
    if 'nr_hugepages' not in dpl_data:
        return "missing required param: nr_hugepages"
    if 'tf_state_bucket_name' not in dpl_data:
        return "missing required param: tf_state_bucket_name"
    if 'tf_workspace' not in dpl_data:
        return "missing required param: tf_workspace"

    return ""

@bp.route('/deployer/tfvars', methods=['POST'])
def set_tf_vars():
    """
    Take the terraform variables and replace the existing values
    Used to set the TF vars that are used currently
    Ideally this should be called immediately the first cluster is created. So that for all the subsequent
    storage node add in a different availability zone can be done using the /deployer API
    """
    dpl_data = request.get_json()
    validation_err = validate_tf_vars(dpl_data)
    if validation_err != "":
        return utils.get_response_error(validation_err, 400)

    d = Deployer()
    d.uuid = dpl_data['cluster_id']
    d.region = dpl_data['region']
    d.az = dpl_data['az']
    d.sbcli_cmd = dpl_data['sbcli_cmd']
    d.sbcli_pkg_version = dpl_data['sbcli_pkg_version']
    d.mgmt_nodes = dpl_data['mgmt_nodes']
    d.storage_nodes = dpl_data['storage_nodes']
    d.extra_nodes = dpl_data['extra_nodes']
    d.mgmt_nodes_instance_type = dpl_data['mgmt_nodes_instance_type']
    d.storage_nodes_instance_type = dpl_data['storage_nodes_instance_type']
    d.extra_nodes_instance_type = dpl_data['extra_nodes_instance_type']
    d.storage_nodes_ebs_size1 = dpl_data['storage_nodes_ebs_size1']
    d.storage_nodes_ebs_size2 = dpl_data['storage_nodes_ebs_size2']
    d.volumes_per_storage_nodes = dpl_data['volumes_per_storage_nodes']
    d.nr_hugepages = dpl_data['nr_hugepages']
    d.tf_state_bucket_name = dpl_data['tf_state_bucket_name']
    d.tf_workspace = dpl_data['tf_workspace']

    d.write_to_db(db_controller.kv_store)
    return utils.get_response(d.to_dict()), 201

@bp.route('/deployer', methods=['POST'])
def add_deployer():
    dpl_data = request.get_json()
    validation_err = validate_tf_vars(dpl_data)
    if validation_err != "":
        return utils.get_response_error(validation_err, 400)

    uuid = dpl_data['cluster_id']
    d = db_controller.get_deployer_by_id(uuid)

    TFSTATE_BUCKET=d.tf_state_bucket_name
    TFSTATE_KEY='csi'
    TFSTATE_REGION='us-east-2'
    TF_WORKSPACE=d.tf_workspace

    # tf mgmt node parameters
    storage_nodes = d.storage_nodes
    mgmt_nodes = d.mgmt_nodes
    availability_zone = d.az
    aws_region = d.region
    status, stdout, stderr = update_cluster(
        TFSTATE_BUCKET,
        TFSTATE_KEY,
        TFSTATE_REGION,
        TF_WORKSPACE,
        storage_nodes,
        mgmt_nodes,
        availability_zone,
        aws_region
    )

    output = {
        "status": status,
        "stdout": stdout,
        "stderr": stderr
    }

    return utils.get_response(output, 201)
