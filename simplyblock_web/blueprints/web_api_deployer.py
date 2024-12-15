#!/usr/bin/env python
# encoding: utf-8
import logging
import boto3
import time
from botocore.exceptions import ClientError
import threading
import uuid

from flask import Blueprint
from flask import request

from simplyblock_web import utils

from simplyblock_core import db_controller
from simplyblock_core.models.deployer import Deployer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("deployer", __name__)
db_controller = db_controller.DBController()


## Terraform variables
document_name = 'AWS-RunShellScript'
output_key_prefix = 'ssm-output'

region = None
ssm = None
s3 = None
try:
    region = utils.get_aws_region()
    ssm = boto3.client('ssm', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    logger.info("boto3 client created!")
except Exception as e:
    logger.error(f"Exception while connecting to AWS: {e}")


def get_instance_tf_engine_instance_id(workspace: str):
    tag_value = f'{workspace}-tfengine'
    tag_key = 'Name'

    ec2 = boto3.client('ec2', region_name=region)
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

# todo: fetch both STDOUT and STDERR based on
def display_logs(command_id, instance_id, status, tf_logs_bucket_name):
    output_s3_key = f'{output_key_prefix}/{command_id}/{instance_id}/awsrunShellScript/0.awsrunShellScript/stdout'
    error_s3_key = f'{output_key_prefix}/{command_id}/{instance_id}/awsrunShellScript/0.awsrunShellScript/stderr'

    stdout_content = ''
    stderr_content = ''
    # if success get STDOUT. else get STDERR
    if status == 'Success':
        wait_for_s3_object(s3, tf_logs_bucket_name, output_s3_key)
        try:
            stdout_object = s3.get_object(Bucket=tf_logs_bucket_name, Key=output_s3_key)
            stdout_content = stdout_object['Body'].read().decode('utf-8')
            print('Output:')
            print(stdout_content)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                print('No object found - returning empty')
    else:
        wait_for_s3_object(s3, tf_logs_bucket_name, error_s3_key)
        try:
            stderr_object = s3.get_object(Bucket=tf_logs_bucket_name, Key=error_s3_key)
            stderr_content = stderr_object['Body'].read().decode('utf-8')
            print('Error:')
            print(stderr_content)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                print('No object found - returning empty')

    return stdout_content, stderr_content

def wait_for_status(command_id, instance_id):
    status = 'Pending'
    while status not in ['Success', 'Failed', 'Cancelled', 'TimedOut']:
        status = check_command_status(ssm, command_id, instance_id)
        print(f'Current status: {status}')
        if status in ['Success', 'Failed', 'Cancelled', 'TimedOut']:
            break
        time.sleep(5)  # Wait for 5 seconds before checking the status again

    return status

def update_cluster(d, kv_store, storage_nodes, availability_zone):

    print('started update_cluster')
    TFSTATE_BUCKET=d.tf_state_bucket_name
    TFSTATE_KEY='csi'
    TFSTATE_REGION=d.tf_state_bucket_region
    TF_WORKSPACE=d.tf_workspace

    mgmt_nodes = d.mgmt_nodes
    aws_region = d.region
    ECR_REGION = d.ecr_region
    ECR_REPOSITORY_NAME = d.ecr_repository_name
    ECR_IMAGE_TAG = d.ecr_image_tag
    ECR_ACCOUNT_ID = d.ecr_account_id
    tf_logs_bucket_name = d.tf_logs_bucket_name
    sbcli_cmd = d.sbcli_cmd
    mgmt_nodes_instance_type = d.mgmt_nodes_instance_type
    storage_nodes_instance_type = d.storage_nodes_instance_type
    volumes_per_storage_nodes = d.volumes_per_storage_nodes

    d.status = "in_progress"
    d.write_to_db(kv_store)

    instance_ids = get_instance_tf_engine_instance_id(d.tf_workspace)
    if len(instance_ids) == 0:
        # wait for a min and try again before returning error on the API
        print('no instance IDs')
        d.status = "no instance IDs"
        d.write_to_db(kv_store)
        return False, "", "no instance IDs"

    commands = [
        f"""
        docker pull $ECR_ACCOUNT_ID.dkr.ecr.{ECR_REGION}.amazonaws.com/{ECR_REPOSITORY_NAME}:{ECR_IMAGE_TAG}
        docker volume create terraform
        """,
        f"""
        docker run --rm -v terraform:/app -w /app {ECR_ACCOUNT_ID}.dkr.ecr.{ECR_REGION}.amazonaws.com/{ECR_REPOSITORY_NAME}:{ECR_IMAGE_TAG} \
            init -reconfigure -input=false \
                    -backend-config='bucket={TFSTATE_BUCKET}' \
                    -backend-config='key={TFSTATE_KEY}' \
                    -backend-config='region={TFSTATE_REGION}'
        """,
        f"""
        docker run --rm -v terraform:/app -e TF_LOG=DEBUG -w /app {ECR_ACCOUNT_ID}.dkr.ecr.{ECR_REGION}.amazonaws.com/{ECR_REPOSITORY_NAME}:{ECR_IMAGE_TAG} \
            workspace select -or-create {TF_WORKSPACE}
        """,
        f"""
        docker run --rm -v terraform:/app -w /app {ECR_ACCOUNT_ID}.dkr.ecr.{ECR_REGION}.amazonaws.com/{ECR_REPOSITORY_NAME}:{ECR_IMAGE_TAG} \
          plan -var mgmt_nodes={mgmt_nodes} -var storage_nodes={storage_nodes} -var az={availability_zone} -var region={aws_region} \
               -var mgmt_nodes_instance_type={mgmt_nodes_instance_type} -var storage_nodes_instance_type={storage_nodes_instance_type} \
               -var volumes_per_storage_nodes={volumes_per_storage_nodes} -var sbcli_cmd={sbcli_cmd}
        """,
        f"""
        docker run --rm -v terraform:/app -w /app {ECR_ACCOUNT_ID}.dkr.ecr.{ECR_REGION}.amazonaws.com/{ECR_REPOSITORY_NAME}:{ECR_IMAGE_TAG} \
         apply -var mgmt_nodes={mgmt_nodes} -var storage_nodes={storage_nodes} -var az={availability_zone} -var region={aws_region} \
               -var mgmt_nodes_instance_type={mgmt_nodes_instance_type} -var storage_nodes_instance_type={storage_nodes_instance_type} \
               -var volumes_per_storage_nodes={volumes_per_storage_nodes} -var sbcli_cmd={sbcli_cmd} \
            --auto-approve
        """
    ]

    # Send command with S3 output parameters
    try:
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName=document_name,
            Parameters={
                'commands': commands
            },
            OutputS3BucketName=tf_logs_bucket_name,
            OutputS3KeyPrefix=output_key_prefix
        )
    except Exception as e:
        print(f"Exception: {e}")
        d.status = "failed"
        d.write_to_db(kv_store)
        return False, "", "Exception"

    command_id = response['Command']['CommandId']
    print(f'Command ID: {command_id}')
    d.status = f"waiting for status. commandID: {command_id}"
    d.write_to_db(kv_store)

    d.status = wait_for_status(command_id, instance_ids[0])
    d.write_to_db(kv_store)

    if d.status == "Success":
        # get terraform outputs
        commands = [
        f"""
        docker run --rm -v terraform:/app -w /app {ECR_ACCOUNT_ID}.dkr.ecr.{ECR_REGION}.amazonaws.com/{ECR_REPOSITORY_NAME}:{ECR_IMAGE_TAG} \
         output --json
        """
        ]
        response = ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName=document_name,
            Parameters={
                'commands': commands
            },
            OutputS3BucketName=tf_logs_bucket_name,
            OutputS3KeyPrefix=output_key_prefix
        )

        command_id = response['Command']['CommandId']
        print(f'Command ID: {command_id}')
        d.status = "fetching tfouputs"
        d.write_to_db(kv_store)

        d.status = wait_for_status(command_id, instance_ids[0])
        d.write_to_db(kv_store)

        stdout, _ = display_logs(command_id, instance_ids[0], d.status, tf_logs_bucket_name)
        d.tf_output = stdout
        d.storage_nodes = storage_nodes
        d.availability_zone = availability_zone
        d.write_to_db(kv_store)


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
        data.append(d)
    return utils.get_response(data)

def validate_tf_settings(dpl_data):
    """
    """
    if 'tf_state_bucket_name' not in dpl_data:
        return "missing required param: tf_state_bucket_name"
    if 'tf_state_bucket_region' not in dpl_data:
        return "missing required param: tf_state_bucket_region"
    if 'tf_workspace' not in dpl_data:
        return "missing required param: tf_workspace"
    if 'tf_logs_bucket_name' not in dpl_data:
        return "missing required param: tf_logs_bucket_name"
    if 'ecr_account_id' not in dpl_data:
        return "missing required param: ecr_account_id"
    if 'ecr_region' not in dpl_data:
        return "missing required param: ecr_region"
    if 'ecr_repository_name' not in dpl_data:
        return "missing required param: ecr_repository_name"
    if 'ecr_image_tag' not in dpl_data:
        return "missing required param: ecr_image_tag"

    return ""

def validate_tf_vars(dpl_data):
    """
    """
    if 'region' not in dpl_data:
        return "missing required param: region"
    if 'availability_zone' not in dpl_data:
        return "missing required param: availability_zone"
    if 'sbcli_cmd' not in dpl_data:
        return "missing required param: sbcli_cmd"
    if 'sbcli_pkg_version' not in dpl_data:
        return "missing required param: sbcli_pkg_version"
    if 'mgmt_nodes' not in dpl_data:
        return "missing required param: mgmt_nodes"
    if 'storage_nodes' not in dpl_data:
        return "missing required param: storage_nodes"
    if 'mgmt_nodes_instance_type' not in dpl_data:
        return "missing required param: mgmt_nodes_instance_type"
    if 'storage_nodes_instance_type' not in dpl_data:
        return "missing required param: storage_nodes_instance_type"
    if 'volumes_per_storage_nodes' not in dpl_data:
        return "missing required param: volumes_per_storage_nodes"

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

    validation_err = validate_tf_settings(dpl_data)
    if validation_err != "":
        return utils.get_response_error(validation_err, 400)

    # if there are no deployments, create a new one.
    # else update the existing one
    d = get_deployer()
    if d is None:
        d = Deployer()
        d.uuid = str(uuid.uuid4())

    # set TF variables
    d.region = dpl_data['region']
    d.availability_zone = dpl_data['availability_zone']
    d.sbcli_cmd = dpl_data['sbcli_cmd']
    d.sbcli_pkg_version = dpl_data['sbcli_pkg_version']
    d.mgmt_nodes = dpl_data['mgmt_nodes']
    d.storage_nodes = dpl_data['storage_nodes']
    d.mgmt_nodes_instance_type = dpl_data['mgmt_nodes_instance_type']
    d.storage_nodes_instance_type = dpl_data['storage_nodes_instance_type']

    # set TF settings
    d.tf_state_bucket_name = dpl_data['tf_state_bucket_name']
    d.tf_state_bucket_region = dpl_data['tf_state_bucket_region']
    d.tf_workspace = dpl_data['tf_workspace']
    d.tf_logs_bucket_name = dpl_data['tf_logs_bucket_name']
    d.ecr_account_id = dpl_data['ecr_account_id']
    d.ecr_region = dpl_data['ecr_region']
    d.ecr_repository_name = dpl_data['ecr_repository_name']
    d.ecr_image_tag = dpl_data['ecr_image_tag']

    d.write_to_db(db_controller.kv_store)
    return utils.get_response(d.to_dict()), 201

@bp.route('/deployer', methods=['POST'])
def add_deployer():
    """
    This API creates the Infrastructure in a different AZ and on the same existing AWS Account
    """

    # validations
    dpl_data = request.get_json()

    if "uuid" not in dpl_data:
        return utils.get_response_error("missing required param: uuid", 400)

    uuid = dpl_data['uuid']
    d = db_controller.get_deployer_by_id(uuid)

    if "storage_nodes" not in dpl_data:
        return utils.get_response_error("missing required param: storage_nodes", 400)

    if "availability_zone" not in dpl_data:
        return utils.get_response_error("missing required param: availability_zone", 400)

    # start the deployment
    d.status = "started"
    d.write_to_db(db_controller.kv_store)

    storage_nodes = int(dpl_data['storage_nodes'])
    if d.storage_nodes+storage_nodes < 0:
        return utils.get_response_error("total storage_nodes cannot be less than 0", 400)

    availability_zone = dpl_data['availability_zone']
    d.write_to_db(db_controller.kv_store)

    t = threading.Thread(
        target=update_cluster,
        args=(d, db_controller.kv_store, d.storage_nodes+storage_nodes, availability_zone))

    t.start()

    output = {
        "status": d.status,
    }

    return utils.get_response(output, http_code=201)

def get_deployer():
    """
    get the deployer if it exists. Else returns an None
    """
    dpls = db_controller.get_deployers()
    if len(dpls) > 0:
        return dpls[0]
    return None
