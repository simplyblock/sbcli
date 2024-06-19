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
ssm = boto3.client('ssm', aws_region='us-east-1')
s3 = boto3.client('s3', aws_region='us-east-1')


def get_instance_tf_engine_instance_id():
    tag_value = 'tfengine'
    tag_key = 'Name'

    ec2 = boto3.client('ec2', aws_region='us-east-1')
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
    response = ssm_client.get_command_invocation(
        CommandId=command_id,
        InstanceId=instance_id
    )
    return response['Status']

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

def main(TFSTATE_BUCKET, TFSTATE_KEY, TFSTATE_REGION, TF_WORKSPACE, storage_nodes, mgmt_nodes, availability_zone, aws_region):
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


@bp.route('/deployer/<string:uuid>', methods=['PUT'])
def update_deployer(uuid):
    dpl = db_controller.get_deployer_by_id(uuid)
    if not dpl:
        return utils.get_response_error(f"Deployer not found: {uuid}", 404)

    dpl_data = request.get_json()
    if 'snodes' not in dpl_data:
        return utils.get_response_error("missing required param: snodes", 400)
    if 'az' not in dpl_data:
        return utils.get_response_error("missing required param: az", 400)

    dpl.snodes = dpl_data['snodes']
    dpl.az = dpl_data['az']
    dpl.write_to_db(db_controller.kv_store)

    # tf state parameter
    # todo: take these params from DB
    TFSTATE_BUCKET='simplyblock-terraform-state-bucket'
    TFSTATE_KEY='csi'
    TFSTATE_REGION=dpl.az
    TF_WORKSPACE='manohar'

    # tf mgmt node parameters
    storage_nodes = 4
    mgmt_nodes = 1
    availability_zone = "us-east-1a"
    aws_region = "us-east-1"
    status, stdout, stderr = main(
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

@bp.route('/deployer', methods=['POST'])
def add_deployer():
    dpl_data = request.get_json()
    if 'snodes' not in dpl_data:
        return utils.get_response_error("missing required param: snodes", 400)
    if 'snodes_type' not in dpl_data:
        return utils.get_response_error("missing required param: snodes_type", 400)
    if 'mnodes' not in dpl_data:
        return utils.get_response_error("missing required param: mnodes", 400)
    if 'mnodes_type' not in dpl_data:
        return utils.get_response_error("missing required param: mnodes_type", 400)
    if 'az' not in dpl_data:
        return utils.get_response_error("missing required param: az", 400)
    if 'cluster_id' not in dpl_data:
        return utils.get_response_error("missing required param: cluster_id", 400)
    if 'region' not in dpl_data:
        return utils.get_response_error("missing required param: region", 400)
    if 'workspace' not in dpl_data:
        return utils.get_response_error("missing required param: workspace", 400)
    if 'bucket_name' not in dpl_data:
        return utils.get_response_error("missing required param: bucket_name", 400)

    d = Deployer()
    d.uuid        = dpl_data['cluster_id']
    d.snodes      = dpl_data['snodes']
    d.snodes_type = dpl_data['snodes_type']
    d.mnodes      = dpl_data['mnodes']
    d.mnodes_type = dpl_data['mnodes_type']
    d.az          = dpl_data['az']
    d.region      = dpl_data['region']
    d.workspace   = dpl_data['workspace']
    d.bucket_name = dpl_data['bucket_name']
    d.write_to_db(db_controller.kv_store)

    return utils.get_response(d.to_dict()), 201

## Todo check if there is already an instance running. Maybe use Step function
## Todo: How to share the ECR Image with the customer?
## Todo: aws ecr get-login-password --region us-east-1 --> automate ECR Login
## Todo: Alert when the instance is inactive or the command is undeliverable
# if the command status is a failure, the code gets stuck
# SSM Ping status: Connection lost: --> Push a metric to CW. And configure ASG Rule based on the Ping Status
# if the command is successful, increment the numbers.
