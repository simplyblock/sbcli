import base64
import random
import re
import string
from typing import Literal, Optional
import traceback

from flask import jsonify
from pydantic import BaseModel, Field, model_validator

from simplyblock_core import constants


IP_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')


def response_schema(result_schema: dict) -> dict:
    return {
        'type': 'object',
        'required': ['status', 'results'],
        'properties': {
            'status': {'type': 'boolean'},
            'results': result_schema,
            'error': {'type': 'string'},
        },
    }


def get_response(data, error=None, http_code=None):
    resp = {
        "status": True,
        "results": [],
    }
    if error:
        resp['status'] = False
        resp['error'] = error
        if http_code:
            return jsonify(resp), http_code
        else:
            return jsonify(resp)
    if data is not None:
        resp['results'] = data
    return jsonify(resp)


def get_response_error(error=None, http_code=None):
    return get_response(data=None, error=error, http_code=http_code)


def get_response_ok(data):
    return get_response(data)


def generate_string(length):
    return ''.join(random.SystemRandom().choice(
        string.ascii_letters + string.digits) for _ in range(length))


def validate_cpu_mask(spdk_cpu_mask):
    return re.match("^(0x|0X)?[a-fA-F0-9]+$", spdk_cpu_mask)


def get_value_or_default(data, key, default):
    if key in data:
        return data[key]
    return default


def get_int_value_or_default(data, key, default):
    try:
        return int(get_value_or_default(data, key, default))
    except Exception:
        return default


def get_cluster_id(request):
    if "Authorization" in request.headers and request.headers["Authorization"]:
        au = request.headers["Authorization"]
        if len(au.split()) == 2:
            cluster_id = au.split()[0]
            cluster_secret = au.split()[1]
            if cluster_id and cluster_id == "Basic":
                try:
                    tkn = base64.b64decode(cluster_secret).decode('utf-8')
                    if tkn:
                        cluster_id = tkn.split(":")[0]
                        cluster_secret = tkn.split(":")[1]
                except Exception as e:
                    print(e)
                    return

            return cluster_id


def get_aws_region():
    try:
        from ec2_metadata import ec2_metadata
        import requests
        session = requests.session()
        session.timeout = 3
        data = ec2_metadata.EC2Metadata(session=session).instance_identity_document
        return data["region"]
    except Exception:
        pass

    return 'us-east-1'


def error_handler(exception: Exception):
    """Return JSON instead of HTML for any exception."""

    traceback.print_exception(type(exception), exception, exception.__traceback__)

    return {
        'exception': str(exception),
        'stacktrace': [
            (frame.filename, frame.lineno, frame.name, frame.line)
            for frame
            in traceback.extract_tb(exception.__traceback__)
        ]
    }, getattr(exception, 'code', 500)


class RPCPortParams(BaseModel):
    rpc_port: int = Field(constants.RPC_HTTP_PROXY_PORT, ge=0, le=65536)


class DeviceParams(BaseModel):
    device_pci: str


class NVMEConnectParams(BaseModel):
    ip: str = Field(pattern=IP_PATTERN)
    port: int = Field(ge=0, le=65536)
    nqn: str


class DisconnectParams(BaseModel):
    nqn: Optional[str]
    device_path: Optional[str]
    all: Optional[Literal[True]]

    @model_validator(mode='after')
    def verify_mutually_exclusive(self):
        if sum(
            getattr(self, attr) is not None
            for attr in ['nqn', 'device_path', 'all']
        ) != 1:
            raise ValueError('Exactly one of the arguments must be set')
        return self
