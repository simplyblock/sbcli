import random
import re
import string

from flask import jsonify


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


def get_csi_response(data, error=None, http_code=None):
    resp = {
        "result": "",
        "error": {
            "code": 0,
            "message": ""
        }
    }
    if error:
        resp['error']['code'] = 1
        resp['error']['message'] = error
        return jsonify(resp)
    if data is not None:
        resp['result'] = data
    return jsonify(resp)


def get_response_error(error=None, http_code=None):
    return get_response(data=None, error=error, http_code=http_code)


def get_response_ok(data):
    return get_response(data)


def generate_string(length):
    return ''.join(random.SystemRandom().choice(
        string.ascii_letters + string.digits) for _ in range(length))


def parse_size(size_string: str):
    try:
        x = int(size_string)
        return x
    except Exception:
        pass
    try:
        if size_string:
            size_string = size_string.lower()
            size_string = size_string.replace(" ", "")
            size_string = size_string.replace("b", "")
            size_number = int(size_string[:-1])
            size_v = size_string[-1]
            if size_v == "k":
                return size_number * 1024
            if size_v == "m":
                return size_number * 1024 * 1024
            elif size_v == "g":
                return size_number * 1024 * 1024 * 1024
            elif size_v == "t":
                return size_number * 1024 * 1024 * 1024 * 1024
            else:
                print(f"Error parsing size: {size_string}")
                return -1
        else:
            return -1
    except:
        print(f"Error parsing size: {size_string}")
        return -1


def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    if not B:
        return ""
    B = float(B)
    KB = float(1000)
    MB = float(KB ** 2) # 1,048,576
    GB = float(KB ** 3) # 1,073,741,824
    TB = float(KB ** 4) # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.1f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.1f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.1f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.1f} TB'.format(B / TB)


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
