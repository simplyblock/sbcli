import math
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
            one_k = 1000
            multi = 0
            if size_v == "k":
                multi = 1
            elif size_v == "m":
                multi = 2
            elif size_v == "g":
                multi = 3
            elif size_v == "t":
                multi = 4
            else:
                print(f"Error parsing size: {size_string}")
                return -1
            return int(size_number * math.pow(one_k, multi))
        else:
            return -1
    except:
        print(f"Error parsing size: {size_string}")
        return -1


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
    au = request.headers["Authorization"]
    if len(au.split()) == 2:
        cluster_id = au.split()[0]
        cluster_secret = au.split()[1]
        return cluster_id
