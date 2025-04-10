#!/usr/bin/env python
# encoding: utf-8

import base64
from functools import wraps
from flask import request

from simplyblock_core.db_controller import DBController


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):

        if request.method == "GET" and request.path.startswith("/swagger"):
            return f(*args, **kwargs)

        cluster_id = None
        cluster_secret = None
        if "Authorization" in request.headers:
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

        headers = {"WWW-Authenticate": 'Basic realm="Login Required"'}
        if not cluster_id or not cluster_secret:
            return {
                "message": "Authentication Token is missing!",
                "data": None,
                "error": "Unauthorized"
            }, 401, headers
        try:
            db_controller = DBController()
            cluster = db_controller.get_cluster_by_id(cluster_id)
            if not cluster:
                return {
                    "message": "Invalid Cluster ID",
                    "data": None,
                    "error": "Unauthorized"
                }, 401, headers
            if cluster.secret != cluster_secret:
                return {
                    "message": "Invalid Cluster secret",
                    "data": None,
                    "error": "Unauthorized"
                }, 401, headers
        except Exception as e:
            return {
                "message": "Something went wrong",
                "data": None,
                "error": str(e)
            }, 500

        return f(*args, **kwargs)

    return decorated
