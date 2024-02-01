#!/usr/bin/env python
# encoding: utf-8

from functools import wraps
from flask import request

from simplyblock_core import kv_store


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == "GET" and request.path.startswith("/cluster"):
            return f(*args, **kwargs)

        cluster_id = None
        cluster_secret = None
        if "Authorization" in request.headers:
            au = request.headers["Authorization"]
            if len(au.split()) == 2:
                cluster_id = au.split()[0]
                cluster_secret = au.split()[1]

        if not cluster_id or not cluster_secret:
            return {
                "message": "Authentication Token is missing!",
                "data": None,
                "error": "Unauthorized"
            }, 401
        try:
            db_controller = kv_store.DBController()
            cls = db_controller.get_clusters(cluster_id)
            if not cls:
                return {
                    "message": "Invalid Cluster ID",
                    "data": None,
                    "error": "Unauthorized"
                }, 401
            if cls[0].secret != cluster_secret:
                return {
                    "message": "Invalid Cluster secret",
                    "data": None,
                    "error": "Unauthorized"
                }, 401
        except Exception as e:
            return {
                "message": "Something went wrong",
                "data": None,
                "error": str(e)
            }, 500

        return f(*args, **kwargs)

    return decorated
