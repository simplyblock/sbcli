#!/usr/bin/env python
# encoding: utf-8

import base64
import logging
from functools import wraps
from typing import Any, Callable, Dict, Tuple, TypeVar, Union, cast

from flask import request, Response
from werkzeug.wrappers import Response as WerkzeugResponse

from simplyblock_core.db_controller import DBController

# Type variable for the decorated function
F = TypeVar('F', bound=Callable[..., Any])

# Type alias for the response type
AuthResponse = Tuple[Dict[str, Any], int, Dict[str, str]]
ResponseType = Union[Response, WerkzeugResponse, AuthResponse]


def token_required(f: F) -> Callable[..., ResponseType]:
    """
    Decorator to enforce token-based authentication for API endpoints.
    
    Args:
        f: The route function to be decorated
        
    Returns:
        Callable: The decorated function that enforces authentication
    """
    @wraps(f)
    def decorated(*args: Any, **kwargs: Any) -> ResponseType:
        # Skip authentication for Swagger UI
        if request.method == "GET" and request.path.startswith("/swagger"):
            return cast(ResponseType, f(*args, **kwargs))
        if request.method == "POST" and request.path.startswith("/cluster/create_first"):
            return cast(ResponseType, f(*args, **kwargs))
        if request.method == "GET" and request.path.startswith("/health/fdb"):
            return cast(ResponseType, f(*args, **kwargs))            

        cluster_id: str = ""
        cluster_secret: str = ""
        
        # Extract credentials from Authorization header
        if "Authorization" in request.headers:
            auth_header: str = request.headers["Authorization"]
            auth_parts: list[str] = auth_header.split()
            
            if len(auth_parts) == 2:
                cluster_id = auth_parts[0]
                cluster_secret = auth_parts[1]
                
                # Handle Basic Auth
                if cluster_id == "Basic":
                    try:
                        decoded_auth = base64.b64decode(cluster_secret).decode('utf-8')
                        if ":" in decoded_auth:
                            cluster_id, cluster_secret = decoded_auth.split(":", 1)
                    except Exception as e:
                        # Log the error but continue with empty credentials
                        logging.warning(f"Failed to decode Basic Auth: {e}")

        # Authentication headers
        headers: Dict[str, str] = {"WWW-Authenticate": 'Basic realm="Login Required"'}
        
        # Validate credentials presence
        if not cluster_id or not cluster_secret:
            return (
                {
                    "message": "Authentication Token is missing!",
                    "data": None,
                    "error": "Unauthorized"
                }, 
                401, 
                headers
            )
            
        try:
            db_controller = DBController()
            
            try:
                # Get cluster by ID
                cluster = db_controller.get_cluster_by_id(cluster_id)
                
                # Validate cluster secret
                if cluster.secret != cluster_secret:
                    return (
                        {
                            "message": "Invalid Cluster secret",
                            "data": None,
                            "error": "Unauthorized"
                        },
                        401,
                        headers
                    )
                    
            except KeyError:
                return (
                    {
                        "message": "Invalid Cluster ID",
                        "data": None,
                        "error": "Unauthorized"
                    },
                    401,
                    headers
                )
                
            # Authentication successful, proceed with the request
            return cast(ResponseType, f(*args, **kwargs))
            
        except Exception as e:
            logging.error(f"Authentication error: {e}", exc_info=True)
            return (
                {
                    "message": "Something went wrong",
                    "data": None,
                    "error": str(e)
                },
                500,
                {}
            )

    return decorated
