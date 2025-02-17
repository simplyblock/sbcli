#!/usr/bin/env python3

import base64
import json
import logging
import os
import socket
import ssl
import sys

from http.server import HTTPServer
from http.server import ThreadingHTTPServer
from http.server import BaseHTTPRequestHandler


rpc_sock = '/var/tmp/spdk.sock'
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.INFO)


def get_env_var(name, default=None, is_required=False):
    if not name:
        logger.warning("Invalid env var name %s", name)
        return False
    if name not in os.environ and is_required:
        logger.error("env value is required: %s" % name)
        raise Exception("env value is required: %s" % name)
    return os.environ.get(name, default)


def rpc_call(req):
    req_data = json.loads(req.decode('ascii'))
    logger.info(f"Request function: {str(req_data['method'])}")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.connect(rpc_sock)
    sock.sendall(req)

    if 'id' not in req_data:
        sock.close()
        return None

    buf = ''
    closed = False
    response = None

    while not closed:
        newdata = sock.recv(1024)
        if newdata == b'':
            closed = True
        buf += newdata.decode('ascii')
        try:
            response = json.loads(buf)
        except ValueError:
            continue  # incomplete response; keep buffering
        break

    sock.close()

    if not response and len(buf) > 0:
        raise

    logger.debug(f"Response data: {buf}")

    return buf


class ServerHandler(BaseHTTPRequestHandler):

    key = ""

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_HEAD_no_content(self):
        self.send_response(204)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_AUTHHEAD(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'text/html')
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_INTERNALERROR(self):
        self.send_response(500)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_POST(self):
        if self.headers['Authorization'] != 'Basic ' + self.key:
            self.do_AUTHHEAD()
        else:
            if "Content-Length" in self.headers:
                data_string = self.rfile.read(int(self.headers['Content-Length']))
            elif "chunked" in self.headers.get("Transfer-Encoding", ""):
                data_string = b''
                while True:
                    line = self.rfile.readline().strip()
                    chunk_length = int(line, 16)

                    if chunk_length != 0:
                        chunk = self.rfile.read(chunk_length)
                        data_string += chunk

                    # Each chunk is followed by an additional empty newline
                    # that we have to consume.
                    self.rfile.readline()

                    # Finally, a chunk size of 0 is an end indication
                    if chunk_length == 0:
                        break

            try:
                response = rpc_call(data_string)
                if response is not None:
                    self.do_HEAD()
                    self.wfile.write(bytes(response.encode(encoding='ascii')))
                else:
                    self.do_HEAD_no_content()

            except ValueError:
                self.do_INTERNALERROR()


def run_server(host, port, user, password, cert=None, is_threading_enabled=False):
    # encoding user and password
    key = base64.b64encode((user+':'+password).encode(encoding='ascii')).decode('ascii')

    try:
        ServerHandler.key = key
        if is_threading_enabled:
            httpd = ThreadingHTTPServer((host, port), ServerHandler)
        else:
            httpd = HTTPServer((host, port), ServerHandler)
        httpd.timeout = TIMEOUT
        if cert is not None:
            httpd.socket = ssl.wrap_socket(httpd.socket, certfile=cert, server_side=True)
        logger.info('Started RPC http proxy server')
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down server')
        httpd.socket.close()


TIMEOUT = int(get_env_var("TIMEOUT", is_required=False, default=60*5))
is_threading_enabled = get_env_var("MULTI_THREADING_ENABLED", is_required=False, default=False)
server_ip = get_env_var("SERVER_IP", is_required=True)
rpc_port = get_env_var("RPC_PORT", is_required=True)
rpc_username = get_env_var("RPC_USERNAME", is_required=True)
rpc_password = get_env_var("RPC_PASSWORD", is_required=True)

try:
    rpc_port = int(rpc_port)
except Exception:
    rpc_port = 8080

is_threading_enabled = bool(is_threading_enabled)
run_server(server_ip, rpc_port, rpc_username, rpc_password, is_threading_enabled=is_threading_enabled)
