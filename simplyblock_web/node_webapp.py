import argparse
import ssl

from flask_openapi3 import OpenAPI
from werkzeug.serving import ThreadedWSGIServer

from simplyblock_core import utils as core_utils
from simplyblock_core.settings import Settings
from simplyblock_web import utils
from simplyblock_web.api import internal as internal_api

logger = core_utils.get_logger(__name__)

HANDSHAKE_TIMEOUT_SECONDS = 10


class _HandshakeDeferredWSGIServer(ThreadedWSGIServer):
    """Threaded WSGI server that keeps the TLS handshake off the accept loop.

    Werkzeug wraps the *listening* socket with the SSL context, so by default
    every accepted connection also performs its TLS handshake inline inside
    ``socket.accept()`` — which runs in the server's single accept loop
    (``socketserver.BaseServer._handle_request_noblock``), before the
    connection is handed to a worker thread. A client that opens the TCP
    connection and then stalls the handshake blocks every other client from
    being accepted, even though this server is threaded. Marking the
    listening socket ``do_handshake_on_connect=False`` makes accept() return
    immediately; the handshake then happens in ``finish_request``, which
    ``ThreadingMixIn`` already runs in a per-connection thread, with its own
    bounded timeout.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.ssl_context is not None:
            assert isinstance(self.socket, ssl.SSLSocket)
            self.socket.do_handshake_on_connect = False  # type: ignore[attr-defined]  # undeclared in typeshed, present at runtime

    def finish_request(self, request, client_address):
        if self.ssl_context is not None:
            request.settimeout(HANDSHAKE_TIMEOUT_SECONDS)
            try:
                request.do_handshake()
            except OSError:
                logger.warning("TLS handshake with %s failed or timed out", client_address, exc_info=True)
                request.close()
                return
            request.settimeout(None)
        super().finish_request(request, client_address)


app = OpenAPI(__name__)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.register_error_handler(Exception, utils.error_handler)


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


MODES = [
    "storage_node",
    "storage_node_k8s",
]

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=MODES)


if __name__ == '__main__':
    args = parser.parse_args()

    mode = args.mode
    if mode == "storage_node":
        app.register_api(internal_api.storage_node.docker.api)

    if mode == "storage_node_k8s":
        app.register_api(internal_api.storage_node.kubernetes.api)

    settings = Settings()
    server = _HandshakeDeferredWSGIServer(
        '0.0.0.0', 5000, app, ssl_context=settings.make_server_ssl_context(),
    )
    server.log_startup()
    server.serve_forever()
