"""Container liveness probe for the web API: ``python3 -m simplyblock_web.healthcheck``.

"Running" only says PID 1 exists. A replica whose event loop has died can keep
its listen socket open, so the kernel goes on completing connections nobody
serves and clients hang instead of failing over. This probe asks the server
itself and fails when no answer arrives in time, letting Docker Swarm replace
the task.

It probes the v2 liveness endpoint, ``/api/v2/_meta/health``, and deliberately
not ``/api/v2/_meta/ready``: an FDB outage would fail every replica at once, and
restarting them would not bring FDB back. The endpoint is mounted with the v2
API, so the probe needs v2 enabled (``SB_API_VERSIONS`` includes 2, the default).
"""

import os
import ssl
import sys
import urllib.error
import urllib.request

from simplyblock_core.settings import Settings

LIVE_PATH = "/api/v2/_meta/health"
DEFAULT_TIMEOUT_SEC = 5.0


def probe(port: int, tls: bool, timeout: float = DEFAULT_TIMEOUT_SEC) -> None:
    """Return when the server answers ``LIVE_PATH``; raise otherwise.

    The connection goes to localhost, so the certificate is not verified. A TLS
    handshake the server rejects (client auth required, the probe holds no client
    certificate) still counts as live: uvicorn performs the handshake on its
    event loop, so a rejection is the loop answering.
    """
    scheme = "https" if tls else "http"
    context = None
    if tls:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(
                f"{scheme}://127.0.0.1:{port}{LIVE_PATH}", timeout=timeout, context=context):
            pass
    except urllib.error.URLError as e:
        if isinstance(e.reason, ssl.SSLError):
            return
        raise


def main() -> None:
    port = int(os.environ.get("FLASK_PORT", 5000))
    try:
        probe(port, tls=Settings().tls_serve)
    except OSError as e:
        print(f"web API on port {port} is not live: {e!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
