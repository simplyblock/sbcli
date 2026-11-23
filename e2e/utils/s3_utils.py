"""Helpers for the ``ObjectStore`` / ``S3Bucket`` suite.

Everything here is a *client* of a deployed cluster. Nothing imports
``simplyblock_core``, ``simplyblock_cli`` or ``simplyblock_web``: the harness
and the system under test are independently versioned, so an import would bind
an assertion to the runner's copy of the product rather than the cluster's
(``e2e/AGENTS.md``). The handful of product constants used below are mirrored
with a comment naming their upstream definition, and the two have to move
together.

## Why the S3 traffic is driven from inside the cluster

The gateway's Service is a ClusterIP on ``spec.port``, serving TLS. Reaching it
from the runner means either a NodePort or a port-forward, and dev's own
``csi-driver/e2e/s3.go`` records that a port-forward "drops connections under
the randomized spec's concurrency", which is why they create a NodePort
instead. On the OCP lab there is a second problem: the data network is
``192.168.10.x`` on ``enp2s0f0`` while the runner talks to the cluster over
the mgmt network, so a NodePort on a worker is not reliably routable from
where the test runs.

So the traffic comes from a pod. :class:`S3Pod` keeps one long-lived client pod
and drives it with ``kubectl exec``, which removes every routing question and
makes "which gateway pod served this" an explicit choice rather than whatever
the Service's load balancing did.

## Why the client is written out by hand

The pod runs a stock UBI python image with no wheels available: a cluster that
cannot reach PyPI is the normal case on these labs, and an image that can is a
dependency the suite does not need. SigV4 over ``http.client`` is about eighty
lines and has no moving parts, so :data:`S3_CLIENT_PY` carries the whole client
and is copied into the pod once.

Payloads are never shipped through ``kubectl exec``. The pod derives object
bodies from ``(key, seed, size)`` with a SHA-256 keystream, so a 64 MiB write
and its byte-for-byte verification both cost one short command and the
comparison happens where the bytes are.
"""

import json
import re
import shlex
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request

from logger_config import setup_logger

logger = setup_logger(__name__)


# ── Errors ───────────────────────────────────────────────────────────────
#
# Split the same way pnfs_utils does, and for the same reason: a failure of
# the harness and a failure of the product read identically in a log unless
# the exception type says which one it is.

class S3EvidenceError(AssertionError):
    """The product did something the design says it must not."""


class S3ProbeError(S3EvidenceError):
    """The harness could not obtain the evidence. Not a product verdict."""


# ── Mirrored product constants ───────────────────────────────────────────
#
# Each of these is defined in simplyblock-operator and duplicated here rather
# than imported. A reviewer changing the upstream definition can grep for the
# counterpart; an import would have silently asserted the runner's value.

#: ``S3BucketFinalizer`` in ``operator/api/v1alpha2/s3bucket_types.go``.
S3BUCKET_FINALIZER = "storage.simplyblock.io/s3bucket"

#: ``S3BucketOwnerAnnotation``, same file. Present only on a bucket the
#: gateway created for a ``CreateBucket`` request, absent on a bucket created
#: directly through the CRD.
S3BUCKET_OWNER_ANNOTATION = "storage.simplyblock.io/s3-owner"

#: Every gateway pod carries this label keyed by its ObjectStore's name. It is
#: how ``S3BucketReconciler`` finds the pods without reading the ObjectStore,
#: and how this suite finds them without reading the Deployment
#: (``objectstore_types.go``, the type's own doc comment).
GATEWAY_LABEL = "storage.simplyblock.io/objectstore"

#: The operator's own Deployment and the label its pods carry, from the
#: operator chart's ``templates/simplyblock-operator.yaml``. Not
#: ``control-plane=controller-manager``: that is kubebuilder's scaffold label,
#: which the chart does not use for the operator, and which the csi-addons and
#: ramen controllers in the same chart do carry.
OPERATOR_DEPLOYMENT = "simplyblock-operator"
OPERATOR_SELECTOR = "app=simplyblock-operator"

#: ``S3BucketPhase`` / ``ObjectStorePhase`` enums. ``Deleting`` exists only on
#: a bucket.
BUCKET_PHASES = ("Pending", "Provisioning", "Ready", "Degraded", "Deleting")
STORE_PHASES = ("Pending", "Provisioning", "Ready", "Degraded")

#: ``S3BucketSpec.BucketName``'s kubebuilder pattern. Mirrored so the boundary
#: case can generate names that sit exactly on it rather than near it.
BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

#: Condition types the two status blocks are documented to carry.
BUCKET_CONDITIONS = ("VolumeBound", "ExportReady", "MountDelivered")
STORE_CONDITIONS = ("DeploymentAvailable", "CertificateReady")

#: Event reasons from ``design-bucket-provisioning.md`` §10 and
#: ``design-objectstore.md`` §9.
EVENT_BUCKET_READY = "BucketReady"
EVENT_PVC_NOT_BOUND = "PersistentVolumeClaimNotBound"
EVENT_NO_MATCHING_EXPORT = "NoMatchingExport"
EVENT_OBJECTSTORE_NOT_FOUND = "ObjectStoreNotFound"
EVENT_MOUNT_DELIVERY_FAILED = "MountDeliveryFailed"

#: Operator metrics this suite scrapes. All five are new in the ObjectStore
#: work (``objectstore/metrics.go``, ``collector.go``).
METRIC_PROVISIONING_SECONDS = "simplyblock_objectstore_bucket_provisioning_seconds"
METRIC_BUCKET_PHASE = "simplyblock_objectstore_bucket_phase_state"
METRIC_DELIVERY_CALLS = "simplyblock_objectstore_mount_delivery_calls_total"
METRIC_STORE_PHASE = "simplyblock_objectstore_phase_state"
METRIC_READY_REPLICAS = "simplyblock_objectstore_ready_replicas_count"

#: Seconds between the operator's rebuilds of the two phase gauges, so a
#: phase reads up to this stale. Mirrors ``DefaultCollectInterval`` in the
#: operator's ``internal/controllers/objectstore/collector.go``; the two must
#: move together.
PHASE_COLLECT_INTERVAL = 30


# ── The in-pod S3 client ─────────────────────────────────────────────────

#: Written to ``/tmp/s3cli.py`` in the client pod once, then invoked per call
#: with a JSON request on argv. Pure stdlib: hmac, hashlib, http.client, ssl.
S3_CLIENT_PY = r'''
import base64, datetime, hashlib, hmac, http.client, json, ssl, sys, urllib.parse

ALGO = "AWS4-HMAC-SHA256"
EMPTY_SHA = hashlib.sha256(b"").hexdigest()


def keystream(key, seed, size):
    """Deterministic body for (key, seed, size).

    The harness never holds these bytes -- it holds the three inputs. That is
    what makes a 64 MiB round trip one short exec instead of a transfer.
    """
    out = bytearray()
    counter = 0
    base = ("%s|%s|" % (key, seed)).encode()
    while len(out) < size:
        out += hashlib.sha256(base + str(counter).encode()).digest()
        counter += 1
    return bytes(out[:size])


def _sign(secret, date, region, service, to_sign):
    k = ("AWS4" + secret).encode()
    for part in (date, region, service, "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    return hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()


def request(cfg, method, path, query=None, body=b"", extra_headers=None):
    host = cfg["host"]
    port = int(cfg["port"])
    region = cfg.get("region", "us-east-1")
    now = datetime.datetime.now(datetime.timezone.utc)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest() if body else EMPTY_SHA

    canonical_query = ""
    if query:
        canonical_query = "&".join(
            "%s=%s" % (urllib.parse.quote(k, safe="-_.~"),
                       urllib.parse.quote(str(v), safe="-_.~"))
            for k, v in sorted(query.items()))

    netloc = "%s:%d" % (host, port)
    headers = {
        "host": netloc,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amzdate,
    }
    for k, v in (extra_headers or {}).items():
        headers[k.lower()] = v

    signed = ";".join(sorted(headers))
    canonical_headers = "".join(
        "%s:%s\n" % (k, str(headers[k]).strip()) for k in sorted(headers))
    canonical_path = urllib.parse.quote(path, safe="/-_.~")
    canonical = "\n".join([method, canonical_path, canonical_query,
                           canonical_headers, signed, payload_hash])
    scope = "%s/%s/s3/aws4_request" % (datestamp, region)
    to_sign = "\n".join(
        [ALGO, amzdate, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    sig = _sign(cfg["secret"], datestamp, region, "s3", to_sign)
    headers["authorization"] = (
        "%s Credential=%s/%s, SignedHeaders=%s, Signature=%s"
        % (ALGO, cfg["access"], scope, signed, sig))

    if cfg.get("tls", True):
        if cfg.get("ca"):
            ctx = ssl.create_default_context(cafile=cfg["ca"])
            # The Service name is what the serving certificate is issued for.
            ctx.check_hostname = True
        else:
            ctx = ssl._create_unverified_context()
        conn = http.client.HTTPSConnection(
            host, port, timeout=cfg.get("timeout", 120), context=ctx)
    else:
        conn = http.client.HTTPConnection(
            host, port, timeout=cfg.get("timeout", 120))

    url = canonical_path + (("?" + canonical_query) if canonical_query else "")
    conn.request(method, url, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    out = {"status": resp.status, "headers": dict(resp.getheaders())}
    conn.close()
    return out, data


def error_code(blob):
    """The S3 error code out of an error document, or "" when there is none.

    Reported verbatim rather than mapped: a case that asserts a specific code
    has to be able to say what actually came back when it was something else.
    """
    try:
        text = blob.decode("utf-8", "replace")
    except Exception:
        return ""
    start = text.find("<Code>")
    if start < 0:
        return ""
    end = text.find("</Code>", start)
    return text[start + 6:end] if end > start else ""


def main():
    req = json.loads(sys.argv[1])
    cfg = req["cfg"]
    op = req["op"]
    res = {"op": op}

    if op == "list_buckets":
        head, body = request(cfg, "GET", "/")
        res["status"] = head["status"]
        res["body"] = body.decode("utf-8", "replace")[:20000]
        res["buckets"] = [
            b.split("</Name>")[0]
            for b in body.decode("utf-8", "replace").split("<Name>")[1:]]

    elif op in ("create_bucket", "delete_bucket", "head_bucket"):
        method = {"create_bucket": "PUT", "delete_bucket": "DELETE",
                  "head_bucket": "HEAD"}[op]
        head, body = request(cfg, method, "/" + req["bucket"])
        res["status"] = head["status"]
        res["code"] = error_code(body)
        res["body"] = body.decode("utf-8", "replace")[:4000]

    elif op == "put_object":
        data = keystream(req["key"], req["seed"], req["size"])
        head, body = request(
            cfg, "PUT", "/%s/%s" % (req["bucket"], req["key"]), body=data)
        res["status"] = head["status"]
        res["code"] = error_code(body)
        res["sha256"] = hashlib.sha256(data).hexdigest()
        res["etag"] = head["headers"].get("ETag", "")
        res["body"] = body.decode("utf-8", "replace")[:4000]

    elif op == "get_object":
        head, body = request(
            cfg, "GET", "/%s/%s" % (req["bucket"], req["key"]))
        res["status"] = head["status"]
        res["code"] = error_code(body)
        res["length"] = len(body)
        res["sha256"] = hashlib.sha256(body).hexdigest()
        if "seed" in req and "size" in req:
            want = keystream(req["key"], req["seed"], req["size"])
            res["expected_sha256"] = hashlib.sha256(want).hexdigest()
            res["match"] = (body == want)
            if body != want:
                # Where it first differs, and both bytes. A mismatch that
                # reports only "False" cannot be told from a short read, and
                # the two have completely different causes.
                first = next((i for i in range(min(len(body), len(want)))
                              if body[i] != want[i]), min(len(body), len(want)))
                res["first_diff"] = first
                res["got_at_diff"] = body[first:first + 16].hex()
                res["want_at_diff"] = want[first:first + 16].hex()
        else:
            res["body_b64"] = base64.b64encode(body[:4000]).decode()

    elif op == "delete_object":
        head, body = request(
            cfg, "DELETE", "/%s/%s" % (req["bucket"], req["key"]))
        res["status"] = head["status"]
        res["code"] = error_code(body)

    elif op == "list_objects":
        head, body = request(cfg, "GET", "/" + req["bucket"],
                             query={"list-type": 2, "max-keys": req.get("max", 1000)})
        text = body.decode("utf-8", "replace")
        res["status"] = head["status"]
        res["keys"] = [k.split("</Key>")[0] for k in text.split("<Key>")[1:]]

    elif op == "fill":
        # Write until something refuses, so the capacity case can report the
        # code the gateway actually returns rather than the one we expected.
        written = []
        for i in range(req["count"]):
            key = "%s%05d" % (req["prefix"], i)
            data = keystream(key, req["seed"], req["size"])
            try:
                head, body = request(
                    cfg, "PUT", "/%s/%s" % (req["bucket"], key), body=data)
            except Exception as exc:
                res["stopped_at"] = i
                res["exception"] = "%s: %s" % (type(exc).__name__, exc)
                break
            if head["status"] >= 300:
                res["stopped_at"] = i
                res["status"] = head["status"]
                res["code"] = error_code(body)
                res["body"] = body.decode("utf-8", "replace")[:4000]
                break
            written.append(key)
        res["written"] = written
        res["written_count"] = len(written)

    else:
        res["error"] = "unknown op %s" % op

    print(json.dumps(res))


main()
'''


# ── Driving the client pod ───────────────────────────────────────────────

class S3Pod:
    """One long-lived pod that speaks S3 to a gateway.

    *endpoint* is whatever the case wants reached: the ObjectStore's ClusterIP
    Service for ordinary work, or a single gateway pod's own address when the
    case has to prove that *that* pod serves a bucket. Load balancing across a
    broken replica is exactly what TC-S3-005 and TC-S3-104 exist to catch, so
    the choice is never left to the Service.
    """

    #: Red Hat's registry, not Docker Hub, and deliberately: an anonymous
    #: Docker Hub pull has already cost this lab a run (ImagePullBackOff read
    #: as an operator bug). An OpenShift node needs no credential for
    #: registry.access.redhat.com, and the image ships python3, which is the
    #: whole requirement.
    IMAGE = "registry.access.redhat.com/ubi9/python-311:latest"

    def __init__(self, k8s, name, namespace=None):
        self.k8s = k8s
        self.name = name
        self.namespace = namespace or k8s.namespace
        self.logger = logger
        self._installed = False

    def start(self, timeout=300):
        self.k8s.apply_yaml(
            "apiVersion: v1\n"
            "kind: Pod\n"
            "metadata:\n"
            f"  name: {self.name}\n"
            "  labels:\n"
            "    simplyblock.io/role: s3-client\n"
            "spec:\n"
            "  restartPolicy: Never\n"
            "  containers:\n"
            "  - name: client\n"
            f"    image: {self.IMAGE}\n"
            '    command: ["sleep", "infinity"]\n',
            namespace=self.namespace)
        self._wait_running(timeout)
        self._install()
        return self

    def _wait_running(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self.k8s._exec_kubectl(
                f"kubectl get pod {self.name} -n {self.namespace} "
                f"-o jsonpath='{{.status.phase}}' 2>/dev/null || true",
                supress_logs=True)
            if (out or "").strip() == "Running":
                return
            time.sleep(5)
        raise S3ProbeError(
            f"S3 client pod {self.name} never reached Running in {timeout}s; "
            f"events: {self.k8s.get_pod_events(self.name, self.namespace)[-800:]}")

    def _install(self):
        heredoc = (
            "cat > /tmp/s3cli.py <<'__S3CLI__'\n" + S3_CLIENT_PY + "\n__S3CLI__\n"
            "python3 -c \"import py_compile;py_compile.compile('/tmp/s3cli.py',"
            "doraise=True)\" && echo INSTALLED")
        out, err = self.k8s.exec_in_pod(self.name, heredoc,
                                        namespace=self.namespace, timeout=120)
        if "INSTALLED" not in (out or ""):
            raise S3ProbeError(
                f"could not install the S3 client in {self.name}: "
                f"{(err or out or '')[-600:]}")
        self._installed = True

    def call(self, cfg, op, timeout=600, **kwargs):
        """One S3 operation. Returns the parsed result dict.

        A non-zero exit or unparseable stdout is a harness failure and raises;
        an S3-level error is *data*, returned in the dict, because several
        cases here assert on the specific code that came back.
        """
        if not self._installed:
            self._install()
        req = dict(kwargs)
        req["cfg"] = cfg
        req["op"] = op
        cmd = f"python3 /tmp/s3cli.py {shlex.quote(json.dumps(req))}"
        out, err = self.k8s.exec_in_pod(self.name, cmd,
                                        namespace=self.namespace, timeout=timeout)
        text = (out or "").strip().splitlines()
        for line in reversed(text):
            try:
                return json.loads(line)
            except Exception:
                continue
        raise S3ProbeError(
            f"S3 {op} produced no JSON from {self.name}. "
            f"stdout={(out or '')[-600:]!r} stderr={(err or '')[-600:]!r}")

    def delete(self):
        self.k8s.delete_resource("pod", self.name, self.namespace)


def gateway_cfg(host, port, access, secret, region="us-east-1",
                tls=True, ca=None, timeout=120):
    """The connection dict the in-pod client takes."""
    return {"host": host, "port": int(port), "access": access,
            "secret": secret, "region": region, "tls": tls,
            "ca": ca, "timeout": timeout}


# ── Reading the CRDs ─────────────────────────────────────────────────────

def objectstore(k8s, name, namespace=None):
    return k8s.get_resource_json("objectstore", name, namespace)


def s3bucket(k8s, name, namespace=None):
    return k8s.get_resource_json("s3bucket", name, namespace)


def phase_of(obj):
    return ((obj or {}).get("status") or {}).get("phase", "")


def condition(obj, ctype):
    """One condition by type, or ``{}``.

    Returned whole rather than as a bool: a Degraded object's ``reason`` and
    ``message`` are the only thing that says *why*, and a case that asserts
    only ``status == "False"`` throws that away.
    """
    for c in (((obj or {}).get("status") or {}).get("conditions") or []):
        if c.get("type") == ctype:
            return c
    return {}


def delivered_nodes(bucket_obj):
    return sorted(
        ((bucket_obj or {}).get("status") or {}).get("mountDeliveredNodes") or [])


def wait_phase(k8s, kind, name, want, timeout=900, namespace=None,
               poll=5, forbid=()):
    """Wait for *name* to reach any phase in *want*.

    *forbid* short-circuits: a bucket that has gone ``Degraded`` is not going
    to become ``Ready`` by waiting another ten minutes, and failing at the
    moment it degrades keeps the surrounding cluster state close to the cause.
    """
    want = (want,) if isinstance(want, str) else tuple(want)
    forbid = (forbid,) if isinstance(forbid, str) else tuple(forbid)
    deadline = time.time() + timeout
    seen = []
    last = None
    while time.time() < deadline:
        obj = k8s.get_resource_json(kind, name, namespace)
        ph = phase_of(obj)
        if ph and ph != last:
            seen.append((time.strftime("%H:%M:%S"), ph))
            logger.info("[s3] %s/%s -> %s", kind, name, ph)
            last = ph
        if ph in want:
            return obj, seen
        if ph in forbid:
            raise S3EvidenceError(
                f"{kind}/{name} reached {ph}, which this case forbids. "
                f"message={((obj.get('status') or {}).get('message', ''))!r} "
                f"conditions={((obj.get('status') or {}).get('conditions') or [])}")
        time.sleep(poll)
    obj = k8s.get_resource_json(kind, name, namespace)
    raise S3EvidenceError(
        f"{kind}/{name} never reached {want} in {timeout}s; last phase "
        f"{phase_of(obj)!r}, transitions {seen}, "
        f"message={((obj.get('status') or {}).get('message', ''))!r}")


def wait_gone(k8s, kind, name, timeout=600, namespace=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not k8s.get_resource_json(kind, name, namespace):
            return True
        time.sleep(5)
    obj = k8s.get_resource_json(kind, name, namespace)
    raise S3EvidenceError(
        f"{kind}/{name} still exists after {timeout}s. "
        f"finalizers={obj.get('metadata', {}).get('finalizers')} "
        f"phase={phase_of(obj)!r}")


def events_for(k8s, kind, name, namespace=None):
    """Event reasons recorded against one object, newest last."""
    ns = namespace or k8s.namespace
    out, _ = k8s._exec_kubectl(
        f"kubectl get events -n {ns} "
        f"--field-selector involvedObject.kind={kind},involvedObject.name={name} "
        f"-o jsonpath='{{range .items[*]}}{{.reason}} {{.type}} {{.message}}{{\"\\n\"}}{{end}}' "
        f"2>/dev/null || true", supress_logs=True)
    return [ln for ln in (out or "").splitlines() if ln.strip()]


# ── Gateway topology ─────────────────────────────────────────────────────

def gateway_pods(k8s, store, namespace=None):
    """``[(pod, node, ip, ready)]`` for one ObjectStore's gateway pods.

    Read off the pod label rather than the Deployment, which is how the
    product's own reconciler finds them -- so a case asserting on this set is
    asserting on what the operator sees, not on a parallel idea of it.
    """
    ns = namespace or k8s.namespace
    out, _ = k8s._exec_kubectl(
        f"kubectl get pods -n {ns} -l {GATEWAY_LABEL}={store} "
        f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} {{.spec.nodeName}} "
        f"{{.status.podIP}} {{.status.containerStatuses[0].ready}}{{\"\\n\"}}{{end}}' "
        f"2>/dev/null || true", supress_logs=True)
    rows = []
    for line in (out or "").splitlines():
        parts = line.split()
        if len(parts) == 4:
            rows.append((parts[0], parts[1], parts[2], parts[3] == "true"))
    return rows


def gateway_nodes(k8s, store, namespace=None, ready_only=True):
    return sorted({n for _, n, _, ready in gateway_pods(k8s, store, namespace)
                   if ready or not ready_only})


def wait_gateway_pods(k8s, store, count, timeout=600, namespace=None,
                      ready=True):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = gateway_pods(k8s, store, namespace)
        have = [p for p in last if p[3]] if ready else last
        if len(have) == count:
            return have
        time.sleep(5)
    raise S3EvidenceError(
        f"ObjectStore {store} never settled at {count} "
        f"{'ready ' if ready else ''}gateway pod(s) in {timeout}s; saw {last}")


def service_host(store, namespace):
    """The in-cluster DNS name of a gateway's Service.

    The ObjectStore reconciler names the Service after the ObjectStore
    (``buildService``), so this is derived rather than looked up -- a lookup
    would hide a rename, and a rename is something this suite should fail on.
    """
    return f"{store}.{namespace}.svc.cluster.local"


# ── Operator metrics ─────────────────────────────────────────────────────

#: The operator pod's container port for the manager's controller-runtime
#: secure metrics endpoint (self-signed certificate). The operator chart
#: declares it only when installed with ``operatorMetrics.enabled=true`` (off
#: by default), and puts no Service in front of it: the metrics belong to one
#: pod. Addressed by name so the chart's ``operatorMetrics.port`` can change.
#: The chart always creates the ClusterRole that grants GET on the
#: ``/metrics`` nonResourceURL. The endpoint authenticates the bearer token
#: and authorizes it against that URL, so an anonymous scrape is refused.
OPERATOR_METRICS_PORT = "metrics"
OPERATOR_METRICS_READER_ROLE = "simplyblock-operator-metrics-reader"

#: The identity this suite scrapes as. Created on first use and left in
#: place: it holds no permission beyond reading ``/metrics``.
METRICS_SCRAPER_SA = "e2e-metrics-scraper"


def ensure_metrics_scraper(k8s, namespace):
    """Apply the scraper's ServiceAccount and its binding. Idempotent."""
    k8s.apply_yaml(
        f"apiVersion: v1\n"
        f"kind: ServiceAccount\n"
        f"metadata:\n"
        f"  name: {METRICS_SCRAPER_SA}\n"
        f"  namespace: {namespace}\n",
        namespace=namespace)
    k8s.apply_yaml_cluster_scoped(
        f"apiVersion: rbac.authorization.k8s.io/v1\n"
        f"kind: ClusterRoleBinding\n"
        f"metadata:\n"
        f"  name: {METRICS_SCRAPER_SA}-{namespace}\n"
        f"roleRef:\n"
        f"  apiGroup: rbac.authorization.k8s.io\n"
        f"  kind: ClusterRole\n"
        f"  name: {OPERATOR_METRICS_READER_ROLE}\n"
        f"subjects:\n"
        f"- kind: ServiceAccount\n"
        f"  name: {METRICS_SCRAPER_SA}\n"
        f"  namespace: {namespace}\n")


def scrape_operator_metrics(k8s, namespace=None):
    """Raw Prometheus text from the operator's ``/metrics``.

    Fetched from the test host through a port-forward to the operator pod's
    :data:`OPERATOR_METRICS_PORT`, with a bearer token for
    :data:`METRICS_SCRAPER_SA`. Not from inside the operator pod: its image
    is ubi-minimal and has no ``curl``, and the endpoint wants a token that
    pod's own identity is not authorized to present.

    Raises :class:`S3ProbeError` when the scrape itself fails, naming the
    HTTP status or the transport error, and when there is not exactly one
    live operator pod: the samples are that pod's own, so picking one of
    several would read an arbitrary replica. An empty result is never returned,
    so a caller can tell "the scrape failed" from "the metric family has no
    samples".
    """
    ns = namespace or k8s.namespace
    if not k8s.use_local_kubectl:
        raise S3ProbeError(
            "operator metrics scrape needs a local kubectl (K8S_LOCAL_KUBECTL=1) "
            "to port-forward from the test host")
    target = f"pod/{_operator_pod(k8s, ns)}"
    ensure_metrics_scraper(k8s, ns)
    token, err = k8s._exec_kubectl(
        f"kubectl create token {METRICS_SCRAPER_SA} -n {ns} --duration=10m",
        supress_logs=True)
    token = (token or "").strip()
    if not token:
        raise S3ProbeError(
            f"operator metrics scrape failed: no token for "
            f"{ns}/{METRICS_SCRAPER_SA}: {(err or '').strip()[:300]}")

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", ns,
         target, f"{port}:{OPERATOR_METRICS_PORT}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        _wait_listening(proc, port, target, timeout=15)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(
            f"https://127.0.0.1:{port}/metrics",
            headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
                text = resp.read().decode(errors="replace")
        except urllib.error.HTTPError as exc:
            raise S3ProbeError(
                f"operator metrics scrape failed: HTTP {exc.code} {exc.reason} "
                f"from {target}: "
                f"{exc.read()[:300]!r}") from exc
        except (urllib.error.URLError, OSError) as exc:
            raise S3ProbeError(
                f"operator metrics scrape failed: "
                f"{getattr(exc, 'reason', exc)} from {target}") from exc
    finally:
        proc.kill()
        proc.wait(timeout=10)
    if not text.strip():
        raise S3ProbeError(
            f"operator metrics scrape failed: {target} "
            f"answered 200 with an empty body")
    return text


def _operator_pod(k8s, namespace):
    """Name of the one operator pod that is running and not being deleted."""
    out, err = k8s._exec_kubectl(
        f"kubectl get pods -n {namespace} -l {OPERATOR_SELECTOR} -o json",
        supress_logs=True)
    try:
        items = json.loads(out or "")["items"]
    except (ValueError, KeyError) as exc:
        raise S3ProbeError(
            f"operator metrics scrape failed: cannot list {OPERATOR_SELECTOR} "
            f"pods in {namespace}: {(err or out or '').strip()[:300]}") from exc
    live = [p["metadata"]["name"] for p in items
            if p.get("status", {}).get("phase") == "Running"
            and not p["metadata"].get("deletionTimestamp")]
    if len(live) != 1:
        raise S3ProbeError(
            f"operator metrics scrape failed: expected one running "
            f"{OPERATOR_SELECTOR} pod in {namespace}, found {len(live)}: {live}")
    return live[0]


def _wait_listening(proc, port, target, timeout):
    """Return once *port* accepts a connection; raise if *proc* dies or times out."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise S3ProbeError(
                f"operator metrics scrape failed: port-forward to "
                f"{target} exited: "
                f"{(proc.stderr.read() or '').strip()[:300]}")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.25)
    raise S3ProbeError(
        f"operator metrics scrape failed: port-forward to "
        f"{target} was not listening within {timeout}s")


def metric_samples(text, name):
    """``[(labels_dict, value)]`` for one metric family out of scrape text."""
    rows = []
    for line in (text or "").splitlines():
        if not line.startswith(name):
            continue
        rest = line[len(name):]
        if rest[:1] not in ("{", " "):
            continue            # a different family sharing this prefix
        labels = {}
        if rest.startswith("{"):
            end = rest.find("}")
            for pair in rest[1:end].split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    labels[k.strip()] = v.strip().strip('"')
            rest = rest[end + 1:]
        try:
            rows.append((labels, float(rest.split()[0])))
        except (IndexError, ValueError):
            continue
    return rows


def metric_total(text, name, **match):
    """Sum of one family's samples, optionally filtered by label."""
    total = 0.0
    for labels, value in metric_samples(text, name):
        if all(labels.get(k) == v for k, v in match.items()):
            total += value
    return total


def histogram_quantiles(text, name, **match):
    """p50/p95/max estimated from a Prometheus histogram's buckets.

    An estimate off bucket boundaries, not an exact quantile -- which is the
    honest thing to report, because the histogram is all the operator exposes.
    Open Question 4 asks for the distribution's shape, and bucket boundaries
    answer that.
    """
    buckets = []
    for labels, value in metric_samples(text, name + "_bucket"):
        if all(labels.get(k) == v for k, v in match.items()):
            le = labels.get("le")
            if le:
                buckets.append((float("inf") if le == "+Inf" else float(le), value))
    if not buckets:
        return {}
    buckets.sort()
    total = buckets[-1][1]
    if not total:
        return {"count": 0}
    out = {"count": total}
    for q in (0.5, 0.95):
        target = q * total
        out[f"p{int(q * 100)}"] = next(
            (le for le, c in buckets if c >= target), buckets[-1][0])
    finite = [le for le, c in buckets if c > 0 and le != float("inf")]
    out["max_bucket"] = finite[-1] if finite else float("inf")
    out["sum"] = metric_total(text, name + "_sum", **match)
    return out


# ── OpenBao ──────────────────────────────────────────────────────────────
#
# The gateway's IAM is versitygw's Vault backend, and on these labs that is a
# single-replica OpenBao with pebbledb. Dev's own deploy notes say it outright:
# "OpenBao re-seals whenever its pod restarts, and only a rerun of this script
# unseals it." In a lane that restarts things on purpose, that turns every
# OpenBao restart into an S3 failure with no product cause -- the run reports a
# 5xx and the finding is the harness's.
#
# So the stress lane runs an unsealer. It is deliberately not a fixture that
# hides the event: every unseal is logged with its timestamp, so a case that
# wants to assert on the sealed window (TC-S3-006) can still see it, and a case
# that does not is simply not derailed by it.

class BaoUnsealer(threading.Thread):
    """Unseals OpenBao whenever it comes back sealed.

    Decided this way rather than by pinning OpenBao outside the blast radius.
    Pinning only works while the exclusion holds -- one new outage type, one
    scheduler decision, and the exclusion is silently not in effect, which is
    the failure mode that is hardest to notice. Unsealing works wherever the
    pod lands.
    """

    def __init__(self, k8s, namespace="vault", pod="openbao-0",
                 secret="openbao-init", interval=15):
        super().__init__(daemon=True, name="bao-unsealer")
        self.k8s = k8s
        self.namespace = namespace
        self.pod = pod
        self.secret = secret
        self.interval = interval
        self._stop = threading.Event()
        #: ``[(iso_time, event)]`` -- every seal observed and every unseal
        #: performed, so a case can report the window rather than just the fact.
        self.events = []
        self.unseal_count = 0
        self.logger = logger

    # -- state --------------------------------------------------------

    def sealed(self):
        out = self.k8s.exec_in_pod_container(
            self.pod, "", "bao status -format=json 2>/dev/null || "
                          "vault status -format=json 2>/dev/null || true",
            namespace=self.namespace, timeout=30)
        for line in (out or "").splitlines():
            try:
                return bool(json.loads(line).get("sealed"))
            except Exception:
                continue
        return None             # unknown: pod not up, or no CLI in it

    def unseal_keys(self):
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get secret {self.secret} -n {self.namespace} -o json "
            f"2>/dev/null || true", supress_logs=True)
        try:
            data = json.loads((out or "{}").strip() or "{}").get("data") or {}
        except Exception:
            return []
        import base64
        keys = []
        for k, v in sorted(data.items()):
            if "unseal" in k.lower() or "key" in k.lower():
                try:
                    text = base64.b64decode(v).decode().strip()
                except Exception:
                    continue
                # The init Secret has been seen holding either the raw key or
                # the whole `bao operator init` JSON. Take both shapes.
                try:
                    blob = json.loads(text)
                    keys.extend(blob.get("unseal_keys_b64")
                                or blob.get("keys_base64") or blob.get("keys") or [])
                except Exception:
                    keys.append(text)
        return [k for k in keys if k]

    def unseal(self):
        keys = self.unseal_keys()
        if not keys:
            self.logger.warning(
                "[bao] sealed, but %s/%s holds no unseal key -- cannot unseal",
                self.namespace, self.secret)
            return False
        for key in keys[:5]:
            self.k8s.exec_in_pod_container(
                self.pod, "",
                f"bao operator unseal {shlex.quote(key)} >/dev/null 2>&1 || "
                f"vault operator unseal {shlex.quote(key)} >/dev/null 2>&1 || true",
                namespace=self.namespace, timeout=45)
            if self.sealed() is False:
                self.unseal_count += 1
                self.events.append((time.strftime("%Y-%m-%d %H:%M:%S"), "unsealed"))
                self.logger.info("[bao] unsealed (%d so far)", self.unseal_count)
                return True
        self.logger.warning("[bao] still sealed after trying %d key(s)", len(keys[:5]))
        return False

    # -- thread -------------------------------------------------------

    def run(self):
        while not self._stop.wait(self.interval):
            try:
                state = self.sealed()
                if state:
                    self.events.append(
                        (time.strftime("%Y-%m-%d %H:%M:%S"), "sealed"))
                    self.logger.warning("[bao] observed sealed -- unsealing")
                    self.unseal()
            except Exception as exc:               # noqa: BLE001
                # A watcher that can kill the run it is protecting is worse
                # than no watcher.
                self.logger.warning("[bao] unsealer pass failed: %s", exc)

    def stop(self):
        self._stop.set()


# ── Small shared assertions ──────────────────────────────────────────────

def assert_roundtrip(pod, cfg, bucket, key, size, seed, context=""):
    """Write *size* bytes, read them back, and compare byte for byte.

    The comparison happens in the pod. A mismatch comes back with the offset
    and both byte runs, because "the read did not match" on its own cannot be
    told from a short read and the two have different causes.
    """
    put = pod.call(cfg, "put_object", bucket=bucket, key=key, size=size, seed=seed)
    if put.get("status") not in (200, 201):
        raise S3EvidenceError(
            f"PutObject {bucket}/{key} ({size} B) failed {context}: "
            f"status={put.get('status')} code={put.get('code')!r} "
            f"body={put.get('body', '')[:400]!r}")
    got = pod.call(cfg, "get_object", bucket=bucket, key=key, size=size, seed=seed)
    if got.get("status") != 200:
        raise S3EvidenceError(
            f"GetObject {bucket}/{key} failed {context}: "
            f"status={got.get('status')} code={got.get('code')!r}")
    if not got.get("match"):
        raise S3EvidenceError(
            f"DATA MISMATCH on {bucket}/{key} {context}: read "
            f"{got.get('length')} B of {size}, first difference at "
            f"{got.get('first_diff')} (got {got.get('got_at_diff')!r}, "
            f"want {got.get('want_at_diff')!r})")
    return put, got


def pvc_of_bucket(k8s, name, namespace=None):
    return ((s3bucket(k8s, name, namespace).get("status") or {})
            .get("persistentVolumeClaimName", ""))


def export_of_bucket(k8s, name, namespace=None):
    return ((s3bucket(k8s, name, namespace).get("status") or {})
            .get("nfsExportName", ""))
