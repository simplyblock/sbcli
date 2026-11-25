"""The Graylog JSON extractor only runs on messages that are JSON documents.

EKS bed bf34 (2026-11-24): unconditioned, it ran on every SPDK and spdk-proxy
line and threw a NullPointerException on each, which kept Graylog 13 minutes
behind ten storage nodes.
"""
import json
import re
from types import SimpleNamespace

from pydantic import SecretStr

from simplyblock_core import cluster_ops


class _Session:
    def __init__(self):
        self.auth = None
        self.posts: list[tuple[str, dict]] = []

    def post(self, url, headers=None, data=None):
        self.posts.append((url, json.loads(data)))
        return SimpleNamespace(status_code=201)

    def get(self, url, headers=None):
        return SimpleNamespace(status_code=200, json=lambda: {
            "inputs": [{"title": "spdk log input", "id": "in1"}]})


def test_the_extractor_is_conditioned_on_a_json_message(monkeypatch):
    s = _Session()
    monkeypatch.setattr(cluster_ops.requests, "session", lambda: s)
    assert cluster_ops._add_graylog_input("http://gl:9000", SecretStr("pw")) is True
    (url, payload), = [p for p in s.posts if p[0].endswith("/extractors")]
    assert url == "http://gl:9000/api/system/inputs/in1/extractors"
    assert payload["extractor_type"] == "json"
    assert payload["condition_type"] == "regex"
    assert payload["condition_value"] == cluster_ops.JSON_MESSAGE_CONDITION


def test_the_condition_matches_json_and_not_plain_lines():
    cond = re.compile(cluster_ops.JSON_MESSAGE_CONDITION)
    assert cond.search('{"cluster_id": "c1", "event": "OBJ_CREATED"}')
    assert cond.search('  {"a": 1}')
    for line in (
        "[2026-11-24 08:28:54.840] blobstore.c:17842:bs_update_blob_set_mds: *NOTICE*: Finished",
        "2026-11-24 16:02:04,133: INFO: 192.168.3.49 - - \"GET /snode/check HTTP/1.1\" 200 -",
        "Reviewed-on: https://review.spdk.io/gerrit/c/spdk/spdk/+/25402",
        "",
    ):
        assert not cond.search(line), line
