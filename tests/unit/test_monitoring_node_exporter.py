"""Guards on the node-exporter service in the swarm monitoring stack.

`--path.rootfs` has to name the container path the host root is bind-mounted
at. When the two disagree, node-exporter still reports the filesystem
collector as successful, but every mountpoint it cannot `statfs()` under that
path is dropped silently — which is how `node_filesystem_avail_bytes` came to
cover only `/`, and only because /var/lib/docker happened to share the root
filesystem. The Root Filesystem Low Space Alert reads those series.
"""

import unittest
from pathlib import Path

import yaml


COMPOSE_FILE = (
    Path(__file__).resolve().parents[2]
    / "simplyblock_core" / "scripts" / "docker-compose-swarm-monitoring.yml"
)
ALERT_RULES_FILE = (
    Path(__file__).resolve().parents[2]
    / "simplyblock_core" / "scripts" / "alerting" / "alert_rules.yaml"
)


def _node_exporter() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text())["services"]["node-exporter"]


def _flag(args: list, name: str) -> str:
    prefix = f"--{name}="
    values = [a[len(prefix):] for a in args if a.startswith(prefix)]
    if len(values) != 1:
        raise AssertionError(f"expected exactly one --{name}, got {values}")
    return values[0]


class TestNodeExporterRootfs(unittest.TestCase):

    def test_path_rootfs_matches_the_host_root_mount_target(self):
        service = _node_exporter()
        targets = [
            v.split(":")[1] for v in service["volumes"]
            if v.split(":")[0] == "/"
        ]
        self.assertEqual(len(targets), 1, "host root should be mounted exactly once")
        self.assertEqual(
            _flag(service["command"], "path.rootfs"),
            targets[0],
            "--path.rootfs must name the container path / is mounted at, or "
            "node-exporter silently drops filesystem series",
        )

    def test_filesystem_excludes_are_left_at_the_upstream_defaults(self):
        # The previous overrides carried literal quote characters, so they
        # matched no mountpoint at all, and the fs-types override dropped
        # `overlay` from the exclude list -- which admits one series set per
        # running container once the rootfs path is correct.
        args = _node_exporter()["command"]
        for flag in (
            "collector.filesystem.mount-points-exclude",
            "collector.filesystem.fs-types-exclude",
            "collector.filesystem.ignored-mount-points",
            "collector.filesystem.ignored-fs-types",
        ):
            for arg in args:
                self.assertFalse(
                    arg.startswith(f"--{flag}"),
                    f"{flag} overrides node-exporter's maintained default list",
                )

    def test_no_flag_value_carries_literal_quotes(self):
        for arg in _node_exporter()["command"]:
            self.assertNotIn(
                '"', arg,
                "quotes in a compose exec-form arg become part of the value",
            )

    def test_root_filesystem_alert_reads_a_metric_node_exporter_exports(self):
        # Ties the alert to the exporter it depends on: the rule is useless if
        # the node-exporter service stops producing these families.
        rules = yaml.safe_load(ALERT_RULES_FILE.read_text())
        titles = [
            rule["title"]
            for group in rules["groups"] for rule in group["rules"]
        ]
        self.assertIn("Root Filesystem Low Space Alert", titles)

        expressions = " ".join(
            query.get("model", {}).get("expr", "")
            for group in rules["groups"] for rule in group["rules"]
            if rule["title"] == "Root Filesystem Low Space Alert"
            for query in rule["data"]
        )
        self.assertIn("node_filesystem_avail_bytes", expressions)
        self.assertIn("node_filesystem_size_bytes", expressions)
        self.assertIn('mountpoint="/"', expressions)


if __name__ == "__main__":
    unittest.main()
