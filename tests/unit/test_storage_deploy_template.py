import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"


def _render_storage_deploy(tls_provider: str) -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("storage_deploy_spdk.yaml.j2")
    return template.render(
        SPDK_IMAGE="spdk:test",
        L_CORES="0-1",
        SPDK_MEM=1024,
        CORES=2,
        SERVER_IP="10.0.0.10",
        RPC_PORT=8080,
        RPC_USERNAME="admin",
        RPC_PASSWORD="secret",
        HOSTNAME="node-a",
        NAMESPACE="simplyblock",
        SIMPLYBLOCK_DOCKER_IMAGE="proxy:test",
        GRAYLOG_SERVER_IP="10.0.0.20",
        MODE="kubernetes",
        CLUSTER_ID="cluster1",
        SSD_PCIE="none",
        PCI_ALLOWED="",
        TOTAL_HP="",
        NSOCKET=0,
        FW_PORT=50001,
        CPU_TOPOLOGY_ENABLED=False,
        MEM_MEGA=1536,
        MEM2_MEGA=1024,
        TLS_ENABLED=True,
        TLS_PROVIDER=tls_provider,
    )


class TestStorageDeployTemplate(unittest.TestCase):

    def test_openshift_uses_service_ca_key(self):
        rendered = _render_storage_deploy("openshift")
        self.assertIn("key: service-ca.crt", rendered)
        self.assertIn('name: SB_TLS_PROVIDER', rendered)
        self.assertIn('value: "openshift"', rendered)

    def test_containers_that_sudo_run_as_root(self):
        """Regression: 2026-09-29, k3s on Ubuntu 24.04. The SPDK and proxy
        images run as the non-root user simplyblock, and both containers
        start through sudo, which fails its PAM account check in the
        container ("Authentication service cannot retrieve authentication
        info"): the SPDK pod exited at once, and node add failed with
        connection refused on the RPC port. In the SPDK image sudo fails
        even as root, so the containers run as root without sudo."""
        import yaml
        docs = [d for d in yaml.safe_load_all(_render_storage_deploy("cert-manager")) if d]
        pod = next(d for d in docs if d.get("kind") == "Pod")
        names = {c["name"] for c in pod["spec"]["containers"]
                 if c.get("securityContext", {}).get("runAsUser") == 0}
        self.assertEqual(names, {"spdk-container", "spdk-proxy-container"})
        # Even as root, sudo fails in the SPDK image: nothing may use it.
        for c in pod["spec"]["containers"]:
            if c["name"] not in names:
                continue
            hook = c.get("lifecycle", {}).get("postStart", {}).get("exec", {}).get("command", [])
            self.assertNotIn("sudo", " ".join(c.get("command", []) + hook), c["name"])

    def test_cert_manager_mounts_secret_directly(self):
        rendered = _render_storage_deploy("cert-manager")
        self.assertIn("secretName: simplyblock-spdk-proxy-tls", rendered)
        self.assertNotIn("simplyblock-certificate-authority", rendered)
        self.assertNotIn("projected:", rendered)
        self.assertIn('name: SB_TLS_PROVIDER', rendered)
        self.assertIn('value: "cert-manager"', rendered)
