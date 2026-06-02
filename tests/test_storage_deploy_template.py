import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "simplyblock_web" / "templates"


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

    def test_cert_manager_mounts_secret_directly(self):
        rendered = _render_storage_deploy("cert-manager")
        self.assertIn("secretName: simplyblock-spdk-proxy-tls", rendered)
        self.assertNotIn("simplyblock-certificate-authority", rendered)
        self.assertNotIn("projected:", rendered)
        self.assertIn('name: SB_TLS_PROVIDER', rendered)
        self.assertIn('value: "cert-manager"', rendered)
