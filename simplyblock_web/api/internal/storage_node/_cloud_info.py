import requests


def get_cloud_info() -> dict | None:
    return next((
        info
        for getter
        in [_google_info, _amazon_info, _equinix_info]
        if (info := getter()) is not None
    ), None)


def _google_info() -> dict | None:
    try:
        headers = {'Metadata-Flavor': 'Google'}
        response = requests.get("http://169.254.169.254/computeMetadata/v1/instance/?recursive=true", headers=headers, timeout=2)
        data = response.json()
        return {
            "id": str(data["id"]),
            "type": data["machineType"].split("/")[-1],
            "cloud": "google",
            "ip": data["networkInterfaces"][0]["ip"],
            "public_ip": data["networkInterfaces"][0]["accessConfigs"][0]["externalIp"],
        }
    except Exception:
        return None


def _amazon_info() -> dict | None:
    try:
        import ec2_metadata
        session = requests.session()
        data = ec2_metadata.EC2Metadata(session=session).instance_identity_document  # type: ignore[call-arg]
        return {
            "id": data["instanceId"],
            "type": data["instanceType"],
            "cloud": "amazon",
            "ip": data["privateIp"],
            "public_ip": "",
        }
    except Exception:
        return None


def _equinix_info(timeout: int = 2) -> dict | None:
    try:
        response = requests.get("https://metadata.platformequinix.com/metadata", timeout=2)
        data = response.json()
        public_ip = ""
        ip = ""
        for interface in data["network"]["addresses"]:
            if interface["address_family"] == 4:
                if interface["enabled"] and interface["public"]:
                    public_ip = interface["address"]
                elif interface["enabled"] and not interface["public"]:
                    public_ip = interface["address"]
        return {
            "id": str(data["id"]),
            "type": data["class"],
            "cloud": "equinix",
            "ip": ip,
            "public_ip": public_ip
        }
    except Exception:
        return None
