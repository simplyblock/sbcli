# External storage arrays

Provision, resize and delete volumes (LUNs and NVMe namespaces) on enterprise arrays, and map them to hosts.
There is no usable cross-vendor standard, so each array has its own adapter. SMI-S is deprecated, Swordfish has almost no array support, and CSI drivers depend on Kubernetes.

## Supported systems

| Key | Systems | API | Protocols |
|---|---|---|---|
| `pure-flasharray` | Pure FlashArray //X, //C, //XL, //E | REST 2.x, API token | iSCSI, FC, NVMe/TCP, /RoCE, /FC |
| `netapp-ontap` | NetApp AFF/FAS/ASA (not ASA r2), Lenovo DM | ONTAP REST | iSCSI, FC, NVMe/TCP, /FC (`protocol_family`) |
| `dell-powerstore` | Dell PowerStore | REST | iSCSI, FC, NVMe/TCP, /FC |
| `dell-powermax` | Dell PowerMax | Unisphere REST | iSCSI, FC, NVMe (port-group per protocol) |
| `dell-powerflex` | Dell PowerFlex 3.6 / 4.x | Gateway REST / PFxM OAuth | SDC |
| `hpe-wsapi` | HPE Alletra Storage MP B10000, Primera, 3PAR | WSAPI | iSCSI, FC |
| `hpe-nimble` | HPE Alletra 5000/6000, Nimble | NimbleOS REST | iSCSI, FC |
| `ibm-flashsystem` | IBM FlashSystem, Storage Virtualize/SVC | REST (port 7443) | iSCSI, FC, NVMe/TCP, /RDMA |
| `hitachi-vsp` | Hitachi VSP One Block, VSP E, VSP 5000 | Configuration Manager REST | iSCSI, FC |
| `huawei-dorado` | Huawei OceanStor Dorado | DeviceManager REST | iSCSI, FC |
| `infinidat-infinibox` | Infinidat InfiniBox | REST | iSCSI, FC |

Each adapter's module docstring lists the options it reads, the credential fields it needs, and a section called **"Verify against a real system"**.
That section names every API detail its author was unsure of. Work through it before the adapter is first used against a real array.

## Layers

- `base.py`: the adapter contract (`ArrayAdapter`) and the shared model.
- `client.py`: `ArrayClient`, the only entry point for the rest of simplyblock. It handles:
  - credentials through `simplyblock_core.credentials`, bound to the array id and endpoint, with re-authentication and one retry on auth failure;
  - `sb-<uuid hex>` volume names and rounding sizes to the array's granularity;
  - making create, map, unmap and delete safe to re-run;
  - safety checks before resize, map or delete: the `sb-` name prefix, the name derived from the recorded volume UUID, and the recorded WWN/NGUID;
  - refusing to shrink a volume.
- `http.py`: TLS (pinned CA bundle per array), timeouts, retries, error mapping.
- `registry.py`: maps vendor keys and aliases to adapters, loading each adapter on first use.

## Testing

1. **Contract suite against fakes** (`tests/unit/storage_arrays/test_contract.py`).
   `testing/<vendor>.py` is a stateful in-memory fake of the vendor's REST API: paths, JSON shapes, status codes, error bodies, async jobs, session tokens.
   Every adapter runs the same suite against its fake, through a `requests` transport, with no network.
   This shows the adapter's logic is right. It does **not** show the fake matches the real API; the fakes encode our reading of each vendor's API.
2. **Vendor unit tests** (`test_<vendor>.py`): unit conversions, identifier derivation, job polling and failures, error mapping, options.
3. **HTTPS mock server**, for end-to-end and UI work:
   ```
   python -m simplyblock_core.storage_arrays.testing --vendor netapp-ontap --port 8443
   ```
   It prints the endpoint, the CA file, the credential and the options to register.
4. **Live tier**: the same contract suite against a simulator or a real array:
   ```
   SB_ARRAY_LIVE_CONFIG=lab-array.json python -m pytest tests/unit/storage_arrays/test_contract.py --timeout-method=thread
   ```
   See `tests/unit/storage_arrays/conftest.py` for the file format. The live tier only creates and deletes `sb-` volumes and `sbtest-` hosts.

### Where to get something real to test against

These are partly from memory; check availability with each vendor.

- **NetApp:** ONTAP Select evaluation, and the ONTAP simulator (support-site login). Both have the full REST API. A Lab on Demand is also an option.
- **Pure:** Pure Test Drive labs. There is no downloadable FlashArray simulator.
- **Dell:** PowerFlex can be installed on 3 VMs. Dell demo labs cover PowerStore and PowerMax. PowerStore has a virtual edition for partners.
- **HPE:** the 3PAR StoreServ simulator (older WSAPI). Alletra through HPE demo and partner labs.
- **IBM:** Storage Virtualize for Public Cloud, plus IBM Technology Zone labs.
- **Hitachi, Huawei:** partner and demo labs only.
- **Infinidat:** `infinisim`, an open-source Python simulator of the InfiniBox API.

Vendor SDKs make good reference code for API details: `py-pure-client`, `netapp-ontap`, `PyPowerStore`, `PyU4V`, `PyPowerFlex`, `hpe3parclient`, `nimble-sdk`, `infinisdk`.
The same goes for each vendor's Ansible collection, CSI driver and OpenStack Cinder driver.
This package uses plain `requests` and does not depend on any of them.
