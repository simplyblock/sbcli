"""Run a vendor fake as an HTTPS mock server.

    python -m tests.unit.storage_arrays.fakes --vendor pure-flasharray --port 8443

Prints the accepted credential, the CA certificate path (pass its content as
the array's ``ca_bundle``) and the options to register the array with.

With ``--lio`` (root, LIO modules and python3-rtslib-fb on the host) the
fake's volumes are real iSCSI LUNs of the kernel target, presented to the
hosts the fake maps them to (see ``fakes.lio``)::

    python -m tests.unit.storage_arrays.fakes --vendor pure-flasharray \
        --host 0.0.0.0 --lio --lio-portal 10.0.0.5:3260 \
        --lio-allow iqn.1994-05.com.redhat:node1,iqn.1994-05.com.redhat:node2
"""
import argparse
import importlib
import json

from simplyblock_core.storage_arrays.registry import ADAPTERS, canonical_vendor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor", required=True, help=", ".join(sorted(ADAPTERS)))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8443)
    parser.add_argument("--lio", action="store_true",
                        help="present the volumes as iSCSI LUNs of the kernel target")
    parser.add_argument("--lio-target", default="iqn.2026-11.io.simplyblock:mock-array",
                        help="IQN of the iSCSI target")
    parser.add_argument("--lio-portal", default="0.0.0.0:3260", help="portal address:port")
    parser.add_argument("--lio-dir", default="/var/lib/sb-mock-lio", help="where the LUN files live")
    parser.add_argument("--lio-allow", default="",
                        help="comma-separated initiators allowed to log in before they are registered")
    args = parser.parse_args()

    vendor = canonical_vendor(args.vendor)
    module = importlib.import_module(
        f"{__package__}.{ADAPTERS[vendor].split(':')[0].split('.')[-1]}")
    setup = module.contract_setup()
    mirror = None
    if args.lio:
        from . import FakeTransport
        from .lio import ConfigfsLio, LioMirror
        ip, _, port = args.lio_portal.rpartition(":")
        backend = ConfigfsLio(args.lio_target, args.lio_dir, (ip or "0.0.0.0", int(port)))
        mirror = LioMirror(setup.fake, setup.config, setup.credential, backend,
                           transport=FakeTransport(setup.fake),
                           allow=[i for i in args.lio_allow.split(",") if i])
        mirror.reconcile()
    server = serve_fake(setup.fake, args.host, args.port,
                        mirror.reconcile if mirror is not None else None)
    print(json.dumps({
        "vendor": vendor,
        "endpoint": f"https://{args.host}:{args.port}",
        "ca_bundle_file": server.certfile,
        "credential_fields": sorted(setup.credential.field_names),
        "credential": {k: setup.credential.get(k).get_secret_value()
                       for k in setup.credential.field_names},
        "options": setup.config.options,
        "iscsi_target": args.lio_target if args.lio else None,
        "iscsi_portal": args.lio_portal if args.lio else None,
    }, indent=2), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


def serve_fake(fake, host, port, after_request=None):
    from . import serve
    return serve(fake, host, port, after_request=after_request)


if __name__ == "__main__":
    main()
