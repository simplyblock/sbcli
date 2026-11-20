"""Run a vendor fake as an HTTPS mock server.

    python -m simplyblock_core.storage_arrays.testing --vendor pure-flasharray --port 8443

Prints the accepted credential, the CA certificate path (pass its content as
the array's ``ca_bundle``) and the options to register the array with.
"""
import argparse
import importlib
import json

from ..registry import ADAPTERS, canonical_vendor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor", required=True, help=", ".join(sorted(ADAPTERS)))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8443)
    args = parser.parse_args()

    vendor = canonical_vendor(args.vendor)
    module = importlib.import_module(
        f"{__package__}.{ADAPTERS[vendor].split(':')[0].split('.')[-1]}")
    setup = module.contract_setup()
    server = serve_fake(setup.fake, args.host, args.port)
    print(json.dumps({
        "vendor": vendor,
        "endpoint": f"https://{args.host}:{args.port}",
        "ca_bundle_file": server.certfile,
        "credential_fields": sorted(setup.credential.field_names),
        "credential": {k: setup.credential.get(k).get_secret_value()
                       for k in setup.credential.field_names},
        "options": setup.config.options,
    }, indent=2))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


def serve_fake(fake, host, port):
    from . import serve
    return serve(fake, host, port)


if __name__ == "__main__":
    main()
