"""
Quick-start script: derives LFDIs from the envoy demo certificates and writes
all client entries into the local .cactus.yaml.

Usage:
    python setup_clients.py /path/to/envoy/demo/tls-termination/test_certs

Run from the directory containing your .cactus.yaml file.
"""

import sys
from dataclasses import replace
from pathlib import Path

from cactus_client.model.config import ClientConfig, load_config
from cactus_client.sep2 import convert_lfdi_to_sfdi, lfdi_from_cert_file
from cactus_test_definitions.server.test_procedures import ClientType


def make_client(
    client_id: str,
    cert_name: str,
    client_type: ClientType,
    certs_dir: Path,
    pin: int = 12345,
    pen: int = 0,
    max_watts: int = 5000,
) -> ClientConfig:
    cert = certs_dir / f"{cert_name}.crt"
    key = certs_dir / f"{cert_name}.key"
    lfdi = lfdi_from_cert_file(str(cert))
    sfdi = convert_lfdi_to_sfdi(lfdi)
    return ClientConfig(
        id=client_id,
        type=client_type,
        certificate_file=str(cert),
        key_file=str(key),
        lfdi=lfdi,
        sfdi=sfdi,
        pen=pen,
        pin=pin,
        max_watts=max_watts,
    )


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} /path/to/envoy/demo/tls-termination/test_certs")
        sys.exit(1)

    certs_dir = Path(sys.argv[1])
    if not certs_dir.is_dir():
        print(f"Error: {certs_dir} is not a directory")
        sys.exit(1)

    clients = [
        make_client("device1", "testdevice1", ClientType.DEVICE, certs_dir),
        make_client("device2", "testdevice2", ClientType.DEVICE, certs_dir),
        make_client("aggregator1", "testaggregator", ClientType.AGGREGATOR, certs_dir),
        make_client("aggregator2", "testaggregator2", ClientType.AGGREGATOR, certs_dir),
    ]

    cfg, cfg_path = load_config(None)
    cfg = replace(cfg, clients=clients)
    cfg.to_yaml_file(cfg_path)

    print(f"Written {len(clients)} clients to {cfg_path}:")
    for c in clients:
        print(f"  {c.id} ({c.type}): lfdi={c.lfdi}  sfdi={c.sfdi}")


if __name__ == "__main__":
    main()
