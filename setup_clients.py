"""
Quick-start script: derives LFDIs from the envoy demo certificates and writes
all client entries into the local .cactus.yaml.

Usage:
    python setup_clients.py /path/to/envoy/demo/tls-termination/test_certs

Run from the directory containing your .cactus.yaml file.
"""

import re
import sys
from dataclasses import replace
from pathlib import Path

from cactus_client.model.config import ClientConfig, load_config
from cactus_client.sep2 import convert_lfdi_to_sfdi, lfdi_from_cert_file
from cactus_test_definitions.server.test_procedures import ClientType

NMI_VALIDATION_VARS = {
    "NMI_VALIDATION_ENABLED": "true",
    "NMI_VALIDATION_PARTICIPANT_ID": "ENERGYAP",
}


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

    env_path = Path(".env")
    env_text = env_path.read_text() if env_path.exists() else ""
    additions = []
    for key, value in NMI_VALIDATION_VARS.items():
        if not re.search(rf"^{key}\s*=", env_text, re.MULTILINE):
            additions.append(f"{key}={value}")
    if additions:
        separator = "\n" if env_text and not env_text.endswith("\n") else ""
        env_path.write_text(env_text + separator + "\n".join(additions) + "\n")
        print(f"Added to {env_path}: {', '.join(additions)}")
    else:
        print(f"{env_path} already contains NMI validation settings")


if __name__ == "__main__":
    main()
