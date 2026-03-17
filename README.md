# cactus-client-envoy

An [apluggy](https://github.com/simonsobs/apluggy) plugin for [cactus-client](../cactus-client) that implements admin instructions against a local [Envoy](../envoy) server via direct database access.

When `cactus-client` encounters an `admin_instruction` step (e.g. `ensure-end-device`), it calls registered plugins via the `cactus_client.admin` hook system. This package provides the Envoy-backed implementation of those hooks.

## How the plugin works

The plugin connects directly to Envoy's PostgreSQL database. It manages its own DB connection using credentials from environment variables.

| Hook | Behaviour |
|---|---|
| `admin_setup` | Opens a SQLAlchemy async engine to the Envoy DB |
| `admin_teardown` | Disposes the engine — always runs, incl on test failure |
| `admin_instruction` | Handles each instruction in its own DB session with explicit commit |

## Full Setup

Everything below assumes all three repos are cloned into the same parent directory and **all commands are run from that parent directory** unless noted otherwise. All commands are copy-paste ready.

```
workspace/               ← run all commands from here
  envoy/
  cactus-client/
  cactus-client-envoy/
  cactus-test/           ← created in step 4 (test outputs)
```

### Prerequisites

- Python 3.12+
- [conda](https://docs.conda.io/) (recommended) or pip
- [Docker](https://docs.docker.com/get-docker/) with Compose v2
- git

### 1 — Clone the repos

```bash
git clone https://github.com/bsgip/envoy.git
git clone https://github.com/bsgip/cactus-client.git
git clone https://github.com/bsgip/cactus-client-envoy.git
```

### 2 — Create a Python environment and install packages

```bash
conda create -n cactus python=3.12 -y
conda activate cactus

pip install -e ./cactus-client
pip install -e ./cactus-client-envoy
```

Once `cactus-client-envoy` is installed, `cactus-client` will automatically discover and load it via the `cactus_client.admin` setuptools entrypoint — no code changes required.

### 3 — Build the envoy Docker image and start the demo

```bash
cd envoy/demo
docker build --no-cache -t envoy:latest -f ../Dockerfile.server ../
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d
cd ../..
```

> **Note:** If you see encrypted key errors, your `test_certs/` directory has stale certs from an older envoy version. Fix with:
> ```bash
> rm -rf envoy/demo/tls-termination/test_certs/*
> docker compose -f envoy/demo/docker-compose.yaml down -v
> cd envoy/demo && docker build --no-cache -t envoy:latest -f ../Dockerfile.server ../ && cd ../..
> HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose -f envoy/demo/docker-compose.yaml up -d
> ```

### 4 — Initialise cactus-client config

```bash
cactus setup --local ./cactus-test
cactus server dcap https://localhost:8443/dcap
cactus server verify true
cactus server serca ./envoy/demo/tls-termination/test_certs/testca.crt
```

Then run `setup_clients.py` to register all three demo clients. It derives LFDIs directly from the certificates:

```bash
python ./cactus-client-envoy/setup_clients.py ./envoy/demo/tls-termination/test_certs
```

This registers:

| ID | Cert | Type |
|---|---|---|
| `device1` | `testdevice1.crt` | device |
| `device2` | `testdevice2.crt` | device |
| `aggregator1` | `testaggregator.crt` | aggregator |

### 5 — Set environment variables

```bash
export ENVOY_DB_DSN=postgresql+asyncpg://test_user:test_pwd@localhost:8003/test_db
export DATABASE_URL=postgresql+asyncpg://test_user:test_pwd@localhost:8003/test_db
```

Add these to your shell profile (`~/.bashrc` or `~/.zshrc`) to avoid setting them every session.

### 6 — Run a test

```bash
cactus tests                           # list all available test procedure IDs
cactus run S_ALL_01 device1            # run a test with a single device client
cactus run S_ALL_05 device1 device2    # run a test requiring two clients
```

Test reports are written to `./cactus-test/`.

---

## Environment variables

| Variable | Description | Example |
|---|---|---|
| `ENVOY_DB_DSN` | SQLAlchemy async DSN for the Envoy PostgreSQL database | `postgresql+asyncpg://user:pass@localhost:8003/envoy_db` |
| `DATABASE_URL` | Required by Envoy's model layer at import time — use the same value | `postgresql+asyncpg://user:pass@localhost:8003/envoy_db` |

## Supported admin instructions

### `ensure-end-device`

Ensures a virtual DER client is registered (or not) as a site in the Envoy database.

```yaml
admin_instructions:
  - type: ensure-end-device
    parameters:
      registered: true   # true = insert the site, false = delete it
```

The optional `client` parameter selects a specific client alias; if omitted, the first configured client is used.

Sites are created with `aggregator_id=0` (null aggregator), `timezone_id=UTC`, `device_category=PHOTOVOLTAIC_SYSTEM`, and `post_rate_seconds=60`. The `registration_pin`, `lfdi`, and `sfdi` are taken from the client's `.cactus.yaml` config.

---

## Writing your own admin plugin

Any Python package can provide admin hooks for `cactus-client` by following this pattern.

### 1 — Depend on `cactus_client`

### 2 — Implement the hooks

Use `server_config.device_capability_uri` to identify which server is under test, and read credentials from environment variables.

### 3 — Register via setuptools entrypoint

```toml
# pyproject.toml
[project.entry-points."cactus_client.admin"]
my-plugin = "my_package.plugin:MyServerPlugin"
```

### 4 — Install alongside cactus-client

---

## Development

```bash
pip install -e .[dev,test]
pytest tests/
```
