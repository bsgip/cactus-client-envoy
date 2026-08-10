# cactus-client-envoy

An [apluggy](https://github.com/simonsobs/apluggy) plugin for [cactus-client](../cactus-client) that implements admin instructions against a local [Envoy](../envoy) server via direct database access.

When `cactus-client` encounters an `admin_instruction` step (e.g. `ensure-end-device`), it calls registered plugins via the `cactus_client.admin` hook system. This package provides the Envoy-backed implementation of those hooks.

## How the plugin works

The plugin connects directly to Envoy's PostgreSQL database. It manages its own DB connection using credentials from a `.env` file in the `cactus-client-envoy/` directory (loaded automatically at import time via `python-dotenv`).

| Hook | Behaviour |
|---|---|
| `admin_setup` | Opens a SQLAlchemy async engine to the Envoy DB |
| `admin_teardown` | Disposes the engine — always runs, incl on test failure |
| `admin_instruction` | Handles each instruction in its own DB session with explicit commit |

## Full Setup

Everything below assumes all three repos are cloned into the same parent directory. This parent directory is referred to as the **workspace root** — most commands are run from there.

```
workspace/               ← run most commands from here
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

Run from the workspace root:

```bash
git clone https://github.com/bsgip/envoy.git
git clone https://github.com/bsgip/cactus-client.git
git clone https://github.com/bsgip/cactus-client-envoy.git
```

### 2 — Create a Python environment and install packages

Run from the workspace root:

```bash
conda create -n cactus python=3.12 -y
conda activate cactus

pip install -e ./cactus-client
pip install -e ./cactus-client-envoy
```

Once `cactus-client-envoy` is installed, `cactus-client` will automatically discover and load it via the `cactus_client.admin` setuptools entrypoint — no code changes required.

### 3 — Build the envoy Docker image and start the demo

Run from **`envoy/demo/`**:

```bash
cd envoy/demo
docker build --no-cache -t envoy:latest -f ../Dockerfile.server ../
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d
cd ../..

> **Note:** If you see encrypted key errors, your `test_certs/` directory has stale certs from an older envoy version. Fix with:
> ```bash
> rm -rf envoy/demo/tls-termination/test_certs/*
> docker compose -f envoy/demo/docker-compose.yaml down -v
> cd envoy/demo && docker build --no-cache -t envoy:latest -f ../Dockerfile.server ../ && cd ../..
> HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose -f envoy/demo/docker-compose.yaml up -d
> ```

### 4 — Initialise cactus-client config

Run from the workspace root:

```bash
cactus setup --local-cfg ./cactus-test
cactus server dcap https://localhost:8443/dcap
cactus server verify true
cactus server serca ./envoy/demo/tls-termination/test_certs/testca.crt
```

Then run `setup_clients.py` to register all demo clients. It derives LFDIs directly from the certificates:

```bash
python ./cactus-client-envoy/setup_clients.py ./envoy/demo/tls-termination/test_certs
```

This registers:

| ID | Cert | Type |
|---|---|---|
| `device1` | `testdevice1.crt` | device |
| `device2` | `testdevice2.crt` | device |
| `aggregator1` | `testaggregator.crt` | aggregator |
| `aggregator2` | `testaggregator2.crt` | aggregator |

### 5 — Set environment variables

Run from the workspace root:

```bash
cp cactus-client-envoy/sample.env cactus-client-envoy/.env
```

`sample.env` contains the correct credentials for the demo environment — no editing required. The plugin loads this file automatically at import time.

### 6 — Run a test

Run from the workspace root:

```bash
cactus tests                            # list all available test procedure IDs
cactus run S-ALL-01 device1             # run a test with a single device client
cactus run S-ALL-05 device1 device2     # run a test requiring two clients
```

Test reports are written to `./cactus-test/`.

---

## Resetting the demo environment

Each test run automatically resets Envoy's database state (registered devices, DER controls, etc.) via the `admin_setup` hook — no manual reset is needed between test runs.

If you need a full reset (e.g. after making local changes to `envoy` source, or if the stack gets into a bad state), use the `reset.sh` script, which rebuilds the Docker image from source and restarts all services with fresh volumes:

Run from **`envoy/demo/`**:

```bash
cd envoy/demo
./reset.sh
cd ../..
```

After `reset.sh` completes, the `test_certs/` directory will be repopulated with freshly generated certificates. Re-run step 4 (`setup_clients.py`) to update the LFDIs in your `.cactus.yaml`, as new certs will have different LFDIs:

```bash
python ./cactus-client-envoy/setup_clients.py ./envoy/demo/tls-termination/test_certs
```

---

## Environment variables

| Variable | Description | Example |
|---|---|---|
| `ENVOY_DB_DSN` | SQLAlchemy async DSN for the Envoy PostgreSQL database | `postgresql+asyncpg://test_user:test_pwd@localhost:8003/test_db` |
| `DATABASE_URL` | Required by Envoy's model layer at import time — use the same value | `postgresql+asyncpg://test_user:test_pwd@localhost:8003/test_db` |
| `ENVOY_ADMIN_URI` | Base URL of the envoy-admin service | `http://localhost:8001` |
| `ENVOY_ADMIN_USERNAME` | Basic auth username for envoy-admin (defaults to `admin`) | `admin` |
| `ENVOY_ADMIN_PASSWORD` | Basic auth password for envoy-admin (defaults to `password`) | `password` |

All are pre-configured in `sample.env` for the demo environment. The plugin loads `cactus-client-envoy/.env` automatically at import time — you do not need to export these variables manually.

## Supported admin instructions

For parameter documentation see [`cactus-test-definitions`](https://github.com/bsgip/cactus-test-definitions). Envoy-specific constraints:

- `ensure-end-device`: `has_registration_link=False` is not supported — envoy always includes a RegistrationLink. For `client_type=aggregator`, the aggregator certificate must already be registered in the Envoy DB.
- `create-der-control`: if no `SiteControlGroup` exists for the given primacy, one is created automatically. Scheduled controls without `start_offset_seconds` are stacked sequentially after the latest existing end time.

Supported: `ensure-end-device`, `ensure-mup-list-empty`, `ensure-fsa`, `ensure-der-program`, `set-client-access`, `ensure-der-control-list`, `create-der-control`, `create-default-der-control`, `clear-der-controls`, `set-poll-rate`, `set-post-rate`

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

Run from **`cactus-client-envoy/`**:

```bash
pip install -e .[dev,test]
pytest tests/
```
