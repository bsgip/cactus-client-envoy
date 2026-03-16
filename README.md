# cactus-client-envoy

An [apluggy](https://github.com/simonsobs/apluggy) plugin for [cactus-client](../cactus-client) that implements admin instructions against a local [Envoy](../envoy) server via direct database access.

When `cactus-client` encounters an `admin_instruction` step (e.g. `ensure-end-device`), it calls registered plugins via the `cactus_client.admin` hook system. This package provides the Envoy-backed implementation of those hooks.

## How it works

The plugin connects directly to Envoy's PostgreSQL database rather than going through an HTTP API. This keeps the plugin self-contained — it manages its own DB connection using credentials from environment variables, with no dependency on Envoy's HTTP layer.

- **`admin_setup`** — opens a SQLAlchemy async engine to the Envoy DB
- **`admin_teardown`** — disposes the engine (always runs, even on test failure)
- **`admin_instruction`** — handles each instruction in its own DB session with an explicit commit

## Installation

Install alongside `cactus-client` in the same Python environment:

```
pip install -e .
```

Once installed, `cactus-client` will automatically discover and load this plugin via the `cactus_client.admin` setuptools entry point — no code changes required.

## Configuration

Two environment variables are required:

| Variable | Description | Example |
|---|---|---|
| `ENVOY_DB_DSN` | SQLAlchemy async DSN for the Envoy PostgreSQL database | `postgresql+asyncpg://user:pass@localhost:5432/envoy` |
| `DATABASE_URL` | Required by Envoy's model layer at import time (same DB, same format) | `postgresql+asyncpg://user:pass@localhost:5432/envoy` |

In practice both variables point to the same database:

```bash
export ENVOY_DB_DSN=postgresql+asyncpg://envoy:secret@localhost:5432/envoy
export DATABASE_URL=postgresql+asyncpg://envoy:secret@localhost:5432/envoy
```

The Envoy database must already have Alembic migrations applied before running tests.

## Supported admin instructions

### `ensure-end-device`

Ensures a virtual DER client is registered (or not) as a site in the Envoy database.

```yaml
admin_instructions:
  - type: ensure-end-device
    parameters:
      registered: true   # true = register the site, false = remove it
```

Optional `client` parameter selects a specific client alias; if omitted, the first configured client is used.

The site is created with `aggregator_id=0` (direct/null aggregator), `timezone_id=UTC`, `device_category=PHOTOVOLTAIC_SYSTEM`, and `post_rate_seconds=60`. The `registration_pin` and LFDI/SFDI are taken from the client's `.cactus.yaml` config.

## Development

```
pip install -e .[dev,test]
pytest tests/
```
