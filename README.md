# cactus-client-envoy

An [apluggy](https://github.com/simonsobs/apluggy) plugin for [cactus-client](../cactus-client) that implements admin instructions against a local [Envoy](../envoy) server via its admin API.

When `cactus-client` encounters an `admin_instruction` step (e.g. `ensure-end-device`, `create-der-control`), it calls registered plugins via the `cactus_client.admin` hook system. This package provides the Envoy-backed implementation of those hooks.

## Installation

Install alongside `cactus-client` in the same environment:

```
pip install -e .
```

Once installed, `cactus-client` will automatically discover and load this plugin via the `cactus_client.admin` setuptools entry point.

## Configuration

The plugin needs to know the base URL of the Envoy admin API. Set the `ENVOY_ADMIN_URL` environment variable:

```
export ENVOY_ADMIN_URL=http://localhost:8888
```

## Development

```
pip install -e .[dev,test]
pytest tests/
```
