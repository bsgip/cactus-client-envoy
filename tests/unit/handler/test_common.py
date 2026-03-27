from assertical.fake.generator import generate_class_instance

from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client_envoy.handler.common import resolve_client_config


def test_resolve_client_config_no_client_returns_first():
    cfg_a = generate_class_instance(ClientConfig, seed=1)
    cfg_b = generate_class_instance(ClientConfig, seed=2)
    ctx = generate_class_instance(AdminContext, client_configs={"a": cfg_a, "b": cfg_b})
    instruction = AdminInstruction(type="ensure-end-device", client=None)

    result = resolve_client_config(instruction, ctx)

    assert result is cfg_a


def test_resolve_client_config_named_client():
    cfg_a = generate_class_instance(ClientConfig, seed=1)
    cfg_b = generate_class_instance(ClientConfig, seed=2)
    ctx = generate_class_instance(AdminContext, client_configs={"a": cfg_a, "b": cfg_b})
    instruction = AdminInstruction(type="ensure-end-device", client="b")

    result = resolve_client_config(instruction, ctx)

    assert result is cfg_b
