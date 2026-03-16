from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AdminContext


def resolve_client_config(instruction: AdminInstruction, context: AdminContext) -> ClientConfig:
    if instruction.client is None:
        return next(iter(context.client_configs.values()))
    return context.client_configs[instruction.client]
