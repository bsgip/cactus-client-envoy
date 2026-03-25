import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.server import RuntimeServerConfig
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)

_POLL_RATE_FIELD_MAP: dict[str, str] = {
    "DeviceCapability": "dcap_pollrate_seconds",
    "EndDeviceList": "edevl_pollrate_seconds",
    "FunctionSetAssignmentsList": "fsal_pollrate_seconds",
    "DERProgramList": "derpl_pollrate_seconds",
    "DERList": "derl_pollrate_seconds",
}

_POST_RATE_FIELD_MAP: dict[str, str] = {
    "MirrorUsagePoint": "mup_postrate_seconds",
    "MirrorUsagePointList": "mup_postrate_seconds",
}


async def set_poll_rate(instruction: AdminInstruction, context: AdminContext, session: AsyncSession) -> ActionResult:
    resource: str = instruction.parameters["resource"]
    rate_seconds: int = instruction.parameters["rate_seconds"]

    field = _POLL_RATE_FIELD_MAP.get(resource)
    if field is None:
        return ActionResult.failed(
            f"set-poll-rate: unsupported resource '{resource}'. " f"Supported: {list(_POLL_RATE_FIELD_MAP)}"
        )

    await _update_runtime_config(session, field, rate_seconds)
    logger.info("set-poll-rate: set %s=%d", field, rate_seconds)
    return ActionResult.done()


async def set_post_rate(instruction: AdminInstruction, context: AdminContext, session: AsyncSession) -> ActionResult:
    resource: str = instruction.parameters["resource"]
    rate_seconds: int = instruction.parameters["rate_seconds"]

    field = _POST_RATE_FIELD_MAP.get(resource)
    if field is None:
        return ActionResult.failed(
            f"set-post-rate: unsupported resource '{resource}'. " f"Supported: {list(_POST_RATE_FIELD_MAP)}"
        )

    await _update_runtime_config(session, field, rate_seconds)
    logger.info("set-post-rate: set %s=%d", field, rate_seconds)
    return ActionResult.done()


async def _update_runtime_config(session: AsyncSession, field: str, value: int) -> None:
    config = (
        await session.execute(select(RuntimeServerConfig).where(RuntimeServerConfig.runtime_server_config_id == 1))
    ).scalar_one_or_none()
    now = utc_now()
    if config is None:
        config = RuntimeServerConfig(changed_time=now)
        session.add(config)
    else:
        config.changed_time = now
    setattr(config, field, value)
    await session.flush()
    await session.commit()
