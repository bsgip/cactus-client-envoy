import logging
from datetime import datetime

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.server import RuntimeServerConfig
from envoy.server.model.subscription import SubscriptionResource
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

# Mirrors ConfigManager.update_current_config: changing these poll rates triggers notifications
# for the associated resources so subscribers receive an immediate update.
_POLL_RATE_NOTIFICATION_MAP: dict[str, SubscriptionResource] = {
    "EndDeviceList": SubscriptionResource.SITE,
    "FunctionSetAssignmentsList": SubscriptionResource.FUNCTION_SET_ASSIGNMENTS,
    "DERProgramList": SubscriptionResource.SITE_CONTROL_GROUP,
}


async def set_poll_rate(instruction: AdminInstruction, context: AdminContext, session: AsyncSession) -> ActionResult:
    resource: str = instruction.parameters["resource"]
    rate_seconds: int = instruction.parameters["rate_seconds"]

    field = _POLL_RATE_FIELD_MAP.get(resource)
    if field is None:
        return ActionResult.failed(
            f"set-poll-rate: unsupported resource '{resource}'. " f"Supported: {list(_POLL_RATE_FIELD_MAP)}"
        )

    now = await _update_runtime_config(session, field, rate_seconds)
    logger.info("set-poll-rate: set %s=%d", field, rate_seconds)

    notification_resource = _POLL_RATE_NOTIFICATION_MAP.get(resource)
    if notification_resource is not None:
        await NotificationManager.notify_changed_deleted_entities(notification_resource, now)

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


async def _update_runtime_config(session: AsyncSession, field: str, value: int) -> datetime:
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
    return now
