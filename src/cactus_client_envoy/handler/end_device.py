import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.aggregator import NULL_AGGREGATOR_ID
from envoy.server.model.site import Site
from envoy_schema.server.schema.sep2.types import DeviceCategory
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now

from cactus_client_envoy.handler.common import resolve_client_config

logger = logging.getLogger(__name__)


async def ensure_end_device(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
) -> ActionResult:
    registered: bool = instruction.parameters["registered"]
    unimplemented = [p for p in ("client_type", "has_der_list", "has_registration_link") if instruction.parameters.get(p) is not None]
    if unimplemented:
        raise NotImplementedError(f"ensure-end-device: unimplemented parameters: {unimplemented}")

    client_config = resolve_client_config(instruction, context)

    stmt = select(Site).where((Site.aggregator_id == NULL_AGGREGATOR_ID) & (Site.lfdi == client_config.lfdi))
    existing = (await session.execute(stmt)).scalar_one_or_none()

    if registered:
        if existing is not None:
            logger.info("ensure-end-device: site already exists for LFDI %s (site_id=%s)", client_config.lfdi, existing.site_id)
            return ActionResult.done()
        site = Site(
            aggregator_id=NULL_AGGREGATOR_ID,
            timezone_id="UTC",
            changed_time=utc_now(),
            lfdi=client_config.lfdi,
            sfdi=client_config.sfdi,
            device_category=DeviceCategory.PHOTOVOLTAIC_SYSTEM,
            registration_pin=client_config.pin,
            post_rate_seconds=60,
        )
        session.add(site)
        await session.flush()
        await session.commit()
        logger.info("ensure-end-device: registered site for LFDI %s", client_config.lfdi)
        return ActionResult.done()
    else:
        if existing is None:
            logger.info("ensure-end-device: no site found for LFDI %s, nothing to remove", client_config.lfdi)
            return ActionResult.done()
        await session.execute(delete(Site).where(Site.site_id == existing.site_id))
        await session.commit()
        logger.info("ensure-end-device: deleted site site_id=%s LFDI=%s", existing.site_id, client_config.lfdi)
        return ActionResult.done()
