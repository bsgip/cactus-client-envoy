import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.site import Site
from envoy.server.model.site_reading import SiteReading, SiteReadingType
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult

from cactus_client_envoy.handler.common import resolve_client_config

logger = logging.getLogger(__name__)


async def ensure_mup_list_empty(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    client_config = resolve_client_config(instruction, context)

    site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
    if site is None:
        logger.info("ensure-mup-list-empty: no site found for LFDI %s, nothing to remove", client_config.lfdi)
        return ActionResult.done()

    srt_ids = (
        (
            await session.execute(
                select(SiteReadingType.site_reading_type_id).where(SiteReadingType.site_id == site.site_id)
            )
        )
        .scalars()
        .all()
    )
    if srt_ids:
        await session.execute(delete(SiteReading).where(SiteReading.site_reading_type_id.in_(srt_ids)))
        await session.execute(delete(SiteReadingType).where(SiteReadingType.site_id == site.site_id))
        logger.info("ensure-mup-list-empty: deleted %d SiteReadingType(s) for site_id=%d", len(srt_ids), site.site_id)
    else:
        logger.info("ensure-mup-list-empty: no MUPs found for site_id=%d", site.site_id)

    await session.commit()
    return ActionResult.done()
