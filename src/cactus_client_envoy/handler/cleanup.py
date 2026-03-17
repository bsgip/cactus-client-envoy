import logging

from envoy.server.model.doe import DynamicOperatingEnvelope, SiteControlGroup, SiteControlGroupDefault
from envoy.server.model.site import Site
from envoy.server.model.site_reading import SiteReading, SiteReadingType
from envoy.server.model.subscription import Subscription
from envoy.server.model.tariff import TariffGeneratedRate
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def reset_test_state(session: AsyncSession) -> None:
    """Delete all test-created envoy state, leaving pre-seeded aggregator/certificate rows intact.

    Deletion order respects FK deps. Site deletion should include SiteDER (and children), SiteGroupAssignment,
    SiteLogEvent, and response.
    """
    await session.execute(delete(SiteReading))
    await session.execute(delete(SiteReadingType))
    await session.execute(delete(Subscription))
    await session.execute(delete(TariffGeneratedRate))
    await session.execute(delete(DynamicOperatingEnvelope))
    await session.execute(delete(SiteControlGroupDefault))
    await session.execute(delete(SiteControlGroup))
    await session.execute(delete(Site))
    await session.commit()
    logger.info("reset_test_state: envoy test state cleared")
