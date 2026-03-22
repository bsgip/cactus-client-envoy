import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from envoy.server.model.aggregator import Aggregator, AggregatorDomain, NULL_AGGREGATOR_ID
from envoy.server.model.doe import DynamicOperatingEnvelope, SiteControlGroup, SiteControlGroupDefault
from envoy.server.model.site import Site
from envoy.server.model.site_reading import SiteReading, SiteReadingType
from envoy.server.model.subscription import Subscription
from envoy.server.model.tariff import TariffGeneratedRate
from sqlalchemy import delete, select
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


async def ensure_notification_domain_whitelisted(session: AsyncSession, notification_uri: str) -> None:
    """Add the notification server's hostname to every aggregator's domain whitelist if not already present."""
    hostname = urlparse(notification_uri).hostname
    if not hostname:
        logger.warning(
            f"ensure_notification_domain_whitelisted: could not parse hostname from {notification_uri}, skipping"
        )
        return

    aggregator_ids = (
        (await session.execute(select(Aggregator.aggregator_id).where(Aggregator.aggregator_id != NULL_AGGREGATOR_ID)))
        .scalars()
        .all()
    )

    now = datetime.now(timezone.utc)
    for aggregator_id in aggregator_ids:
        existing = (
            await session.execute(
                select(AggregatorDomain).where(
                    (AggregatorDomain.aggregator_id == aggregator_id) & (AggregatorDomain.domain == hostname)
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(AggregatorDomain(aggregator_id=aggregator_id, domain=hostname, changed_time=now))
            logger.info(
                f"ensure_notification_domain_whitelisted: added domain {hostname} for aggregator_id={aggregator_id}"
            )

    await session.commit()
