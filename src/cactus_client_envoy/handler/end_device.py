import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction, ClientType
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.aggregator import AggregatorCertificateAssignment
from envoy.server.model.base import Certificate
from envoy.server.model.doe import DynamicOperatingEnvelope
from envoy.server.model.site import Site, SiteDER
from envoy.server.model.site_reading import SiteReading, SiteReadingType
from envoy.server.model.subscription import Subscription, SubscriptionResource
from envoy.server.model.tariff import TariffGeneratedRate
from envoy_schema.server.schema.sep2.types import DeviceCategory
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now

from cactus_client_envoy.handler.common import find_aggregator_id, resolve_client_config

logger = logging.getLogger(__name__)


async def ensure_end_device(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    registered: bool = instruction.parameters["registered"]
    has_der_list: bool | None = instruction.parameters.get("has_der_list")
    has_registration_link: bool | None = instruction.parameters.get("has_registration_link")
    client_type_param: str | None = instruction.parameters.get("client_type")

    client_config = resolve_client_config(instruction, context)

    # envoy always includes a RegistrationLink for registered sites (registration_pin is non-nullable)
    if has_registration_link is False:
        raise NotImplementedError(
            "ensure-end-device: has_registration_link=False is not supported — envoy always includes one"
        )

    is_aggregator = client_type_param == ClientType.AGGREGATOR or (
        client_type_param is None and client_config.type == ClientType.AGGREGATOR
    )

    if is_aggregator:
        aggregator_id = await _resolve_aggregator_id(client_config.lfdi, session)
        if aggregator_id is None:
            if not registered:
                logger.info(
                    "ensure-end-device: no aggregator assignment found for LFDI %s, nothing to remove",
                    client_config.lfdi,
                )
                return ActionResult.done()
            cert = (
                await session.execute(select(Certificate).where(Certificate.lfdi == client_config.lfdi.lower()))
            ).scalar_one_or_none()
            if cert is None:
                return ActionResult.failed(
                    f"ensure-end-device: no certificate found for LFDI {client_config.lfdi} — "
                    "ensure the certificate is registered in the envoy DB."
                )
            aggregator_id = await find_aggregator_id(client_config.lfdi, context, session)
            if aggregator_id is None:
                return ActionResult.failed(
                    "ensure-end-device: cannot determine which aggregator to assign — "
                    "ensure an aggregator exists in the envoy DB."
                )
            session.add(
                AggregatorCertificateAssignment(certificate_id=cert.certificate_id, aggregator_id=aggregator_id)
            )
            await session.flush()
            logger.info(
                "ensure-end-device: granted aggregator access for LFDI %s to aggregator_id=%d",
                client_config.lfdi,
                aggregator_id,
            )
        device_category = DeviceCategory.VIRTUAL_OR_MIXED_DER
    else:
        aggregator_id = 0
        device_category = DeviceCategory.PHOTOVOLTAIC_SYSTEM

    stmt = select(Site).where((Site.aggregator_id == aggregator_id) & (Site.lfdi == client_config.lfdi))
    existing = (await session.execute(stmt)).scalar_one_or_none()

    if registered:
        site_created = False
        if existing is None:
            site_created = True
            site_changed_time = utc_now()
            site = Site(
                aggregator_id=aggregator_id,
                timezone_id="UTC",
                changed_time=site_changed_time,
                lfdi=client_config.lfdi,
                sfdi=client_config.sfdi,
                device_category=device_category,
                registration_pin=client_config.pin,
                post_rate_seconds=60,
            )
            session.add(site)
            await session.flush()
            logger.info("ensure-end-device: registered site for LFDI %s (site_id=%s)", client_config.lfdi, site.site_id)
        else:
            site = existing
            logger.info(
                "ensure-end-device: site already exists for LFDI %s (site_id=%s)", client_config.lfdi, site.site_id
            )

        if has_der_list:
            await _ensure_site_der(site.site_id, session)

        await session.commit()
        if site_created:
            await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.SITE, site_changed_time)
        return ActionResult.done()
    else:
        if existing is None:
            logger.info("ensure-end-device: no site found for LFDI %s, nothing to remove", client_config.lfdi)
            return ActionResult.done()
        await _delete_site(existing.site_id, session)
        await session.commit()
        logger.info("ensure-end-device: deleted site site_id=%s LFDI=%s", existing.site_id, client_config.lfdi)
        return ActionResult.done()


async def _delete_site(site_id: int, session: AsyncSession) -> None:
    """Delete a site and all dependent records that lack ON DELETE CASCADE in the DB schema.

    Mirrors delete_site_for_aggregator in envoy.server.crud.site, with two differences:
    - Plain DELETE instead of delete_rows_into_archive — no archival needed for test teardown.
    - Skips explicit DER child and SubscriptionCondition cleanup — the ON DELETE CASCADE handles it
    """

    # site_reading - site_reading_type (no cascade): delete readings before their types
    srt_ids = (
        (await session.execute(select(SiteReadingType.site_reading_type_id).where(SiteReadingType.site_id == site_id)))
        .scalars()
        .all()
    )
    if srt_ids:
        await session.execute(delete(SiteReading).where(SiteReading.site_reading_type_id.in_(srt_ids)))
    await session.execute(delete(SiteReadingType).where(SiteReadingType.site_id == site_id))

    # subscriptions (scoped_site_id is nullable FK, no cascade); SubscriptionCondition cascades from subscription
    await session.execute(delete(Subscription).where(Subscription.scoped_site_id == site_id))

    # tariff rates and DOEs (no cascade)
    await session.execute(delete(TariffGeneratedRate).where(TariffGeneratedRate.site_id == site_id))
    await session.execute(delete(DynamicOperatingEnvelope).where(DynamicOperatingEnvelope.site_id == site_id))

    # site itself (cascade handles: site_group_assignment, site_der + children, response, log_events)
    await session.execute(delete(Site).where(Site.site_id == site_id))


async def _resolve_aggregator_id(lfdi: str, session: AsyncSession) -> int | None:
    """Look up the aggregator_id for an aggregator client LFDI via certificate → aggregator assignment tables."""
    stmt = (
        select(AggregatorCertificateAssignment.aggregator_id)
        .join(Certificate, Certificate.certificate_id == AggregatorCertificateAssignment.certificate_id)
        .where(Certificate.lfdi == lfdi.lower())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def _ensure_site_der(site_id: int, session: AsyncSession) -> None:
    """Create a SiteDER for the given site if one does not already exist.
    Mirrors generate_default_site_der in envoy.server.crud.der
    """
    existing = (await session.execute(select(SiteDER).where(SiteDER.site_id == site_id))).scalar_one_or_none()
    if existing is None:
        session.add(SiteDER(site_id=site_id, changed_time=utc_now()))
        await session.flush()
        logger.info("ensure-end-device: created SiteDER for site_id=%s", site_id)
