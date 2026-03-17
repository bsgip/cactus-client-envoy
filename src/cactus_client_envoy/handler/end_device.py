import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction, ClientType
from envoy.server.model.aggregator import AggregatorCertificateAssignment, NULL_AGGREGATOR_ID
from envoy.server.model.base import Certificate
from envoy.server.model.site import Site, SiteDER
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
    has_der_list: bool | None = instruction.parameters.get("has_der_list")
    has_registration_link: bool | None = instruction.parameters.get("has_registration_link")
    client_type_param: str | None = instruction.parameters.get("client_type")

    client_config = resolve_client_config(instruction, context)

    # envoy always includes a RegistrationLink for registered sites (registration_pin is non-nullable)
    if has_registration_link is False: # TODO: What should behaviour be?
        raise NotImplementedError(
            "ensure-end-device: has_registration_link=False is not supported — "
            "envoy always includes a RegistrationLink for registered sites"
        )

    is_aggregator = client_type_param == ClientType.AGGREGATOR or (
        client_type_param is None and client_config.type == ClientType.AGGREGATOR
    )

    if is_aggregator:
        aggregator_id = await _resolve_aggregator_id(client_config.lfdi, session)
        if aggregator_id is None:
            return ActionResult.failed(
                f"ensure-end-device: no aggregator found for LFDI {client_config.lfdi} — "
                "is the aggregator certificate registered in the envoy DB?"
            )
        device_category = DeviceCategory.VIRTUAL_OR_MIXED_DER
    else:
        aggregator_id = NULL_AGGREGATOR_ID
        device_category = DeviceCategory.PHOTOVOLTAIC_SYSTEM

    stmt = select(Site).where((Site.aggregator_id == aggregator_id) & (Site.lfdi == client_config.lfdi))
    existing = (await session.execute(stmt)).scalar_one_or_none()

    if registered:
        if existing is None:
            site = Site(
                aggregator_id=aggregator_id,
                timezone_id="UTC",
                changed_time=utc_now(),
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
        return ActionResult.done()
    else:
        if existing is None:
            logger.info("ensure-end-device: no site found for LFDI %s, nothing to remove", client_config.lfdi)
            return ActionResult.done()
        await session.execute(delete(Site).where(Site.site_id == existing.site_id))
        await session.commit()
        logger.info("ensure-end-device: deleted site site_id=%s LFDI=%s", existing.site_id, client_config.lfdi)
        return ActionResult.done()


async def _resolve_aggregator_id(lfdi: str, session: AsyncSession) -> int | None:
    """Look up the aggregator_id for an aggregator client LFDI via certificate → aggregator assignment tables."""
    stmt = (
        select(AggregatorCertificateAssignment.aggregator_id)
        .join(Certificate, Certificate.certificate_id == AggregatorCertificateAssignment.certificate_id)
        .where(Certificate.lfdi == lfdi)
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def _ensure_site_der(site_id: int, session: AsyncSession) -> None:
    """Create a SiteDER for the given site if one does not already exist."""
    existing = (await session.execute(select(SiteDER).where(SiteDER.site_id == site_id))).scalar_one_or_none()
    if existing is None:
        session.add(SiteDER(site_id=site_id, changed_time=utc_now()))
        await session.flush()
        logger.info("ensure-end-device: created SiteDER for site_id=%s", site_id)
