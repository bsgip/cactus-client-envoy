import logging
from datetime import timedelta

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now
from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.site import Site
from envoy.server.model.subscription import SubscriptionResource
from envoy.server.model.tariff import Tariff, TariffComponent, TariffGeneratedRate
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client_envoy.handler.common import site_group_ids_for_site

logger = logging.getLogger(__name__)


async def ensure_tariff_profile(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession, fsa_annotations: dict[str, int]
) -> ActionResult:
    """Ensure a Tariff (TariffProfile) exists within the FSA identified by fsa_annotation.

    Resolves fsa_id from the annotation map populated by ensure-fsa, mirroring ensure_der_program.
    """
    fsa_annotation: str | None = instruction.parameters.get("fsa_annotation")
    primacy: int = instruction.parameters["primacy"]
    currency_code: int = instruction.parameters["currency_code"]

    if fsa_annotation is not None:
        fsa_id = fsa_annotations.get(fsa_annotation)
        if fsa_id is None:
            return ActionResult.failed(
                f"ensure-tariff-profile: unknown fsa_annotation '{fsa_annotation}' — run ensure-fsa with"
                " annotation first"
            )
    else:
        fsa_id = 1

    tariff = (await session.execute(select(Tariff).where(Tariff.fsa_id == fsa_id))).scalar_one_or_none()

    if tariff is None:
        now = utc_now()
        tariff = Tariff(
            name=f"cactus-fsa{fsa_id}",
            dnsp_code="CACTUS",
            currency_code=currency_code,
            primacy=primacy,
            fsa_id=fsa_id,
            changed_time=now,
        )
        session.add(tariff)
        await session.flush()
        logger.info("ensure-tariff-profile: created Tariff fsa_id=%d (id=%d)", fsa_id, tariff.tariff_id)
        await NotificationManager.notify_changed_deleted_entities(session, SubscriptionResource.TARIFF, now)
        await session.commit()
    else:
        logger.info("ensure-tariff-profile: Tariff already exists fsa_id=%d (id=%d)", fsa_id, tariff.tariff_id)
        await session.commit()

    return ActionResult.done()


async def create_rate_component(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession, rate_component_tags: dict[str, int]
) -> ActionResult:
    """Create a TariffComponent (RateComponent) within the (single) Tariff, recording tariff_component_id under
    tag for later reference from create-time-tariff-interval."""
    tag: str = instruction.parameters["tag"]

    tariff = (await session.execute(select(Tariff).limit(1))).scalar_one_or_none()
    if tariff is None:
        return ActionResult.failed("create-rate-component: no Tariff found — run ensure-tariff-profile first")

    now = utc_now()
    component = TariffComponent(
        tariff_id=tariff.tariff_id,
        role_flags=instruction.parameters["role_flags"],
        commodity=instruction.parameters.get("commodity"),
        flow_direction=instruction.parameters.get("flow_direction"),
        uom=instruction.parameters.get("uom"),
        changed_time=now,
    )
    session.add(component)
    await session.flush()

    rate_component_tags[tag] = component.tariff_component_id
    logger.info("create-rate-component: created TariffComponent tag='%s' (id=%d)", tag, component.tariff_component_id)
    await NotificationManager.notify_changed_deleted_entities(session, SubscriptionResource.TARIFF_COMPONENT, now)
    await session.commit()
    return ActionResult.done()


async def create_time_tariff_interval(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession, rate_component_tags: dict[str, int]
) -> ActionResult:
    """Create a TariffGeneratedRate (TimeTariffInterval) within the RateComponent identified by
    rate_component_tag."""
    rate_component_tag: str = instruction.parameters["rate_component_tag"]
    duration_seconds: int = instruction.parameters["duration_seconds"]
    price_pow10_encoded: int = instruction.parameters["price_pow10_encoded"]

    tariff_component_id = rate_component_tags.get(rate_component_tag)
    if tariff_component_id is None:
        return ActionResult.failed(
            f"create-time-tariff-interval: unknown rate_component_tag '{rate_component_tag}' — run"
            " create-rate-component first"
        )

    component = (
        await session.execute(select(TariffComponent).where(TariffComponent.tariff_component_id == tariff_component_id))
    ).scalar_one_or_none()
    if component is None:
        return ActionResult.failed(
            f"create-time-tariff-interval: TariffComponent id={tariff_component_id} no longer exists"
        )

    client_config = context.client_config_for(instruction.client)
    site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
    if site is None:
        return ActionResult.failed(
            f"create-time-tariff-interval: no site found for LFDI {client_config.lfdi} — run ensure-end-device first"
        )

    site_group_ids = await site_group_ids_for_site(session, site.site_id)
    if not site_group_ids:
        return ActionResult.failed(
            f"create-time-tariff-interval: site_id={site.site_id} has no SiteGroup membership — run"
            " ensure-end-device first"
        )
    site_group_id = site_group_ids[0]

    now = utc_now()
    start_time = now
    end_time = start_time + timedelta(seconds=duration_seconds)

    rate = TariffGeneratedRate(
        tariff_id=component.tariff_id,
        tariff_component_id=tariff_component_id,
        site_group_id=site_group_id,
        start_time=start_time,
        duration_seconds=duration_seconds,
        end_time=end_time,
        price_pow10_encoded=price_pow10_encoded,
        changed_time=now,
    )
    session.add(rate)
    await session.flush()
    logger.info(
        "create-time-tariff-interval: created TariffGeneratedRate tag='%s' (id=%d) start=%s end=%s",
        rate_component_tag,
        rate.tariff_generated_rate_id,
        start_time,
        end_time,
    )
    await NotificationManager.notify_changed_deleted_entities(session, SubscriptionResource.TARIFF_GENERATED_RATE, now)
    await NotificationManager.notify_changed_deleted_entities(
        session, SubscriptionResource.COMBINED_TARIFF_GENERATED_RATE, now
    )
    await session.commit()
    return ActionResult.done()
