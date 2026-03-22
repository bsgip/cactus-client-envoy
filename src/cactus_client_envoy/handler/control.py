import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.doe import DynamicOperatingEnvelope, SiteControlGroup
from envoy.server.model.site import Site
from envoy.server.model.subscription import SubscriptionResource
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now

from cactus_client_envoy.handler.common import resolve_client_config

logger = logging.getLogger(__name__)


async def ensure_der_control_list(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    """Ensure the DERControlList is accessible to the client.

    In envoy the DERControlList is accessible once at least one SiteControlGroup (DERProgram) exists.
    Creates a default SiteControlGroup if none exists.
    The subscribable flag is advisory — envoy supports subscriptions for all registered clients by default.
    """
    group = (await session.execute(select(SiteControlGroup).limit(1))).scalar_one_or_none()
    if group is None:
        now = utc_now()
        group = SiteControlGroup(description="cactus-default", primacy=1, fsa_id=1, changed_time=now)
        session.add(group)
        await session.commit()
        logger.info("ensure-der-control-list: created default SiteControlGroup (id=%d)", group.site_control_group_id)
        await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.SITE_CONTROL_GROUP, now)
    return ActionResult.done()


async def clear_der_controls(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    """Cancel active DERControls by superseding them.

    If all=True, all currently active controls for the client's site are superseded.
    Otherwise only the most recently started active control is superseded.
    """
    clear_all: bool = instruction.parameters.get("all", False)
    client_config = resolve_client_config(instruction, context)

    site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
    if site is None:
        return ActionResult.failed(
            f"clear-der-controls: no site found for LFDI {client_config.lfdi} — run ensure-end-device first"
        )

    now = utc_now()
    active_filter = (
        (DynamicOperatingEnvelope.site_id == site.site_id)
        & (DynamicOperatingEnvelope.start_time <= now)
        & (DynamicOperatingEnvelope.end_time > now)
        & (DynamicOperatingEnvelope.superseded == False)  # noqa: E712
    )

    if clear_all:
        result = await session.execute(
            update(DynamicOperatingEnvelope).where(active_filter).values(superseded=True, changed_time=now)
        )
        logger.info("clear-der-controls: superseded %d active control(s) for site_id=%d", result.rowcount, site.site_id)
    else:
        most_recent = (
            await session.execute(
                select(DynamicOperatingEnvelope)
                .where(active_filter)
                .order_by(DynamicOperatingEnvelope.start_time.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        if most_recent is not None:
            await session.execute(
                update(DynamicOperatingEnvelope)
                .where(
                    DynamicOperatingEnvelope.dynamic_operating_envelope_id == most_recent.dynamic_operating_envelope_id
                )
                .values(superseded=True, changed_time=now)
            )
            logger.info(
                "clear-der-controls: superseded most recent active control id=%d for site_id=%d",
                most_recent.dynamic_operating_envelope_id,
                site.site_id,
            )
        else:
            logger.info("clear-der-controls: no active controls found for site_id=%d", site.site_id)

    await session.commit()
    await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.DYNAMIC_OPERATING_ENVELOPE, now)
    return ActionResult.done()
