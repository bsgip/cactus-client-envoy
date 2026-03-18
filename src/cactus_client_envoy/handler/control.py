import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.doe import DynamicOperatingEnvelope, SiteControlGroup
from envoy.server.model.site import Site
from sqlalchemy import delete, select
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
        group = SiteControlGroup(description="cactus-default", primacy=1, fsa_id=1, changed_time=utc_now())
        session.add(group)
        await session.commit()
        logger.info("ensure-der-control-list: created default SiteControlGroup (id=%d)", group.site_control_group_id)
    return ActionResult.done()


async def clear_der_controls(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    """Cancel active DERControls by deleting them from the DB.

    If all=True, all currently active controls for the client's site are removed.
    Otherwise only the most recently started active control is removed.
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
        result = await session.execute(delete(DynamicOperatingEnvelope).where(active_filter))
        logger.info("clear-der-controls: deleted %d active control(s) for site_id=%d", result.rowcount, site.site_id)
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
                delete(DynamicOperatingEnvelope).where(
                    DynamicOperatingEnvelope.dynamic_operating_envelope_id == most_recent.dynamic_operating_envelope_id
                )
            )
            logger.info(
                "clear-der-controls: deleted most recent active control id=%d for site_id=%d",
                most_recent.dynamic_operating_envelope_id,
                site.site_id,
            )
        else:
            logger.info("clear-der-controls: no active controls found for site_id=%d", site.site_id)

    await session.commit()
    return ActionResult.done()
