import logging
from datetime import timedelta
from decimal import Decimal
from typing import Optional

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.admin.crud.doe import supersede_then_insert_does
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.doe import DynamicOperatingEnvelope, SiteControlGroup, SiteControlGroupDefault
from envoy.server.model.site import Site
from envoy.server.model.subscription import SubscriptionResource
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now

from cactus_client_envoy.handler.common import resolve_client_config

logger = logging.getLogger(__name__)

DEFAULT_DURATION_SECONDS = 8
DEFAULT_SCHEDULED_OFFSET_SECONDS = 2


async def create_der_control(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
) -> ActionResult:
    status: str = instruction.parameters["status"]  # "active" or "scheduled"
    primacy: int = instruction.parameters.get("primacy", 1)
    duration_seconds: int = instruction.parameters.get("duration_seconds", DEFAULT_DURATION_SECONDS)
    start_offset_seconds: Optional[int] = instruction.parameters.get("start_offset_seconds")

    client_config = resolve_client_config(instruction, context)

    # Look up site by LFDI
    site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
    if site is None:
        return ActionResult.failed(
            f"create-der-control: no site found for LFDI {client_config.lfdi} — run ensure-end-device first"
        )

    # Find or create a SiteControlGroup (DERProgram) for the given primacy
    group = (
        await session.execute(select(SiteControlGroup).where(SiteControlGroup.primacy == primacy))
    ).scalar_one_or_none()
    if group is None:
        group = SiteControlGroup(
            description=f"cactus-primacy-{primacy}", primacy=primacy, fsa_id=1, changed_time=utc_now()
        )
        session.add(group)
        await session.flush()
        logger.info(
            "create-der-control: created SiteControlGroup primacy=%d (id=%d)", primacy, group.site_control_group_id
        )

    now = utc_now()

    if status == "active":
        # Start in the past so the control is currently active
        start_time = now - timedelta(seconds=start_offset_seconds if start_offset_seconds is not None else 1)
    else:
        # "scheduled" — start in the future
        if start_offset_seconds is not None:
            start_time = now + timedelta(seconds=start_offset_seconds)
        else:
            # Stack sequentially after the latest existing non-expired control for this site+group.
            # If there is no control (or latest end_time is already in the past), use a default future offset so the
            # DOE stays "Scheduled" long enough for discovery before its "Active".
            latest_end = (
                await session.execute(
                    select(func.max(DynamicOperatingEnvelope.end_time)).where(
                        (DynamicOperatingEnvelope.site_id == site.site_id)
                        & (DynamicOperatingEnvelope.site_control_group_id == group.site_control_group_id)
                    )
                )
            ).scalar_one_or_none()
            if latest_end is not None and latest_end > now:
                start_time = latest_end + timedelta(seconds=1)
            else:
                start_time = now + timedelta(seconds=DEFAULT_SCHEDULED_OFFSET_SECONDS)

    end_time = start_time + timedelta(seconds=duration_seconds)

    export_limit = _dec(instruction.parameters.get("opModExpLimW"))
    if export_limit is None and all(
        instruction.parameters.get(k) is None
        for k in ("opModImpLimW", "opModGenLimW", "opModLoadLimW", "opModConnect", "opModEnergize", "opModFixedW")
    ):
        export_limit = Decimal(0)

    doe = DynamicOperatingEnvelope(
        site_control_group_id=group.site_control_group_id,
        site_id=site.site_id,
        changed_time=now,
        start_time=start_time,
        duration_seconds=duration_seconds,
        end_time=end_time,
        superseded=False,
        randomize_start_seconds=instruction.parameters.get("randomizeStart_seconds"),
        import_limit_active_watts=_dec(instruction.parameters.get("opModImpLimW")),
        export_limit_watts=export_limit,
        generation_limit_active_watts=_dec(instruction.parameters.get("opModGenLimW")),
        load_limit_active_watts=_dec(instruction.parameters.get("opModLoadLimW")),
        set_connected=instruction.parameters.get("opModConnect"),
        set_energized=instruction.parameters.get("opModEnergize"),
        set_point_percentage=_dec(instruction.parameters.get("opModFixedW")),
        ramp_time_seconds=_dec(
            instruction.parameters.get("rampTms"), divisor=100
        ),  # rampTms is hundredths of seconds; DB stores seconds
    )
    await supersede_then_insert_does(session, [doe], now)
    await session.commit()
    logger.info(
        "create-der-control: created DOE site_id=%d start=%s end=%s",
        site.site_id,
        start_time,
        end_time,
    )
    await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.DYNAMIC_OPERATING_ENVELOPE, now)
    return ActionResult.done()


async def create_default_der_control(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
) -> ActionResult:
    primacy: int = instruction.parameters.get("primacy", 1)

    # Find or create a SiteControlGroup (DERProgram) for the given primacy
    group = (
        await session.execute(select(SiteControlGroup).where(SiteControlGroup.primacy == primacy))
    ).scalar_one_or_none()
    if group is None:
        group = SiteControlGroup(
            description=f"cactus-primacy-{primacy}", primacy=primacy, fsa_id=1, changed_time=utc_now()
        )
        session.add(group)
        await session.flush()
        logger.info(
            "create-default-der-control: created SiteControlGroup primacy=%d (id=%d)",
            primacy,
            group.site_control_group_id,
        )

    existing = (
        await session.execute(
            select(SiteControlGroupDefault).where(
                SiteControlGroupDefault.site_control_group_id == group.site_control_group_id
            )
        )
    ).scalar_one_or_none()

    now = utc_now()
    if existing is not None:
        existing.import_limit_active_watts = _dec(instruction.parameters.get("opModImpLimW"))
        existing.export_limit_active_watts = _dec(instruction.parameters.get("opModExpLimW"))
        existing.generation_limit_active_watts = _dec(instruction.parameters.get("opModGenLimW"))
        existing.load_limit_active_watts = _dec(instruction.parameters.get("opModLoadLimW"))
        existing.ramp_rate_percent_per_second = instruction.parameters.get("setGradW")
        existing.version += 1
        existing.changed_time = now
        logger.info(
            "create-default-der-control: updated SiteControlGroupDefault id=%d (version=%d)",
            existing.site_control_group_default_id,
            existing.version,
        )
    else:
        default = SiteControlGroupDefault(
            site_control_group_id=group.site_control_group_id,
            changed_time=now,
            import_limit_active_watts=_dec(instruction.parameters.get("opModImpLimW")),
            export_limit_active_watts=_dec(instruction.parameters.get("opModExpLimW")),
            generation_limit_active_watts=_dec(instruction.parameters.get("opModGenLimW")),
            load_limit_active_watts=_dec(instruction.parameters.get("opModLoadLimW")),
            ramp_rate_percent_per_second=instruction.parameters.get("setGradW"),
        )
        session.add(default)
        logger.info(
            "create-default-der-control: created SiteControlGroupDefault for group_id=%d",
            group.site_control_group_id,
        )

    await session.flush()
    await session.commit()
    await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.DEFAULT_SITE_CONTROL, now)
    return ActionResult.done()


def _dec(value: Optional[float], divisor: int = 1) -> Optional[Decimal]:
    return Decimal(str(value)) / divisor if value is not None else None
