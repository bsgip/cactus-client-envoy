import logging
from datetime import timedelta
from decimal import Decimal

import aiohttp
from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now
from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.doe import (
    DynamicOperatingEnvelope,
    SiteControlGroup,
    SiteControlGroupDefault,
)
from envoy.server.model.site import Site
from envoy.server.model.subscription import SubscriptionResource
from envoy_schema.admin.schema.site_control import SiteControlRequest
from envoy_schema.admin.schema.uri import SiteControlUri
from pydantic import TypeAdapter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client_envoy.handler.common import site_group_ids_for_site

logger = logging.getLogger(__name__)

DEFAULT_DURATION_SECONDS = 8
DEFAULT_SCHEDULED_OFFSET_SECONDS = 2

_site_control_list_adapter: TypeAdapter[list[SiteControlRequest]] = TypeAdapter(list[SiteControlRequest])


async def create_der_control(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
    admin_uri: str,
    admin_username: str,
    admin_password: str,
) -> ActionResult:
    status: str = instruction.parameters["status"]  # "active" or "scheduled"
    primacy: int = instruction.parameters.get("primacy", 1)
    duration_seconds: int = instruction.parameters.get("duration_seconds", DEFAULT_DURATION_SECONDS)
    start_offset_seconds: int | None = instruction.parameters.get("start_offset_seconds")

    client_config = context.client_config_for(instruction.client)

    # Look up site by LFDI
    site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
    if site is None:
        return ActionResult.failed(
            f"create-der-control: no site found for LFDI {client_config.lfdi} — run ensure-end-device first"
        )

    site_group_ids = await site_group_ids_for_site(session, site.site_id)
    if not site_group_ids:
        return ActionResult.failed(
            f"create-der-control: site_id={site.site_id} has no SiteGroup membership — run ensure-end-device first"
        )
    site_group_id = site_group_ids[0]

    # Find or create a SiteControlGroup (DERProgram) for the given primacy
    group = (
        await session.execute(select(SiteControlGroup).where(SiteControlGroup.primacy == primacy))
    ).scalar_one_or_none()
    if group is None:
        group = SiteControlGroup(
            description=f"cactus-primacy-{primacy}",
            primacy=primacy,
            fsa_id=1,
            changed_time=utc_now(),
        )
        session.add(group)
        await session.flush()
        await session.commit()
        logger.info(
            "create-der-control: created SiteControlGroup primacy=%d (id=%d)",
            primacy,
            group.site_control_group_id,
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
                        (DynamicOperatingEnvelope.site_group_id == site_group_id)
                        & (DynamicOperatingEnvelope.site_control_group_id == group.site_control_group_id)
                    )
                )
            ).scalar_one_or_none()
            if latest_end is not None and latest_end > now:
                start_time = latest_end + timedelta(seconds=1)
            else:
                start_time = now + timedelta(seconds=DEFAULT_SCHEDULED_OFFSET_SECONDS)

    export_limit = _dec(instruction.parameters.get("opModExpLimW"))
    if export_limit is None and all(
        instruction.parameters.get(k) is None
        for k in (
            "opModImpLimW",
            "opModGenLimW",
            "opModLoadLimW",
            "opModConnect",
            "opModEnergize",
            "opModFixedW",
        )
    ):
        export_limit = Decimal(0)

    request = SiteControlRequest(
        site_group_id=site_group_id,
        calculation_log_id=None,
        duration_seconds=duration_seconds,
        start_time=start_time,
        randomize_start_seconds=instruction.parameters.get("randomizeStart_seconds"),
        set_energized=instruction.parameters.get("opModEnergize"),
        set_connect=instruction.parameters.get("opModConnect"),
        import_limit_watts=_dec(instruction.parameters.get("opModImpLimW")),
        export_limit_watts=export_limit,
        generation_limit_watts=_dec(instruction.parameters.get("opModGenLimW")),
        load_limit_watts=_dec(instruction.parameters.get("opModLoadLimW")),
        set_point_percentage=_dec(instruction.parameters.get("opModFixedW")),
        ramp_time_seconds=_dec(instruction.parameters.get("rampTms"), divisor=100),
    )

    url = admin_uri.rstrip("/") + SiteControlUri.format(group_id=group.site_control_group_id)
    body = _site_control_list_adapter.dump_json([request])

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(admin_username, admin_password)) as http_session:
        async with http_session.post(url, data=body, headers={"Content-Type": "application/json"}) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                return ActionResult.failed(
                    f"create-der-control: admin API POST {url} returned HTTP {resp.status}: {text}"
                )

    logger.info(
        "create-der-control: created SiteControl via admin API for site_id=%d start=%s",
        site.site_id,
        start_time,
    )
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
            description=f"cactus-primacy-{primacy}",
            primacy=primacy,
            fsa_id=1,
            changed_time=utc_now(),
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
    await NotificationManager.notify_changed_deleted_entities(session, SubscriptionResource.DEFAULT_SITE_CONTROL, now)
    await session.commit()
    return ActionResult.done()


def _dec(value: float | None, divisor: int = 1) -> Decimal | None:
    return Decimal(str(value)) / divisor if value is not None else None
