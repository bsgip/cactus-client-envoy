import logging
from datetime import timedelta

import aiohttp
from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now
from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model import Tariff, TariffComponent, TariffGeneratedRate
from envoy_schema.admin.schema.base import BatchCreateResponse
from envoy_schema.admin.schema.pricing import TariffComponentRequest, TariffGeneratedRateRequest, TariffRequest
from envoy_schema.admin.schema.uri import (
    TariffComponentCreateUri,
    TariffGeneratedRateCreateUri,
    TariffListUri,
)
from envoy_schema.server.schema.sep2.types import (
    CommodityType,
    CurrencyCode,
    FlowDirectionType,
    KindType,
    RoleFlagsType,
    UomType,
)
from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client_envoy.handler.common import get_default_site_group_id

logger = logging.getLogger(__name__)

DEFAULT_DURATION_SECONDS = 8
DEFAULT_SCHEDULED_OFFSET_SECONDS = 2

CACTUS_DEFAULT_TARIFF_NAME = "cactus-tariff"
CACTUS_DEFAULT_TARIFF_COMPONENT_NAME = "cactus-rate"

_tariff_generated_rate_list_adapter: TypeAdapter[list[TariffGeneratedRateRequest]] = TypeAdapter(
    list[TariffGeneratedRateRequest]
)


async def ensure_tariff_profile(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
    admin_uri: str,
    admin_username: str,
    admin_password: str,
    tp_annotations: dict[str, int],
) -> ActionResult:

    annotation: str | None = instruction.parameters.get("annotation", None)
    primacy: int = instruction.parameters.get("primacy", 1)

    # Find candidate Tariffs
    matching_tariff = (
        await session.execute(
            select(Tariff)
            .where(Tariff.primacy == primacy)
            .where(Tariff.required_site_group_id.is_(None))
            .order_by(Tariff.tariff_id)
            .limit(1)
        )
    ).scalar_one_or_none()

    # If we have an existing candidate - use that
    if matching_tariff is not None:
        logger.info(
            "ensure-tariff-profile: Tariff primacy=%d (id=%d) already exists",
            primacy,
            matching_tariff.tariff_id,
        )
        if annotation is not None:
            tp_annotations[annotation] = matching_tariff.tariff_id
        return ActionResult.done()

    # Otherwise - create one
    url = admin_uri.rstrip("/") + TariffListUri
    request = TariffRequest(
        name=CACTUS_DEFAULT_TARIFF_NAME,
        dnsp_code="",
        currency_code=int(CurrencyCode.AUSTRALIAN_DOLLAR),
        price_power_of_ten_multiplier=0,
        primacy=primacy,
    )
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(admin_username, admin_password)) as http_session:
        async with http_session.post(
            url, data=request.model_dump_json(), headers={"Content-Type": "application/json"}
        ) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                return ActionResult.failed(
                    f"ensure-tariff-profile: admin API POST {url} returned HTTP {resp.status}: {text}"
                )
            batch_create = BatchCreateResponse.model_validate_json(await resp.read())
            created_id = batch_create.ids[0]
            logger.info(
                "ensure-tariff-profile: created Tariff primacy=%d (id=%d)",
                primacy,
                created_id,
            )
            if annotation is not None:
                tp_annotations[annotation] = created_id

            return ActionResult.done()


async def ensure_rate_component(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
    admin_uri: str,
    admin_username: str,
    admin_password: str,
    tp_annotations: dict[str, int],
    rc_annotations: dict[str, int],
) -> ActionResult:
    annotation: str | None = instruction.parameters.get("annotation", None)
    tariff_profile_annotation: str | None = instruction.parameters.get("tariff_profile_annotation", None)
    flow_direction: int = instruction.parameters.get("flow_direction", int(FlowDirectionType.FORWARD))

    # Find parent Tariff ID
    if tariff_profile_annotation is not None:
        tariff_id = tp_annotations.get(tariff_profile_annotation)
        if tariff_id is None:
            return ActionResult.failed(
                f"ensure-rate-component: unknown tariff_profile_annotation '{tariff_profile_annotation}'"
                + " - run ensure-tariff-profile with annotation first"
            )
    else:
        # If there is no specific tariff mentioned - pick the "first" one
        first_matching_tariff = (
            await session.execute(
                select(Tariff).where(Tariff.required_site_group_id.is_(None)).order_by(Tariff.tariff_id).limit(1)
            )
        ).scalar_one_or_none()
        if first_matching_tariff is None:
            return ActionResult.failed(
                "ensure-rate-component: couldn't find a VISIBLE Tariff - run ensure-tariff-profile first"
            )
        tariff_id = first_matching_tariff.tariff_id

    # Find candidate RateComponents
    matching_rc = (
        await session.execute(
            select(TariffComponent)
            .where(TariffComponent.tariff_id == tariff_id)
            .where(TariffComponent.flow_direction == flow_direction)
            .order_by(TariffComponent.tariff_component_id)
            .limit(1)
        )
    ).scalar_one_or_none()

    # If we have an existing candidate - use that
    if matching_rc is not None:
        logger.info(
            "ensure-rate-component: TariffComponent tariff_id=%d flow_direction=%d (id=%d) already exists",
            tariff_id,
            flow_direction,
            matching_rc.tariff_component_id,
        )
        if annotation is not None:
            rc_annotations[annotation] = matching_rc.tariff_component_id
        return ActionResult.done()

    # Otherwise - create one
    url = admin_uri.rstrip("/") + TariffComponentCreateUri
    request = TariffComponentRequest(
        tariff_id=tariff_id,
        role_flags=int(RoleFlagsType.NONE),
        description=CACTUS_DEFAULT_TARIFF_COMPONENT_NAME,
        flow_direction=flow_direction,
        power_of_ten_multiplier=0,
        uom=int(UomType.REAL_ENERGY_WATT_HOURS),
        commodity=int(CommodityType.ELECTRICITY_PRIMARY_METERED_VALUE),
        kind=int(KindType.ENERGY),
    )
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(admin_username, admin_password)) as http_session:
        async with http_session.post(
            url, data=request.model_dump_json(), headers={"Content-Type": "application/json"}
        ) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                return ActionResult.failed(
                    f"ensure-rate-component: admin API POST {url} returned HTTP {resp.status}: {text}"
                )
            batch_create = BatchCreateResponse.model_validate_json(await resp.read())
            created_id = batch_create.ids[0]
            logger.info(
                "ensure-rate-component: created RateComponent flow_direction=%d (id=%d)",
                flow_direction,
                created_id,
            )
            if annotation is not None:
                rc_annotations[annotation] = created_id

            return ActionResult.done()


async def ensure_time_tariff_interval(
    instruction: AdminInstruction,
    context: AdminContext,
    session: AsyncSession,
    admin_uri: str,
    admin_username: str,
    admin_password: str,
    rc_annotations: dict[str, int],
) -> ActionResult:
    rate_component_annotation: str | None = instruction.parameters.get("rate_component_annotation", None)
    status: str = instruction.parameters.get("status", "scheduled")  # "active" or "scheduled"
    price: int | None = instruction.parameters.get("price", None)
    now = utc_now()

    # Find parent RateComponent ID
    if rate_component_annotation is not None:
        tc_id = rc_annotations.get(rate_component_annotation)
        if tc_id is None:
            return ActionResult.failed(
                f"ensure-time-tariff-interval: unknown rate_component_annotation '{rate_component_annotation}'"
                + " - run ensure-rate-component with annotation first"
            )
    else:
        # If there is no specific tariff mentioned - pick the "first" one
        first_matching_rc = (
            await session.execute(select(TariffComponent).order_by(TariffComponent.tariff_component_id).limit(1))
        ).scalar_one_or_none()
        if first_matching_rc is None:
            return ActionResult.failed(
                "ensure-time-tariff-interval: couldn't find a TariffComponent - run ensure-rate-component first"
            )
        tc_id = first_matching_rc.tariff_component_id

    # Now look for candidate TimeTariffInterval
    stmt = (
        select(TariffGeneratedRate)
        .where(TariffGeneratedRate.tariff_component_id == tc_id)
        .order_by(TariffGeneratedRate.tariff_generated_rate_id)
        .limit(1)
    )
    if price is not None:
        stmt = stmt.where(TariffGeneratedRate.price_pow10_encoded == price)
    if status == "active":
        # TTI must be active now
        stmt = stmt.where(TariffGeneratedRate.start_time <= now).where(TariffGeneratedRate.end_time > now)
    else:
        # TTI must be in the future (uses a 1 second buffer)
        stmt = stmt.where(TariffGeneratedRate.start_time > (now + timedelta(seconds=1)))
    matching_price = (await session.execute(stmt)).scalar_one_or_none()

    # If we have an existing candidate - use that
    if matching_price is not None:
        logger.info(
            "ensure-time-tariff-interval: TariffGeneratedRate tariff_component_id=%d price=%d (id=%d) already exists",
            tc_id,
            matching_price.price_pow10_encoded,
            matching_price.tariff_generated_rate_id,
        )
        return ActionResult.done()

    # Otherwise we are creating a price record
    if status == "active":
        # Start in the past so the control is currently active
        start_time = now - timedelta(seconds=1)
    else:
        # start in the future
        start_time = now + timedelta(seconds=DEFAULT_SCHEDULED_OFFSET_SECONDS)

    # Look up site group
    site_group_id = await get_default_site_group_id(instruction, context, session)
    if site_group_id is None:
        return ActionResult.failed(
            "ensure-time-tariff-interval: Couldn't resolve SiteGroup membership — run ensure-end-device first"
        )

    # Otherwise - create one
    url = admin_uri.rstrip("/") + TariffGeneratedRateCreateUri
    request = TariffGeneratedRateRequest(
        tariff_component_id=tc_id,
        site_group_id=site_group_id,
        calculation_log_id=None,
        start_time=start_time,
        duration_seconds=DEFAULT_DURATION_SECONDS,
        price_pow10_encoded=1 if price is None else price,
    )

    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(admin_username, admin_password)) as http_session:
        async with http_session.post(
            url,
            data=_tariff_generated_rate_list_adapter.dump_json([request]),
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                return ActionResult.failed(
                    f"ensure-time-tariff-interval: admin API POST {url} returned HTTP {resp.status}: {text}"
                )
            batch_create = BatchCreateResponse.model_validate_json(await resp.read())
            created_id = batch_create.ids[0]
            logger.info(
                "ensure-time-tariff-interval: created TariffGeneratedRate status=%s price=%d tc=%d (id=%d)",
                status,
                request.price_pow10_encoded,
                tc_id,
                created_id,
            )

            return ActionResult.done()
