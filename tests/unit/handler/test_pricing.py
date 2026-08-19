from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client_envoy.handler.pricing import (
    create_rate_component,
    create_time_tariff_interval,
    ensure_tariff_profile,
)


def _result(value):
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = value
    return mock_result


def _session_with_results(*values) -> AsyncMock:
    """Return a mock session whose session.execute() calls yield scalar_one_or_none() results in order."""
    session = AsyncMock()
    session.execute.side_effect = [_result(v) for v in values]
    return session


def _admin_context(lfdi: str = "abc123") -> AdminContext:
    client_config = generate_class_instance(ClientConfig, lfdi=lfdi)
    return generate_class_instance(AdminContext, client_configs={"client": client_config})


@pytest.mark.asyncio
async def test_ensure_tariff_profile_creates_tariff_when_missing():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(
        type="ensure-tariff-profile",
        parameters={"fsa_annotation": "fsa1", "primacy": 1, "currency_code": 36},
    )
    session = _session_with_results(None)
    session.add = MagicMock(side_effect=lambda t: setattr(t, "tariff_id", 5))

    with patch("cactus_client_envoy.handler.pricing.NotificationManager.notify_changed_deleted_entities") as notify:
        notify.return_value = None
        result = await ensure_tariff_profile(instruction, ctx, session, {"fsa1": 1})

    assert result.completed
    session.add.assert_called_once()
    session.flush.assert_called_once()
    notify.assert_called_once()


@pytest.mark.asyncio
async def test_ensure_tariff_profile_skips_when_exists():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(
        type="ensure-tariff-profile",
        parameters={"fsa_annotation": "fsa1", "primacy": 1, "currency_code": 36},
    )
    existing = MagicMock()
    existing.tariff_id = 9
    session = _session_with_results(existing)

    with patch("cactus_client_envoy.handler.pricing.NotificationManager.notify_changed_deleted_entities") as notify:
        result = await ensure_tariff_profile(instruction, ctx, session, {"fsa1": 1})

    assert result.completed
    session.add.assert_not_called()
    notify.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_tariff_profile_fails_unknown_annotation():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(
        type="ensure-tariff-profile",
        parameters={"fsa_annotation": "missing", "primacy": 1, "currency_code": 36},
    )
    session = _session_with_results()

    result = await ensure_tariff_profile(instruction, ctx, session, {})

    assert not result.completed


@pytest.mark.asyncio
async def test_create_rate_component_records_tag():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(
        type="create-rate-component", parameters={"tag": "rc1", "role_flags": 0, "commodity": 2}
    )
    tariff = MagicMock()
    tariff.tariff_id = 1
    session = _session_with_results(tariff)
    session.add = MagicMock(side_effect=lambda c: setattr(c, "tariff_component_id", 42))
    rate_component_tags: dict[str, int] = {}

    with patch("cactus_client_envoy.handler.pricing.NotificationManager.notify_changed_deleted_entities") as notify:
        notify.return_value = None
        result = await create_rate_component(instruction, ctx, session, rate_component_tags)

    assert result.completed
    assert rate_component_tags == {"rc1": 42}
    notify.assert_called_once()


@pytest.mark.asyncio
async def test_create_rate_component_fails_when_no_tariff():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="create-rate-component", parameters={"tag": "rc1"})
    session = _session_with_results(None)

    result = await create_rate_component(instruction, ctx, session, {})

    assert not result.completed


@pytest.mark.asyncio
async def test_create_time_tariff_interval_fails_unknown_tag():
    ctx = _admin_context()
    instruction = AdminInstruction(
        type="create-time-tariff-interval",
        client="client",
        parameters={"rate_component_tag": "missing", "duration_seconds": 10, "price_pow10_encoded": 100},
    )
    session = _session_with_results()

    result = await create_time_tariff_interval(instruction, ctx, session, {})

    assert not result.completed


@pytest.mark.asyncio
async def test_create_time_tariff_interval_fails_when_no_site():
    ctx = _admin_context()
    instruction = AdminInstruction(
        type="create-time-tariff-interval",
        client="client",
        parameters={"rate_component_tag": "rc1", "duration_seconds": 10, "price_pow10_encoded": 100},
    )
    component = MagicMock()
    component.tariff_id = 1
    session = _session_with_results(component, None)

    result = await create_time_tariff_interval(instruction, ctx, session, {"rc1": 42})

    assert not result.completed


@pytest.mark.asyncio
async def test_create_time_tariff_interval_fails_when_no_site_group():
    ctx = _admin_context()
    instruction = AdminInstruction(
        type="create-time-tariff-interval",
        client="client",
        parameters={"rate_component_tag": "rc1", "duration_seconds": 10, "price_pow10_encoded": 100},
    )
    component = MagicMock()
    component.tariff_id = 1
    site = MagicMock()
    site.site_id = 7

    session = _session_with_results(component, site)
    site_group_result = MagicMock()
    site_group_result.scalars.return_value.all.return_value = []
    session.execute.side_effect = list(session.execute.side_effect) + [site_group_result]

    result = await create_time_tariff_interval(instruction, ctx, session, {"rc1": 42})

    assert not result.completed


@pytest.mark.asyncio
async def test_create_time_tariff_interval_success():
    ctx = _admin_context()
    instruction = AdminInstruction(
        type="create-time-tariff-interval",
        client="client",
        parameters={"rate_component_tag": "rc1", "duration_seconds": 10, "price_pow10_encoded": 100},
    )
    component = MagicMock()
    component.tariff_id = 1
    site = MagicMock()
    site.site_id = 7

    session = _session_with_results(component, site)
    site_group_result = MagicMock()
    site_group_result.scalars.return_value.all.return_value = [11]
    session.execute.side_effect = list(session.execute.side_effect) + [site_group_result]
    session.add = MagicMock(side_effect=lambda r: setattr(r, "tariff_generated_rate_id", 99))

    with patch("cactus_client_envoy.handler.pricing.NotificationManager.notify_changed_deleted_entities") as notify:
        notify.return_value = None
        result = await create_time_tariff_interval(instruction, ctx, session, {"rc1": 42})

    assert result.completed
    session.add.assert_called_once()
    assert notify.call_count == 2
