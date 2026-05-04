from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from assertical.fake.generator import generate_class_instance

from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client_envoy.handler.fsa import ensure_der_program, ensure_fsa


def _session_with_existing_group(group_id: int = 1) -> AsyncMock:
    """Return a mock session where the SiteControlGroup already exists."""
    session = AsyncMock()
    mock_group = MagicMock()
    mock_group.site_control_group_id = group_id
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = mock_group
    session.execute.return_value = mock_result
    return session


def _session_without_group() -> AsyncMock:
    """Return a mock session where no SiteControlGroup exists yet."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute.return_value = mock_result
    session.add = MagicMock(side_effect=lambda g: setattr(g, "site_control_group_id", 99))
    return session


@pytest.mark.asyncio
async def test_ensure_fsa_records_annotation():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"annotation": "fsa1", "primacy": 3})
    fsa_annotations: dict[str, int] = {}

    result = await ensure_fsa(instruction, ctx, _session_with_existing_group(), fsa_annotations)

    assert result.completed
    assert fsa_annotations == {"fsa1": 3}


@pytest.mark.asyncio
async def test_ensure_fsa_default_primacy():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"annotation": "fsa2"})
    fsa_annotations: dict[str, int] = {}

    result = await ensure_fsa(instruction, ctx, _session_with_existing_group(), fsa_annotations)

    assert result.completed
    assert fsa_annotations == {"fsa2": 1}


@pytest.mark.asyncio
async def test_ensure_fsa_no_annotation_does_not_record():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"primacy": 2})
    fsa_annotations: dict[str, int] = {}

    result = await ensure_fsa(instruction, ctx, _session_with_existing_group(), fsa_annotations)

    assert result.completed
    assert fsa_annotations == {}


@pytest.mark.asyncio
async def test_ensure_fsa_creates_site_control_group_when_missing():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"annotation": "fsa1", "primacy": 1})
    fsa_annotations: dict[str, int] = {}
    session = _session_without_group()

    with patch("cactus_client_envoy.handler.fsa.NotificationManager.notify_changed_deleted_entities") as mock_notify:
        mock_notify.return_value = None
        result = await ensure_fsa(instruction, ctx, session, fsa_annotations)

    assert result.completed
    assert fsa_annotations == {"fsa1": 1}
    session.add.assert_called_once()
    session.flush.assert_called_once()
    session.commit.assert_called_once()
    mock_notify.assert_called_once()


@pytest.mark.asyncio
async def test_ensure_fsa_skips_create_when_group_exists():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"annotation": "fsa1", "primacy": 1})
    fsa_annotations: dict[str, int] = {}
    session = _session_with_existing_group()

    with patch("cactus_client_envoy.handler.fsa.NotificationManager.notify_changed_deleted_entities") as mock_notify:
        result = await ensure_fsa(instruction, ctx, session, fsa_annotations)

    assert result.completed
    session.add.assert_not_called()
    mock_notify.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_der_program_creates_group_and_notifies():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-der-program", parameters={"fsa_annotation": "fsa1"})
    fsa_annotations = {"fsa1": 1}
    session = _session_without_group()

    with patch("cactus_client_envoy.handler.fsa.NotificationManager.notify_changed_deleted_entities") as mock_notify:
        mock_notify.return_value = None
        result = await ensure_der_program(instruction, ctx, session, fsa_annotations)

    assert result.completed
    session.add.assert_called_once()
    assert mock_notify.call_count == 2


@pytest.mark.asyncio
async def test_ensure_der_program_notifies_even_when_group_exists():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-der-program", parameters={"fsa_annotation": "fsa1"})
    fsa_annotations = {"fsa1": 1}
    session = _session_with_existing_group()

    with patch("cactus_client_envoy.handler.fsa.NotificationManager.notify_changed_deleted_entities") as mock_notify:
        mock_notify.return_value = None
        result = await ensure_der_program(instruction, ctx, session, fsa_annotations)

    assert result.completed
    session.add.assert_not_called()
    assert mock_notify.call_count == 2


@pytest.mark.asyncio
async def test_ensure_der_program_fails_on_unknown_annotation():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-der-program", parameters={"fsa_annotation": "missing"})
    fsa_annotations: dict[str, int] = {}
    session = _session_with_existing_group()

    result = await ensure_der_program(instruction, ctx, session, fsa_annotations)

    assert not result.completed
