import pytest
from assertical.fake.generator import generate_class_instance
from unittest.mock import AsyncMock

from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client_envoy.handler.rate import set_poll_rate, set_post_rate


@pytest.mark.asyncio
async def test_set_poll_rate_unsupported_resource_returns_failed():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="set-poll-rate", parameters={"resource": "NotARealResource", "rate_seconds": 5})
    session = AsyncMock()

    result = await set_poll_rate(instruction, ctx, session)

    assert not result.completed
    assert "NotARealResource" in (result.description or "")
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_set_post_rate_unsupported_resource_returns_failed():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(
        type="set-post-rate", parameters={"resource": "NotARealResource", "rate_seconds": 5}
    )
    session = AsyncMock()

    result = await set_post_rate(instruction, ctx, session)

    assert not result.completed
    assert "NotARealResource" in (result.description or "")
    session.execute.assert_not_called()
