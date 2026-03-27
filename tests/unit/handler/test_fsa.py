import pytest
from assertical.fake.generator import generate_class_instance

from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client_envoy.handler.fsa import ensure_fsa


@pytest.mark.asyncio
async def test_ensure_fsa_records_annotation():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"annotation": "fsa1", "primacy": 3})
    fsa_annotations: dict[str, int] = {}

    result = await ensure_fsa(instruction, ctx, fsa_annotations)

    assert result.completed
    assert fsa_annotations == {"fsa1": 3}


@pytest.mark.asyncio
async def test_ensure_fsa_default_primacy():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"annotation": "fsa2"})
    fsa_annotations: dict[str, int] = {}

    result = await ensure_fsa(instruction, ctx, fsa_annotations)

    assert result.completed
    assert fsa_annotations == {"fsa2": 1}


@pytest.mark.asyncio
async def test_ensure_fsa_no_annotation_does_not_record():
    ctx = generate_class_instance(AdminContext)
    instruction = AdminInstruction(type="ensure-fsa", parameters={"primacy": 2})
    fsa_annotations: dict[str, int] = {}

    result = await ensure_fsa(instruction, ctx, fsa_annotations)

    assert result.completed
    assert fsa_annotations == {}
