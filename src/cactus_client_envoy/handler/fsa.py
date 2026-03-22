import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.notification.manager.notification import NotificationManager
from envoy.server.model.doe import SiteControlGroup
from envoy.server.model.subscription import SubscriptionResource
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


async def ensure_fsa(
    instruction: AdminInstruction, context: AdminContext, fsa_annotations: dict[str, int]
) -> ActionResult:
    """Record a FunctionSetAssignment annotation - primacy mapping for use by ensure-der-program.

    envoy has no FSA table; the fsa_id field on SiteControlGroup is the only representation of FSA grouping. fsa_id is
    set to the FSA primacy, which MUST be unique across FSAs in a test.
    """
    annotation: str | None = instruction.parameters.get("annotation")
    primacy: int = instruction.parameters.get("primacy", 1)

    if annotation:
        fsa_annotations[annotation] = primacy
        logger.info("ensure-fsa: annotation '%s' → fsa_id=%d (primacy)", annotation, primacy)
    else:
        logger.info("ensure-fsa: no annotation provided, fsa_id=%d will be used by default", primacy)
    return ActionResult.done()


async def ensure_der_program(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession, fsa_annotations: dict[str, int]
) -> ActionResult:
    """Ensure a DERProgram (SiteControlGroup) exists within the FSA identified by fsa_annotation.

    Resolves fsa_id from the annotation map populated by ensure-fsa. If the requested primacy matches the FSA primacy
    the same SiteControlGroup serves as both; otherwise a new one is created under the same fsa_id.
    """
    fsa_annotation: str | None = instruction.parameters.get("fsa_annotation")
    primacy: int = instruction.parameters.get("primacy", 1)

    if fsa_annotation is not None:
        fsa_id = fsa_annotations.get(fsa_annotation)
        if fsa_id is None:
            return ActionResult.failed(
                f"ensure-der-program: unknown fsa_annotation '{fsa_annotation}' — run ensure-fsa with annotation first"
            )
    else:
        fsa_id = 1

    group = (
        await session.execute(
            select(SiteControlGroup).where((SiteControlGroup.primacy == primacy) & (SiteControlGroup.fsa_id == fsa_id))
        )
    ).scalar_one_or_none()

    if group is None:
        now = utc_now()
        group = SiteControlGroup(
            description=f"cactus-fsa{fsa_id}-primacy-{primacy}", primacy=primacy, fsa_id=fsa_id, changed_time=now
        )
        session.add(group)
        await session.flush()
        logger.info(
            "ensure-der-program: created SiteControlGroup fsa_id=%d primacy=%d (id=%d)",
            fsa_id,
            primacy,
            group.site_control_group_id,
        )
        await session.commit()
        await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.SITE_CONTROL_GROUP, now)
        await NotificationManager.notify_changed_deleted_entities(SubscriptionResource.FUNCTION_SET_ASSIGNMENTS, now)
    else:
        logger.info(
            "ensure-der-program: SiteControlGroup already exists fsa_id=%d primacy=%d (id=%d)",
            fsa_id,
            primacy,
            group.site_control_group_id,
        )
        await session.commit()
    return ActionResult.done()
