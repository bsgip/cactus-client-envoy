import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.aggregator import Aggregator, AggregatorCertificateAssignment, NULL_AGGREGATOR_ID
from envoy.server.model.base import Certificate
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult

from cactus_client_envoy.handler.common import resolve_client_config

logger = logging.getLogger(__name__)


async def set_client_access(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    granted: bool = instruction.parameters["granted"]
    client_config = resolve_client_config(instruction, context)

    cert = (
        await session.execute(select(Certificate).where(Certificate.lfdi == client_config.lfdi))
    ).scalar_one_or_none()
    if cert is None:
        return ActionResult.failed(
            f"set-client-access: no certificate found for LFDI {client_config.lfdi} — "
            "is the certificate registered in the envoy DB?"
        )

    if granted:
        aggregator_id = await _find_aggregator_id(client_config.lfdi, context, session)
        if aggregator_id is None:
            return ActionResult.failed(
                "set-client-access: cannot determine which aggregator to grant access to — "
                "ensure another aggregator client is already registered or an aggregator exists in the DB"
            )

        existing = (
            await session.execute(
                select(AggregatorCertificateAssignment).where(
                    (AggregatorCertificateAssignment.certificate_id == cert.certificate_id)
                    & (AggregatorCertificateAssignment.aggregator_id == aggregator_id)
                )
            )
        ).scalar_one_or_none()

        if existing is None:
            session.add(
                AggregatorCertificateAssignment(certificate_id=cert.certificate_id, aggregator_id=aggregator_id)
            )
            logger.info(
                "set-client-access: granted access for LFDI %s to aggregator_id=%d", client_config.lfdi, aggregator_id
            )
        else:
            logger.info(
                "set-client-access: access already granted for LFDI %s to aggregator_id=%d",
                client_config.lfdi,
                aggregator_id,
            )
    else:
        await session.execute(
            delete(AggregatorCertificateAssignment).where(
                AggregatorCertificateAssignment.certificate_id == cert.certificate_id
            )
        )
        logger.info("set-client-access: revoked access for LFDI %s", client_config.lfdi)

    await session.commit()
    return ActionResult.done()


async def _find_aggregator_id(exclude_lfdi: str, context: AdminContext, session: AsyncSession) -> int | None:
    """Find the aggregator_id to use for granting access.

    Strategy: look at other aggregator-type clients in the context that already have an assignment,
    then fall back to the first non-null aggregator in the DB.
    """

    for cfg in context.client_configs.values():
        if cfg.lfdi == exclude_lfdi:
            continue
        agg_id = (
            await session.execute(
                select(AggregatorCertificateAssignment.aggregator_id)
                .join(Certificate, Certificate.certificate_id == AggregatorCertificateAssignment.certificate_id)
                .where(Certificate.lfdi == cfg.lfdi)
                .limit(1)
            )
        ).scalar_one_or_none()
        if agg_id is not None:
            return agg_id

    # Fall back: first real aggregator in DB
    return (
        await session.execute(
            select(Aggregator.aggregator_id).where(Aggregator.aggregator_id != NULL_AGGREGATOR_ID).limit(1)
        )
    ).scalar_one_or_none()
