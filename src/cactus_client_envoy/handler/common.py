from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.aggregator import Aggregator, AggregatorCertificateAssignment, NULL_AGGREGATOR_ID
from envoy.server.model.base import Certificate
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AdminContext


def resolve_client_config(instruction: AdminInstruction, context: AdminContext) -> ClientConfig:
    if instruction.client is None:
        return next(iter(context.client_configs.values()))
    return context.client_configs[instruction.client]


async def find_aggregator_id(exclude_lfdi: str, context: AdminContext, session: AsyncSession) -> int | None:
    """Find the aggregator_id to assign to a client cert.

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
                .where(Certificate.lfdi == cfg.lfdi.lower())
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
