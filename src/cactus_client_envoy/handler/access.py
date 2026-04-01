import logging

from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.aggregator import AggregatorCertificateAssignment
from envoy.server.model.base import Certificate
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult

from cactus_client_envoy.handler.common import find_aggregator_id

logger = logging.getLogger(__name__)


async def set_client_access(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    granted: bool = instruction.parameters["granted"]
    client_config = context.client_config_for(instruction.client)

    cert = (
        await session.execute(select(Certificate).where(Certificate.lfdi == client_config.lfdi.lower()))
    ).scalar_one_or_none()
    if cert is None:
        return ActionResult.failed(
            f"set-client-access: no certificate found for LFDI {client_config.lfdi} — "
            "is the certificate registered in the envoy DB?"
        )

    if granted:
        aggregator_id = await find_aggregator_id(client_config.lfdi, context, session)
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
