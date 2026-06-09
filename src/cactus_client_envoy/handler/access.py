import logging

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult
from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.aggregator import AggregatorCertificateAssignment
from envoy.server.model.base import Certificate
from envoy.server.model.site import Site
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult

from cactus_client.sep2 import lfdi_from_cert_file

from cactus_client_envoy.handler.common import find_aggregator_id
from cactus_client_envoy.handler.end_device import delete_site

logger = logging.getLogger(__name__)


async def set_client_access(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> ActionResult:
    granted: bool = instruction.parameters["granted"]
    client_config = context.client_config_for(instruction.client)
    # Certificate/aggregator identity comes from the cert itself; client_config.lfdi is the managed Site LFDI.
    cert_lfdi = lfdi_from_cert_file(client_config.certificate_file)

    if granted:
        cert = (
            await session.execute(select(Certificate).where(Certificate.lfdi == cert_lfdi.lower()))
        ).scalar_one_or_none()
        if cert is None:
            return ActionResult.failed(
                f"set-client-access: no certificate found for cert LFDI {cert_lfdi} — "
                "is the certificate registered in the envoy DB?"
            )

        aggregator_id = await find_aggregator_id(cert_lfdi, context, session)
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
        site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
        if site is None:
            logger.info("set-client-access: no site found for LFDI %s, nothing to remove", client_config.lfdi)
            return ActionResult.done()

        await delete_site(site.site_id, session)
        logger.info("set-client-access: deleted site site_id=%s for LFDI %s", site.site_id, client_config.lfdi)

        # Also clean up any aggregator cert assignment (aggregator-type clients only)
        cert = (
            await session.execute(select(Certificate).where(Certificate.lfdi == cert_lfdi.lower()))
        ).scalar_one_or_none()
        if cert is not None:
            await session.execute(
                delete(AggregatorCertificateAssignment).where(
                    AggregatorCertificateAssignment.certificate_id == cert.certificate_id
                )
            )

        logger.info("set-client-access: revoked access for LFDI %s", client_config.lfdi)

    await session.commit()
    return ActionResult.done()
