import logging
from collections.abc import Sequence

from cactus_client.model.context import AdminContext
from cactus_client.sep2 import lfdi_from_cert_file
from cactus_client.time import utc_now
from cactus_test_definitions.server.admin_instructions import AdminInstruction
from envoy.server.model import Site
from envoy.server.model.aggregator import NULL_AGGREGATOR_ID, Aggregator, AggregatorCertificateAssignment
from envoy.server.model.base import Certificate
from envoy.server.model.site import SiteGroup, SiteGroupAssignment
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)
CACTUS_DEFAULT_SITE_GROUP_NAME = "cactus-default"


async def find_aggregator_id(exclude_cert_lfdi: str, context: AdminContext, session: AsyncSession) -> int | None:
    """Find the aggregator_id to assign to a client cert.

    Strategy: look at other aggregator-type clients in the context that already have an assignment,
    then fall back to the first non-null aggregator in the DB.
    """
    for cfg in context.client_configs.values():
        cfg_cert_lfdi = lfdi_from_cert_file(cfg.certificate_file)
        if cfg_cert_lfdi == exclude_cert_lfdi:
            continue
        agg_id = (
            await session.execute(
                select(AggregatorCertificateAssignment.aggregator_id)
                .join(Certificate, Certificate.certificate_id == AggregatorCertificateAssignment.certificate_id)
                .where(Certificate.lfdi == cfg_cert_lfdi.lower())
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


async def ensure_default_site_group(session: AsyncSession) -> SiteGroup:
    """Lazily create the shared default SiteGroup all cactus-managed sites are assigned to.

    Mirrors the existing lazy-default-SiteControlGroup pattern (see control.py/fsa.py) — recreated each test
    run since reset_test_state deletes it on admin_setup. Does not commit; caller manages the transaction.
    """
    group = (await session.execute(select(SiteGroup).where(SiteGroup.default_group.is_(True)))).scalar_one_or_none()
    if group is None:
        group = SiteGroup(name=CACTUS_DEFAULT_SITE_GROUP_NAME, default_group=True, changed_time=utc_now())
        session.add(group)
        await session.flush()
    return group


async def site_group_ids_for_site(session: AsyncSession, site_id: int) -> Sequence[int]:
    """Return the site_group_id(s) that site_id is a member of."""
    return (
        (await session.execute(select(SiteGroupAssignment.site_group_id).where(SiteGroupAssignment.site_id == site_id)))
        .scalars()
        .all()
    )


async def get_default_site_group_id(
    instruction: AdminInstruction, context: AdminContext, session: AsyncSession
) -> int | None:
    """Gets the CACTUS default SiteGroup that is accessible to all sites"""

    client_config = context.client_config_for(instruction.client)
    site = (await session.execute(select(Site).where(Site.lfdi == client_config.lfdi))).scalar_one_or_none()
    if site is None:
        logging.error(f"get_default_site_group: no site found for LFDI {client_config.lfdi}")
        return None

    site_group_ids = await site_group_ids_for_site(session, site.site_id)
    if not site_group_ids:
        logging.error(f"get_default_site_group: site_id={site.site_id} has no SiteGroup membership")
        return None

    return site_group_ids[0]
