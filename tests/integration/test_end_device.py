from datetime import UTC, datetime

import pytest
from assertical.fake.generator import generate_class_instance
from assertical.fixtures.postgres import generate_async_conn_str_from_connection
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction, ClientType
from envoy.server.model.aggregator import AggregatorCertificateAssignment
from envoy.server.model.site import Site
from psycopg import Connection
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from cactus_client_envoy.handler import common as common_handler
from cactus_client_envoy.handler import end_device as end_device_handler
from cactus_client_envoy.handler.end_device import ensure_end_device

DEVICE_LFDI = "aabbccddee001122334455aabbccdd00112233ff"

# For an aggregator the configured `lfdi` is the managed EndDevice LFDI — deliberately distinct from the
# aggregator's certificate LFDI (which only identifies the aggregator in envoy's Certificate table).
AGG_CERT_LFDI = "1122334455667788990011223344556677889900"
AGG_MANAGED_LFDI = "ffeeddccbbaa00998877665544332211ffeeddcc"


@pytest.fixture
def device_client_config() -> ClientConfig:
    return ClientConfig(
        id="device1",
        type=ClientType.DEVICE,
        certificate_file="dummy.crt",
        key_file=None,
        lfdi=DEVICE_LFDI,
        sfdi=123456789,
        pen=28547,
        pin=123455,
        max_watts=5000,
    )


@pytest.fixture
def admin_context(device_client_config: ClientConfig) -> AdminContext:
    return generate_class_instance(AdminContext, client_configs={"device1": device_client_config})


@pytest.mark.asyncio
async def test_ensure_end_device_registers_and_deletes(pg_base_config: Connection, admin_context: AdminContext):
    """Integration: ensure_end_device creates a Site on registered=True, is idempotent, and removes it on
    registered=False."""
    conn_str = generate_async_conn_str_from_connection(pg_base_config)
    engine = create_async_engine(conn_str)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    try:
        register = AdminInstruction(type="ensure-end-device", parameters={"registered": True})
        deregister = AdminInstruction(type="ensure-end-device", parameters={"registered": False})

        # Register — site should be created
        async with session_maker() as session:
            result = await ensure_end_device(register, admin_context, session)
        assert result.completed

        async with session_maker() as session:
            site = (await session.execute(select(Site).where(Site.lfdi == DEVICE_LFDI))).scalar_one_or_none()
        assert site is not None

        # Register again — idempotent, still exactly one site
        async with session_maker() as session:
            result = await ensure_end_device(register, admin_context, session)
        assert result.completed

        async with session_maker() as session:
            sites = (await session.execute(select(Site).where(Site.lfdi == DEVICE_LFDI))).scalars().all()
        assert len(sites) == 1

        # Deregister — site should be removed
        async with session_maker() as session:
            result = await ensure_end_device(deregister, admin_context, session)
        assert result.completed

        async with session_maker() as session:
            site = (await session.execute(select(Site).where(Site.lfdi == DEVICE_LFDI))).scalar_one_or_none()
        assert site is None

    finally:
        await engine.dispose()


@pytest.fixture
def aggregator_client_config() -> ClientConfig:
    return ClientConfig(
        id="aggregator1",
        type=ClientType.AGGREGATOR,
        certificate_file="dummy-aggregator.crt",
        key_file=None,
        lfdi=AGG_MANAGED_LFDI,
        sfdi=987654321,
        pen=28547,
        pin=123455,
        max_watts=5000,
    )


@pytest.fixture
def aggregator_admin_context(aggregator_client_config: ClientConfig) -> AdminContext:
    return generate_class_instance(AdminContext, client_configs={"aggregator1": aggregator_client_config})


@pytest.mark.asyncio
async def test_ensure_end_device_aggregator_uses_cert_lfdi_for_identity_and_managed_lfdi_for_site(
    pg_base_config: Connection,
    aggregator_admin_context: AdminContext,
    monkeypatch: pytest.MonkeyPatch,
):
    """Integration: for an aggregator, the certificate/aggregator is resolved via the *cert* LFDI (derived from
    the cert file) while the registered Site uses the distinct *managed* LFDI from client_config.lfdi.

    This guards the regression where client_config.lfdi was used as both, forcing lfdi == cert LFDI and
    colliding with the aggregator's virtual EndDevice.
    """
    # Avoid needing a real certificate file — the cert identity is derived here. Both modules bind their own
    # reference to lfdi_from_cert_file, so patch both.
    monkeypatch.setattr(end_device_handler, "lfdi_from_cert_file", lambda _cert_file: AGG_CERT_LFDI)
    monkeypatch.setattr(common_handler, "lfdi_from_cert_file", lambda _cert_file: AGG_CERT_LFDI)

    now = datetime(2000, 1, 1, tzinfo=UTC)
    with pg_base_config.cursor() as cur:
        # A real (non-null) aggregator that the cert should be assigned to.
        cur.execute(
            "INSERT INTO aggregator (aggregator_id, name, created_time, changed_time) VALUES (%s, %s, %s, %s)",
            (1, "TEST AGGREGATOR", now, now),
        )
        # The aggregator's certificate, stored under the (lowercased) cert LFDI.
        cur.execute(
            "INSERT INTO certificate (certificate_id, created, lfdi, expiry) VALUES (%s, %s, %s, %s)",
            (1, now, AGG_CERT_LFDI.lower(), datetime(2100, 1, 1, tzinfo=UTC)),
        )
    pg_base_config.commit()

    conn_str = generate_async_conn_str_from_connection(pg_base_config)
    engine = create_async_engine(conn_str)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    try:
        register = AdminInstruction(type="ensure-end-device", parameters={"registered": True})

        async with session_maker() as session:
            result = await ensure_end_device(register, aggregator_admin_context, session)
        assert result.completed

        async with session_maker() as session:
            # Site is registered under the managed LFDI, not the cert LFDI.
            managed_site = (
                await session.execute(select(Site).where(Site.lfdi == AGG_MANAGED_LFDI))
            ).scalar_one_or_none()
            cert_site = (await session.execute(select(Site).where(Site.lfdi == AGG_CERT_LFDI))).scalar_one_or_none()
            assignment = (
                await session.execute(
                    select(AggregatorCertificateAssignment).where(AggregatorCertificateAssignment.certificate_id == 1)
                )
            ).scalar_one_or_none()

        assert managed_site is not None, "Site should be registered under the managed EndDevice LFDI"
        assert managed_site.aggregator_id == 1
        assert cert_site is None, "No Site should be registered under the aggregator's cert LFDI"
        assert assignment is not None and assignment.aggregator_id == 1

    finally:
        await engine.dispose()
