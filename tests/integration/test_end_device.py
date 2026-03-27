import pytest
from assertical.fake.generator import generate_class_instance
from assertical.fixtures.postgres import generate_async_conn_str_from_connection
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction, ClientType
from envoy.server.model.site import Site
from psycopg import Connection
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from cactus_client_envoy.handler.end_device import ensure_end_device

DEVICE_LFDI = "aabbccddee001122334455aabbccdd00112233ff"


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
        pin=123450,
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
