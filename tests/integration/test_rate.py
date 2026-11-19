import pytest
from assertical.fake.generator import generate_class_instance
from assertical.fixtures.postgres import generate_async_conn_str_from_connection
from cactus_client.model.context import AdminContext
from cactus_test_definitions.server.test_procedures import AdminInstruction
from envoy.server.model.server import RuntimeServerConfig
from psycopg import Connection
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from cactus_client_envoy.handler.cleanup import reset_test_state
from cactus_client_envoy.handler.rate import set_poll_rate, set_post_rate


@pytest.mark.asyncio
async def test_reset_test_state_resets_rates(pg_base_config: Connection):
    """Rates set by one test run must not leak into the next run, but unrelated RuntimeServerConfig fields are left"""
    engine = create_async_engine(generate_async_conn_str_from_connection(pg_base_config))
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    ctx = generate_class_instance(AdminContext)

    try:
        async with session_maker() as session:
            await set_post_rate(
                AdminInstruction(
                    type="set-post-rate", parameters={"resource": "MirrorUsagePoint", "rate_seconds": 300}
                ),
                ctx,
                session,
            )
            await set_poll_rate(
                AdminInstruction(
                    type="set-poll-rate", parameters={"resource": "DeviceCapability", "rate_seconds": 120}
                ),
                ctx,
                session,
            )
            config = (await session.execute(select(RuntimeServerConfig))).scalar_one()
            config.disable_edev_registration = True
            await session.commit()

        async with session_maker() as session:
            await reset_test_state(session)

        async with session_maker() as session:
            config = (await session.execute(select(RuntimeServerConfig))).scalar_one()
            assert config.mup_postrate_seconds is None
            assert config.dcap_pollrate_seconds is None
            assert config.disable_edev_registration is True

            # No config row at all is fine (fresh envoy DB)
            await session.delete(config)
            await session.commit()
            await reset_test_state(session)
            assert (await session.execute(select(RuntimeServerConfig))).scalar_one_or_none() is None
    finally:
        await engine.dispose()
