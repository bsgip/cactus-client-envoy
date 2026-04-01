import logging
import os

from cactus_test_definitions.server.test_procedures import AdminInstruction
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

import envoy.notification.handler as _nh
from envoy.notification.handler import STATE_DB_SESSION_MAKER

from cactus_client.admin.plugins import hookimpl
from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult, StepExecution

from cactus_client_envoy.handler.access import set_client_access
from cactus_client_envoy.handler.cleanup import ensure_notification_domain_whitelisted, reset_test_state
from cactus_client_envoy.handler.control import clear_der_controls, ensure_der_control_list
from cactus_client_envoy.handler.der_control import create_default_der_control, create_der_control
from cactus_client_envoy.handler.end_device import ensure_end_device
from cactus_client_envoy.handler.fsa import ensure_der_program, ensure_fsa
from cactus_client_envoy.handler.mup import ensure_mup_list_empty
from cactus_client_envoy.handler.rate import set_poll_rate, set_post_rate

ENVOY_DB_DSN_ENV = "ENVOY_DB_DSN"

logger = logging.getLogger(__name__)


class EnvoyAdminPlugin:
    """Admin plugin that fulfils admin instructions against a local Envoy server via direct DB access."""

    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._broker: InMemoryBroker | None = None
        self._fsa_annotations: dict[str, int] = {}

    @hookimpl
    async def admin_setup(self, context: AdminContext) -> ActionResult:
        dsn = os.environ.get(ENVOY_DB_DSN_ENV)
        if not dsn:
            return ActionResult.failed(f"{ENVOY_DB_DSN_ENV} environment variable is not set.")
        self._engine = create_async_engine(dsn)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        self._fsa_annotations = {}

        self._broker = InMemoryBroker()
        await self._broker.startup()
        setattr(self._broker.state, STATE_DB_SESSION_MAKER, self._sessionmaker)
        _nh._enabled_broker = self._broker

        async with self._sessionmaker() as session:
            await reset_test_state(session)
            if context.server_config.notification_uri:
                await ensure_notification_domain_whitelisted(session, context.server_config.notification_uri)
        return ActionResult.done()

    @hookimpl
    async def admin_teardown(self, context: AdminContext) -> ActionResult:
        if self._broker is not None:
            _nh._enabled_broker = None
            await self._broker.shutdown()
            self._broker = None
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None
        return ActionResult.done()

    @hookimpl
    async def admin_instruction(
        self, instruction: AdminInstruction, step: StepExecution, context: AdminContext
    ) -> ActionResult | None:
        assert self._sessionmaker is not None  # nosec B101

        match instruction.type:
            case "ensure-end-device":
                async with self._sessionmaker() as session:
                    return await ensure_end_device(instruction, context, session)
            case "ensure-mup-list-empty":
                async with self._sessionmaker() as session:
                    return await ensure_mup_list_empty(instruction, context, session)
            case "ensure-fsa":
                return await ensure_fsa(instruction, context, self._fsa_annotations)
            case "ensure-der-program":
                async with self._sessionmaker() as session:
                    return await ensure_der_program(instruction, context, session, self._fsa_annotations)
            case "set-client-access":
                async with self._sessionmaker() as session:
                    return await set_client_access(instruction, context, session)
            case "ensure-der-control-list":
                async with self._sessionmaker() as session:
                    return await ensure_der_control_list(instruction, context, session)
            case "create-der-control":
                async with self._sessionmaker() as session:
                    return await create_der_control(instruction, context, session)
            case "create-default-der-control":
                async with self._sessionmaker() as session:
                    return await create_default_der_control(instruction, context, session)
            case "clear-der-controls":
                async with self._sessionmaker() as session:
                    return await clear_der_controls(instruction, context, session)
            case "set-poll-rate":
                async with self._sessionmaker() as session:
                    return await set_poll_rate(instruction, context, session)
            case "set-post-rate":
                async with self._sessionmaker() as session:
                    return await set_post_rate(instruction, context, session)
        return None
