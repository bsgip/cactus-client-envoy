import logging
import os

from cactus_test_definitions.server.test_procedures import AdminInstruction
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from cactus_client.admin.plugins import hookimpl
from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult, StepExecution

from cactus_client_envoy.handler.end_device import ensure_end_device

ENVOY_DB_DSN_ENV = "ENVOY_DB_DSN"

logger = logging.getLogger(__name__)


class EnvoyAdminPlugin:
    """Admin plugin that fulfils admin instructions against a local Envoy server via direct DB access."""

    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None

    @hookimpl
    async def admin_setup(self, context: AdminContext) -> ActionResult:
        dsn = os.environ.get(ENVOY_DB_DSN_ENV)
        if not dsn:
            return ActionResult.failed(f"{ENVOY_DB_DSN_ENV} environment variable is not set.")
        self._engine = create_async_engine(dsn)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        return ActionResult.done()

    @hookimpl
    async def admin_teardown(self, context: AdminContext) -> ActionResult:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None
        return ActionResult.done()

    @hookimpl
    async def admin_instruction(
        self, instruction: AdminInstruction, step: StepExecution, context: AdminContext
    ) -> ActionResult | None:
        assert self._sessionmaker is not None
        match instruction.type:
            case "ensure-end-device":
                async with self._sessionmaker() as session:
                    return await ensure_end_device(instruction, context, session)
        return None
