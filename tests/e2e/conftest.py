from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

REQUIRED_E2E_ENV_VARS = {
    "E2E_API_BASE_URL": "http://localhost:8000",
    "E2E_WEBHOOK_BASE_URL": "http://localhost:8081",
    "E2E_INTERNAL_WEBHOOK_BASE_URL": "http://webhook-mock:8080",
    "E2E_DATABASE_URL": "postgresql+asyncpg://payment:payment@localhost:5432/payments",
}


def _missing_e2e_env() -> list[str]:
    return [name for name in REQUIRED_E2E_ENV_VARS if not os.getenv(name)]


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config
    missing = _missing_e2e_env()
    if not missing:
        return

    skip_marker = pytest.mark.skip(
        reason=f"missing e2e environment variables: {', '.join(missing)}"
    )
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip_marker)


@pytest.fixture(scope="session")
def e2e_env() -> dict[str, str]:
    return {key: os.environ.get(key, default) for key, default in REQUIRED_E2E_ENV_VARS.items()}


@pytest.fixture(scope="session")
def migrated_database(e2e_env: dict[str, str]) -> Iterator[None]:
    previous_database_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = e2e_env["E2E_DATABASE_URL"]
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "head")
    try:
        yield
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url


@pytest_asyncio.fixture
async def e2e_api_client(
    e2e_env: dict[str, str],
    migrated_database: None,
) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(base_url=e2e_env["E2E_API_BASE_URL"], timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture
async def e2e_webhook_client(e2e_env: dict[str, str]) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(base_url=e2e_env["E2E_WEBHOOK_BASE_URL"], timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture
async def e2e_session_factory(
    e2e_env: dict[str, str],
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine: AsyncEngine = create_async_engine(e2e_env["E2E_DATABASE_URL"], future=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def reset_e2e_state(
    e2e_webhook_client: AsyncClient,
    e2e_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    await e2e_webhook_client.delete("/events")
    async with e2e_session_factory() as session:
        await session.execute(text("TRUNCATE TABLE outbox, payments CASCADE"))
        await session.commit()
    yield
