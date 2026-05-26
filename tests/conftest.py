from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic_settings import SettingsConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.db import models  # noqa: F401
from app.db.base import Base
from app.main import create_app


class TestSettings(Settings):
    model_config = SettingsConfigDict(env_file=None, case_sensitive=False)


@pytest_asyncio.fixture
async def session_factory(tmp_path: Path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    database_path = tmp_path / "test.sqlite3"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}", future=True)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def api_client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    settings = TestSettings(
        api_key="test-api-key",
        database_url="sqlite+aiosqlite:///unused.sqlite3",
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
    )
    app = create_app(settings=settings, session_factory=session_factory)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        yield client
