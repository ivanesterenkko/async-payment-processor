from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings


def build_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def build_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(build_engine(database_url), expire_on_commit=False)


@lru_cache
def get_engine() -> AsyncEngine:
    return build_engine(get_settings().database_url)


@lru_cache
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


async def session_scope(
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> AsyncIterator[AsyncSession]:
    factory = session_factory or get_session_factory()
    async with factory() as session:
        yield session
