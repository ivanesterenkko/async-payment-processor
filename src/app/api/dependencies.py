from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, cast

from fastapi import Header, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.errors import UnauthorizedError
from app.core.security import validate_idempotency_key


def get_app_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)


async def get_db_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = get_session_factory(request)
    async with session_factory() as session:
        yield session


async def require_api_key(
    request: Request,
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    settings = get_app_settings(request)
    if api_key != settings.api_key:
        raise UnauthorizedError()


async def get_idempotency_key(
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> str:
    if idempotency_key is None:
        from app.core.errors import BadRequestError

        raise BadRequestError(
            "Idempotency-Key header is required.",
            code="missing_idempotency_key",
        )
    return validate_idempotency_key(idempotency_key)
