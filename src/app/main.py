from __future__ import annotations

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.dependencies import require_api_key
from app.api.payments import router as payments_router
from app.core.config import Settings, get_settings
from app.core.errors import AppError
from app.core.logging import configure_logging
from app.db.session import build_session_factory


def build_health_router() -> APIRouter:
    router = APIRouter()

    @router.get("/health", tags=["health"])
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    return router


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    app_settings = settings or get_settings()
    configure_logging(app_settings.log_level)

    app = FastAPI(title=app_settings.app_name)
    app.state.settings = app_settings
    app.state.session_factory = session_factory or build_session_factory(app_settings.database_url)

    @app.exception_handler(AppError)
    async def handle_app_error(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @app.exception_handler(HTTPException)
    async def handle_http_error(_: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": "http_error",
                    "message": str(exc.detail),
                    "details": {},
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed.",
                    "details": {"errors": exc.errors()},
                }
            },
        )

    protected_router = APIRouter(
        prefix="/api/v1",
        dependencies=[Depends(require_api_key)],
    )
    protected_router.include_router(payments_router)

    app.include_router(build_health_router())
    app.include_router(protected_router)
    return app
