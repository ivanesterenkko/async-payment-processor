from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AppError(Exception):
    message: str
    status_code: int
    code: str
    details: dict[str, Any] = field(default_factory=dict)


class BadRequestError(AppError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "bad_request",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message=message,
            status_code=400,
            code=code,
            details=details or {},
        )


class UnauthorizedError(AppError):
    def __init__(self, message: str = "Invalid API key.") -> None:
        super().__init__(
            message=message,
            status_code=401,
            code="unauthorized",
        )


class NotFoundError(AppError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "not_found",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message=message,
            status_code=404,
            code=code,
            details=details or {},
        )


class ConflictError(AppError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "conflict",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message=message,
            status_code=409,
            code=code,
            details=details or {},
        )
