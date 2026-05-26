from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.core.security import validate_public_webhook_url
from app.domain.enums import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    amount: Decimal = Field(..., gt=0, decimal_places=2, max_digits=18)
    currency: Currency
    description: str = Field(..., min_length=1, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        validate_public_webhook_url(str(value))
        return value


class PaymentAcceptedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    webhook_attempts: int
    webhook_delivered_at: datetime | None
    last_error: str | None
    created_at: datetime
    processed_at: datetime | None
    updated_at: datetime


class ErrorDetailResponse(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorDetailResponse
