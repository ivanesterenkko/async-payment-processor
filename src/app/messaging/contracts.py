from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from app.domain.enums import Currency, PaymentStatus


class PaymentCreatedEvent(BaseModel):
    event_id: UUID
    payment_id: UUID
    idempotency_key: str
    created_at: datetime
    webhook_attempt: int = 0


class WebhookNotification(BaseModel):
    event_id: UUID
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    processed_at: datetime
    webhook_attempt: int
    webhook_url: str
