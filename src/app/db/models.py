from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.domain.enums import Currency, PaymentStatus

JSON_TYPE = sa.JSON().with_variant(JSONB, "postgresql")


def utcnow() -> datetime:
    return datetime.now(UTC)


def enum_values(enum_cls: type[StrEnum]) -> list[str]:
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.Index("ix_payments_status", "status"),
        sa.Index("ix_payments_gateway_claimed_at", "gateway_claimed_at"),
        sa.Index("ix_payments_webhook_claimed_at", "webhook_claimed_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(sa.Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        sa.Enum(
            Currency,
            name="currency_enum",
            values_callable=enum_values,
        ),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    metadata_payload: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSON_TYPE,
        nullable=False,
        default=dict,
    )
    status: Mapped[PaymentStatus] = mapped_column(
        sa.Enum(
            PaymentStatus,
            name="payment_status_enum",
            values_callable=enum_values,
        ),
        nullable=False,
        default=PaymentStatus.PENDING,
    )
    idempotency_key: Mapped[str] = mapped_column(sa.String(255), nullable=False, unique=True)
    request_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(sa.String(2048), nullable=False)
    gateway_claimed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    webhook_attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
    webhook_claimed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(sa.Text())
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    processed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        sa.Index("ix_outbox_unpublished_created_at", "published_at", "created_at"),
        sa.Index("ix_outbox_routing_key", "routing_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), primary_key=True, default=uuid.uuid4)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    routing_key: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, nullable=False)
    headers: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, nullable=False, default=dict)
    attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
    published_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(sa.Text())
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )
