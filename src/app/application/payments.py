from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.outbox_records import build_outbox_event
from app.core.errors import ConflictError
from app.core.logging import log_context
from app.db.models import Payment, utcnow
from app.domain.enums import PaymentStatus
from app.messaging.contracts import PaymentCreatedEvent
from app.schemas.payments import PaymentCreateRequest, PaymentDetailResponse

logger = logging.getLogger(__name__)

@dataclass(slots=True)
class CreatePaymentResult:
    payment: Payment
    was_replayed: bool


def build_request_hash(payload: PaymentCreateRequest) -> str:
    normalized = {
        "amount": format(payload.amount, "f"),
        "currency": payload.currency.value,
        "description": payload.description,
        "metadata": payload.metadata,
        "webhook_url": str(payload.webhook_url),
    }
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def to_payment_detail_response(payment: Payment) -> PaymentDetailResponse:
    return PaymentDetailResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_payload,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=str(payment.webhook_url),
        webhook_attempts=payment.webhook_attempts,
        webhook_delivered_at=payment.webhook_delivered_at,
        last_error=payment.last_error,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        updated_at=payment.updated_at,
    )


class PaymentService:
    def __init__(self, session: AsyncSession, *, main_routing_key: str = "payments.new") -> None:
        self._session = session
        self._main_routing_key = main_routing_key

    async def create_payment(
        self,
        payload: PaymentCreateRequest,
        idempotency_key: str,
    ) -> CreatePaymentResult:
        request_hash = build_request_hash(payload)
        existing = await self._get_by_idempotency_key(idempotency_key)
        if existing is not None:
            self._ensure_same_payload(existing, request_hash)
            return CreatePaymentResult(payment=existing, was_replayed=True)

        created_at = utcnow()
        payment_id = uuid.uuid4()
        event_id = uuid.uuid4()
        payment = Payment(
            id=payment_id,
            event_id=event_id,
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
            metadata_payload=payload.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=str(payload.webhook_url),
            created_at=created_at,
            updated_at=created_at,
        )

        event = PaymentCreatedEvent(
            event_id=event_id,
            payment_id=payment_id,
            idempotency_key=idempotency_key,
            created_at=created_at,
            webhook_attempt=0,
        )
        outbox_event = build_outbox_event(
            payment_id,
            event_type="payment.created",
            routing_key=self._main_routing_key,
            payload=event.model_dump(mode="json"),
            headers={"idempotency_key": idempotency_key},
            created_at=created_at,
        )

        self._session.add(payment)
        self._session.add(outbox_event)

        try:
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            existing = await self._get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            self._ensure_same_payload(existing, request_hash)
            logger.info(
                "Payment creation replayed after unique constraint race.",
                extra=log_context(
                    payment_id=existing.id,
                    idempotency_key=idempotency_key,
                ),
            )
            return CreatePaymentResult(payment=existing, was_replayed=True)

        await self._session.refresh(payment)
        logger.info(
            "Payment created.",
            extra=log_context(
                payment_id=payment.id,
                idempotency_key=idempotency_key,
                status=payment.status,
            ),
        )
        return CreatePaymentResult(payment=payment, was_replayed=False)

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def _get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        query = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self._session.execute(query)
        return result.scalar_one_or_none()

    @staticmethod
    def _ensure_same_payload(payment: Payment, request_hash: str) -> None:
        if payment.request_hash != request_hash:
            raise ConflictError(
                "Idempotency key is already bound to another payload.",
                code="idempotency_conflict",
                details={"idempotency_key": payment.idempotency_key},
            )
