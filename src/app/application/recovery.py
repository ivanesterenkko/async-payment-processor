from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.outbox_records import build_outbox_event
from app.core.logging import log_context
from app.db.models import Payment, utcnow
from app.domain.enums import PaymentStatus
from app.messaging.contracts import PaymentCreatedEvent

logger = logging.getLogger(__name__)


class ClaimRecoveryService:
    def __init__(
        self,
        *,
        session: AsyncSession,
        main_routing_key: str,
        batch_size: int,
        gateway_claim_timeout_seconds: float,
        webhook_claim_timeout_seconds: float,
    ) -> None:
        self._session = session
        self._main_routing_key = main_routing_key
        self._batch_size = batch_size
        self._gateway_claim_timeout_seconds = gateway_claim_timeout_seconds
        self._webhook_claim_timeout_seconds = webhook_claim_timeout_seconds

    async def recover_stale_claims(self) -> int:
        now = utcnow()
        gateway_cutoff = now - timedelta(seconds=self._gateway_claim_timeout_seconds)
        webhook_cutoff = now - timedelta(seconds=self._webhook_claim_timeout_seconds)
        query = (
            select(Payment)
            .where(
                or_(
                    and_(
                        Payment.status == PaymentStatus.PENDING,
                        Payment.gateway_claimed_at.is_not(None),
                        Payment.gateway_claimed_at < gateway_cutoff,
                    ),
                    and_(
                        Payment.processed_at.is_not(None),
                        Payment.webhook_delivered_at.is_(None),
                        Payment.webhook_claimed_at.is_not(None),
                        Payment.webhook_claimed_at < webhook_cutoff,
                    ),
                )
            )
            .order_by(Payment.updated_at.asc())
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )
        payments = list((await self._session.execute(query)).scalars())

        for payment in payments:
            recovery_reason = "gateway_claim_expired"
            if payment.status is PaymentStatus.PENDING:
                payment.gateway_claimed_at = None
            else:
                recovery_reason = "webhook_claim_expired"
                payment.webhook_claimed_at = None

            payment.updated_at = now
            event = PaymentCreatedEvent(
                event_id=payment.event_id,
                payment_id=payment.id,
                idempotency_key=payment.idempotency_key,
                created_at=payment.created_at,
                webhook_attempt=payment.webhook_attempts,
            )
            self._session.add(
                build_outbox_event(
                    payment.id,
                    event_type="payment.recovery",
                    routing_key=self._main_routing_key,
                    payload=event.model_dump(mode="json"),
                    headers={
                        "idempotency_key": payment.idempotency_key,
                        "recovery_reason": recovery_reason,
                    },
                    created_at=now,
                )
            )
            logger.warning(
                "Expired processing claim rescheduled.",
                extra=log_context(
                    event_id=payment.event_id,
                    payment_id=payment.id,
                    idempotency_key=payment.idempotency_key,
                    recovery_reason=recovery_reason,
                ),
            )

        await self._session.commit()
        return len(payments)
