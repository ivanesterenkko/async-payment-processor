from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import or_, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.interfaces import PaymentGateway, WebhookSender
from app.application.outbox_records import build_outbox_event
from app.application.webhooks import WebhookDeliveryError
from app.core.logging import log_context
from app.db.models import Payment, utcnow
from app.domain.enums import PaymentStatus
from app.messaging.contracts import PaymentCreatedEvent, WebhookNotification

logger = logging.getLogger(__name__)


class PaymentEventProcessor:
    def __init__(
        self,
        session: AsyncSession,
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        *,
        max_delivery_attempts: int,
        processing_retry_routing_key: str,
        retry_routing_keys: tuple[str, ...],
        dlq_routing_key: str,
        gateway_claim_timeout_seconds: float,
        webhook_claim_timeout_seconds: float,
    ) -> None:
        self._session = session
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._max_delivery_attempts = max_delivery_attempts
        self._processing_retry_routing_key = processing_retry_routing_key
        self._retry_routing_keys = retry_routing_keys
        self._dlq_routing_key = dlq_routing_key
        self._gateway_claim_timeout_seconds = gateway_claim_timeout_seconds
        self._webhook_claim_timeout_seconds = webhook_claim_timeout_seconds

    async def process(self, event: PaymentCreatedEvent) -> None:
        logger.info(
            "Payment event received.",
            extra=log_context(
                event_id=event.event_id,
                payment_id=event.payment_id,
                idempotency_key=event.idempotency_key,
                webhook_attempt=event.webhook_attempt,
            ),
        )

        payment = await self._fetch_payment(event.payment_id)
        if payment is None:
            logger.warning(
                "Payment event skipped because payment was not found.",
                extra=log_context(
                    event_id=event.event_id,
                    payment_id=event.payment_id,
                    idempotency_key=event.idempotency_key,
                ),
            )
            return

        if payment.status is PaymentStatus.PENDING:
            gateway_claimed_at = await self._claim_gateway(event.payment_id)
            if gateway_claimed_at is not None:
                payment = await self._fetch_payment(event.payment_id)
                if payment is None:
                    return
                try:
                    payment_result = await self._gateway.process(payment)
                except Exception as exc:
                    await self._record_gateway_failure(event, gateway_claimed_at, exc)
                    return
                await self._finalize_gateway(event, gateway_claimed_at, payment_result)
                payment = await self._fetch_payment(event.payment_id)
            else:
                payment = await self._fetch_payment(event.payment_id)

        if (
            payment is None
            or payment.status is PaymentStatus.PENDING
            or payment.processed_at is None
        ):
            return

        if payment.webhook_delivered_at is not None:
            logger.info(
                "Webhook delivery already completed for payment event.",
                extra=log_context(
                    event_id=event.event_id,
                    payment_id=event.payment_id,
                    idempotency_key=event.idempotency_key,
                    webhook_attempt=payment.webhook_attempts,
                ),
            )
            return

        webhook_claimed_at = await self._claim_webhook(payment, event)
        if webhook_claimed_at is None:
            return

        payment = await self._fetch_payment(event.payment_id)
        if payment is None or payment.processed_at is None:
            return

        attempt_number = event.webhook_attempt + 1
        notification = self._build_webhook_notification(payment, event, attempt_number)

        try:
            await self._webhook_sender.send(notification)
        except WebhookDeliveryError as exc:
            await self._record_webhook_failure(
                payment=payment,
                event=event,
                claimed_at=webhook_claimed_at,
                attempt_number=attempt_number,
                error=exc,
            )
            return

        await self._record_webhook_success(
            payment=payment,
            event=event,
            claimed_at=webhook_claimed_at,
            attempt_number=attempt_number,
        )

    async def _fetch_payment(self, payment_id: object) -> Payment | None:
        return await self._session.get(Payment, payment_id, populate_existing=True)

    async def _claim_gateway(self, payment_id: object) -> datetime | None:
        now = utcnow()
        claim_cutoff = now - timedelta(seconds=self._gateway_claim_timeout_seconds)
        statement = (
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.status == PaymentStatus.PENDING,
                or_(
                    Payment.gateway_claimed_at.is_(None),
                    Payment.gateway_claimed_at < claim_cutoff,
                ),
            )
            .values(gateway_claimed_at=now, updated_at=now, last_error=None)
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        await self._session.commit()
        claimed = (result.rowcount or 0) > 0
        if claimed:
            logger.info(
                "Gateway claim acquired.",
                extra=log_context(payment_id=payment_id),
            )
        return now if claimed else None

    async def _record_gateway_failure(
        self,
        event: PaymentCreatedEvent,
        claimed_at: datetime,
        error: Exception,
    ) -> None:
        now = utcnow()
        statement = (
            update(Payment)
            .where(
                Payment.id == event.payment_id,
                Payment.status == PaymentStatus.PENDING,
                Payment.gateway_claimed_at == claimed_at,
            )
            .values(
                gateway_claimed_at=None,
                last_error=f"Gateway processing failed: {error}",
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        if (result.rowcount or 0) == 0:
            await self._session.commit()
            logger.info(
                "Stale gateway failure ignored.",
                extra=log_context(event_id=event.event_id, payment_id=event.payment_id),
            )
            return
        self._session.add(
            build_outbox_event(
                event.payment_id,
                event_type="payment.gateway.retry",
                routing_key=self._processing_retry_routing_key,
                payload=event.model_dump(mode="json"),
                headers={"idempotency_key": event.idempotency_key},
                created_at=now,
            )
        )
        await self._session.commit()
        logger.warning(
            "Gateway processing failed.",
            extra=log_context(
                event_id=event.event_id,
                payment_id=event.payment_id,
                idempotency_key=event.idempotency_key,
                error=str(error),
                routing_key=self._processing_retry_routing_key,
            ),
        )

    async def _finalize_gateway(
        self,
        event: PaymentCreatedEvent,
        claimed_at: datetime,
        status: PaymentStatus,
    ) -> None:
        now = utcnow()
        statement = (
            update(Payment)
            .where(
                Payment.id == event.payment_id,
                Payment.status == PaymentStatus.PENDING,
                Payment.gateway_claimed_at == claimed_at,
            )
            .values(
                status=status,
                processed_at=now,
                gateway_claimed_at=None,
                updated_at=now,
                last_error=None,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        await self._session.commit()
        if (result.rowcount or 0) == 0:
            logger.info(
                "Stale gateway result ignored.",
                extra=log_context(event_id=event.event_id, payment_id=event.payment_id),
            )
            return
        logger.info(
            "Gateway processing finished.",
            extra=log_context(
                event_id=event.event_id,
                payment_id=event.payment_id,
                idempotency_key=event.idempotency_key,
                status=status,
            ),
        )

    async def _claim_webhook(
        self,
        payment: Payment,
        event: PaymentCreatedEvent,
    ) -> datetime | None:
        now = utcnow()
        claim_cutoff = now - timedelta(seconds=self._webhook_claim_timeout_seconds)
        statement = (
            update(Payment)
            .where(
                Payment.id == payment.id,
                Payment.processed_at.is_not(None),
                Payment.webhook_delivered_at.is_(None),
                Payment.webhook_attempts == event.webhook_attempt,
                or_(
                    Payment.webhook_claimed_at.is_(None),
                    Payment.webhook_claimed_at < claim_cutoff,
                ),
            )
            .values(webhook_claimed_at=now, updated_at=now)
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        await self._session.commit()
        claimed = (result.rowcount or 0) > 0
        if claimed:
            logger.info(
                "Webhook claim acquired.",
                extra=log_context(
                    event_id=event.event_id,
                    payment_id=event.payment_id,
                    idempotency_key=event.idempotency_key,
                    webhook_attempt=event.webhook_attempt,
                ),
            )
        return now if claimed else None

    async def _record_webhook_success(
        self,
        *,
        payment: Payment,
        event: PaymentCreatedEvent,
        claimed_at: datetime,
        attempt_number: int,
    ) -> None:
        now = utcnow()
        statement = (
            update(Payment)
            .where(
                Payment.id == payment.id,
                Payment.webhook_attempts == event.webhook_attempt,
                Payment.webhook_delivered_at.is_(None),
                Payment.webhook_claimed_at == claimed_at,
            )
            .values(
                webhook_attempts=attempt_number,
                webhook_claimed_at=None,
                webhook_delivered_at=now,
                last_error=None,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        await self._session.commit()
        if (result.rowcount or 0) == 0:
            logger.info(
                "Stale webhook success ignored.",
                extra=log_context(event_id=event.event_id, payment_id=event.payment_id),
            )
            return
        logger.info(
            "Webhook delivered successfully.",
            extra=log_context(
                event_id=event.event_id,
                payment_id=event.payment_id,
                idempotency_key=event.idempotency_key,
                webhook_attempt=attempt_number,
            ),
        )

    async def _record_webhook_failure(
        self,
        *,
        payment: Payment,
        event: PaymentCreatedEvent,
        claimed_at: datetime,
        attempt_number: int,
        error: WebhookDeliveryError,
    ) -> None:
        routing_key, event_type = self._next_routing(error=error, attempt_number=attempt_number)
        follow_up_event = event.model_copy(update={"webhook_attempt": attempt_number})
        now = utcnow()

        statement = (
            update(Payment)
            .where(
                Payment.id == payment.id,
                Payment.webhook_attempts == event.webhook_attempt,
                Payment.webhook_delivered_at.is_(None),
                Payment.webhook_claimed_at == claimed_at,
            )
            .values(
                webhook_attempts=attempt_number,
                webhook_claimed_at=None,
                last_error=str(error),
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        if (result.rowcount or 0) > 0:
            self._session.add(
                build_outbox_event(
                    payment.id,
                    event_type=event_type,
                    routing_key=routing_key,
                    payload=follow_up_event.model_dump(mode="json"),
                    headers={
                        "idempotency_key": event.idempotency_key,
                        "retryable": error.retryable,
                        "status_code": error.status_code,
                    },
                    created_at=now,
                )
            )
        await self._session.commit()
        if (result.rowcount or 0) == 0:
            logger.info(
                "Stale webhook failure ignored.",
                extra=log_context(event_id=event.event_id, payment_id=event.payment_id),
            )
            return
        logger.warning(
            "Webhook delivery failed.",
            extra=log_context(
                event_id=event.event_id,
                payment_id=event.payment_id,
                idempotency_key=event.idempotency_key,
                webhook_attempt=attempt_number,
                retryable=error.retryable,
                status_code=error.status_code,
                routing_key=routing_key,
            ),
        )

    def _build_webhook_notification(
        self,
        payment: Payment,
        event: PaymentCreatedEvent,
        attempt_number: int,
    ) -> WebhookNotification:
        if payment.processed_at is None:
            raise RuntimeError(
                "Processed payment must include processed_at before webhook delivery."
            )

        return WebhookNotification(
            event_id=event.event_id,
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_payload,
            processed_at=payment.processed_at,
            webhook_attempt=attempt_number,
            webhook_url=str(payment.webhook_url),
        )

    def _next_routing(
        self,
        *,
        error: WebhookDeliveryError,
        attempt_number: int,
    ) -> tuple[str, str]:
        if error.retryable and attempt_number < self._max_delivery_attempts:
            return self._retry_routing_keys[attempt_number - 1], "payment.webhook.retry"
        return self._dlq_routing_key, "payment.webhook.dlq"
