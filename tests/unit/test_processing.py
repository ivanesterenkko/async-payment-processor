from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.interfaces import PaymentGateway
from app.application.processing import PaymentEventProcessor
from app.application.webhooks import WebhookDeliveryError
from app.db.models import OutboxEvent, Payment, utcnow
from app.domain.enums import Currency, PaymentStatus
from app.messaging.contracts import PaymentCreatedEvent, WebhookNotification

RETRY_ROUTING_KEYS = (
    "payments.new.retry.1",
    "payments.new.retry.2",
)


class StaticGateway(PaymentGateway):
    def __init__(self, result: PaymentStatus) -> None:
        self._result = result
        self.calls = 0

    async def process(self, payment: Payment) -> PaymentStatus:
        del payment
        self.calls += 1
        return self._result


class ExplodingGateway(PaymentGateway):
    async def process(self, payment: Payment) -> PaymentStatus:
        del payment
        raise RuntimeError("gateway unavailable")


@dataclass
class RecordingWebhookSender:
    notifications: list[WebhookNotification] = field(default_factory=list)

    async def send(self, notification: WebhookNotification) -> None:
        self.notifications.append(notification)


class FailingWebhookSender:
    def __init__(
        self,
        message: str = "temporary error",
        *,
        retryable: bool = True,
        status_code: int | None = None,
    ) -> None:
        self.message = message
        self.calls = 0
        self.retryable = retryable
        self.status_code = status_code

    async def send(self, notification: WebhookNotification) -> None:
        del notification
        self.calls += 1
        raise WebhookDeliveryError(
            self.message,
            retryable=self.retryable,
            status_code=self.status_code,
        )


class SequenceWebhookSender:
    def __init__(self, failures_before_success: int) -> None:
        self.failures_before_success = failures_before_success
        self.calls = 0
        self.notifications: list[WebhookNotification] = []

    async def send(self, notification: WebhookNotification) -> None:
        self.calls += 1
        self.notifications.append(notification)
        if self.calls <= self.failures_before_success:
            raise WebhookDeliveryError("temporary error", retryable=True, status_code=500)


async def create_payment(
    session: AsyncSession,
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    processed: bool = False,
    delivered: bool = False,
    webhook_attempts: int = 0,
) -> Payment:
    now = utcnow()
    payment = Payment(
        id=uuid4(),
        amount=Decimal("15.00"),
        currency=Currency.USD,
        description="Webhook processing",
        metadata_payload={"customer_id": "customer-1"},
        status=status,
        idempotency_key=str(uuid4()),
        request_hash="hash",
        webhook_url="https://example.com/webhooks/payments",
        gateway_claimed_at=None,
        webhook_attempts=webhook_attempts,
        webhook_claimed_at=None,
        created_at=now,
        processed_at=now if processed else None,
        webhook_delivered_at=now if delivered else None,
        updated_at=now,
    )
    session.add(payment)
    await session.commit()
    return payment


def build_processor(
    session: AsyncSession,
    *,
    gateway: PaymentGateway,
    webhook_sender: object,
) -> PaymentEventProcessor:
    return PaymentEventProcessor(
        session=session,
        gateway=gateway,
        webhook_sender=webhook_sender,  # type: ignore[arg-type]
        max_delivery_attempts=3,
        retry_routing_keys=RETRY_ROUTING_KEYS,
        dlq_routing_key="payments.dlq",
        gateway_claim_timeout_seconds=30.0,
        webhook_claim_timeout_seconds=30.0,
    )


async def test_processor_updates_status_and_sends_webhook(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(session)

    gateway = StaticGateway(PaymentStatus.SUCCEEDED)
    sender = RecordingWebhookSender()
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=0,
    )

    async with session_factory() as session:
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(event)
        refreshed = await session.get(Payment, payment.id)

    assert refreshed is not None
    assert refreshed.status == PaymentStatus.SUCCEEDED
    assert refreshed.processed_at is not None
    assert refreshed.webhook_delivered_at is not None
    assert refreshed.webhook_attempts == 1
    assert len(sender.notifications) == 1
    assert gateway.calls == 1


async def test_processor_requeues_failed_webhook(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(session)

    gateway = StaticGateway(PaymentStatus.SUCCEEDED)
    sender = FailingWebhookSender()
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=0,
    )

    async with session_factory() as session:
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(event)
        refreshed = await session.get(Payment, payment.id)
        outbox_events = list((await session.execute(select(OutboxEvent))).scalars())

    assert refreshed is not None
    assert refreshed.status == PaymentStatus.SUCCEEDED
    assert refreshed.webhook_delivered_at is None
    assert refreshed.webhook_attempts == 1
    assert outbox_events[0].routing_key == "payments.new.retry.1"
    assert sender.calls == 1


async def test_processor_routes_to_dlq_after_last_retry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(
            session,
            status=PaymentStatus.SUCCEEDED,
            processed=True,
            webhook_attempts=2,
        )

    gateway = StaticGateway(PaymentStatus.SUCCEEDED)
    sender = FailingWebhookSender("still failing")
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=2,
    )

    async with session_factory() as session:
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(event)
        refreshed = await session.get(Payment, payment.id)
        outbox_events = list((await session.execute(select(OutboxEvent))).scalars())

    assert refreshed is not None
    assert refreshed.webhook_delivered_at is None
    assert refreshed.webhook_attempts == 3
    assert outbox_events[0].routing_key == "payments.dlq"


async def test_processor_non_retryable_webhook_goes_directly_to_dlq(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(session)

    gateway = StaticGateway(PaymentStatus.SUCCEEDED)
    sender = FailingWebhookSender("bad request", retryable=False, status_code=400)
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=0,
    )

    async with session_factory() as session:
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(event)
        outbox_events = list((await session.execute(select(OutboxEvent))).scalars())

    assert outbox_events[0].routing_key == "payments.dlq"


async def test_webhook_event_succeeds_after_two_retries(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(session)

    gateway = StaticGateway(PaymentStatus.SUCCEEDED)
    sender = SequenceWebhookSender(failures_before_success=2)
    first_event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=0,
    )

    async with session_factory() as session:
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(first_event)

    async with session_factory() as session:
        first_retry = (
            await session.execute(select(OutboxEvent).order_by(OutboxEvent.created_at.asc()))
        ).scalars().all()[-1]
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(PaymentCreatedEvent.model_validate(first_retry.payload))

    async with session_factory() as session:
        second_retry = (
            await session.execute(select(OutboxEvent).order_by(OutboxEvent.created_at.asc()))
        ).scalars().all()[-1]
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(PaymentCreatedEvent.model_validate(second_retry.payload))
        refreshed = await session.get(Payment, payment.id)

    assert refreshed is not None
    assert refreshed.webhook_delivered_at is not None
    assert refreshed.webhook_attempts == 3
    assert sender.calls == 3


async def test_duplicate_event_after_success_is_ignored(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(
            session,
            status=PaymentStatus.SUCCEEDED,
            processed=True,
            delivered=True,
            webhook_attempts=1,
        )

    gateway = StaticGateway(PaymentStatus.FAILED)
    sender = RecordingWebhookSender()
    event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=0,
    )

    async with session_factory() as session:
        processor = build_processor(session, gateway=gateway, webhook_sender=sender)
        await processor.process(event)
        refreshed = await session.get(Payment, payment.id)

    assert refreshed is not None
    assert refreshed.status == PaymentStatus.SUCCEEDED
    assert refreshed.webhook_attempts == 1
    assert sender.notifications == []
    assert gateway.calls == 0


async def test_gateway_failure_clears_claim_and_records_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_payment(session)

    event = PaymentCreatedEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        webhook_attempt=0,
    )

    async with session_factory() as session:
        processor = build_processor(
            session,
            gateway=ExplodingGateway(),
            webhook_sender=RecordingWebhookSender(),
        )
        with pytest.raises(RuntimeError, match="gateway unavailable"):
            await processor.process(event)
        refreshed = await session.get(Payment, payment.id)

    assert refreshed is not None
    assert refreshed.status == PaymentStatus.PENDING
    assert refreshed.gateway_claimed_at is None
    assert refreshed.processed_at is None
    assert refreshed.last_error == "Gateway processing failed: gateway unavailable"
