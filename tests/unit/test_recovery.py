from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.recovery import ClaimRecoveryService
from app.db.models import OutboxEvent, Payment, utcnow
from app.domain.enums import Currency, PaymentStatus
from app.messaging.contracts import PaymentCreatedEvent


async def create_claimed_payment(
    session: AsyncSession,
    *,
    status: PaymentStatus,
) -> Payment:
    now = utcnow()
    payment = Payment(
        id=uuid4(),
        event_id=uuid4(),
        amount=Decimal("15.00"),
        currency=Currency.USD,
        description="Recovery test",
        metadata_payload={},
        status=status,
        idempotency_key=str(uuid4()),
        request_hash="hash",
        webhook_url="https://example.com/webhooks/payments",
        gateway_claimed_at=now - timedelta(seconds=60) if status is PaymentStatus.PENDING else None,
        webhook_attempts=1 if status is not PaymentStatus.PENDING else 0,
        webhook_claimed_at=(
            now - timedelta(seconds=60) if status is not PaymentStatus.PENDING else None
        ),
        webhook_delivered_at=None,
        created_at=now,
        processed_at=now if status is not PaymentStatus.PENDING else None,
        updated_at=now,
    )
    session.add(payment)
    await session.commit()
    return payment


async def test_expired_gateway_claim_creates_durable_resume_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_claimed_payment(session, status=PaymentStatus.PENDING)
        service = ClaimRecoveryService(
            session=session,
            main_routing_key="payments.new",
            batch_size=10,
            gateway_claim_timeout_seconds=30.0,
            webhook_claim_timeout_seconds=30.0,
        )
        assert await service.recover_stale_claims() == 1
        refreshed = await session.get(Payment, payment.id)
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()

    assert refreshed is not None
    assert refreshed.gateway_claimed_at is None
    assert outbox.routing_key == "payments.new"
    assert PaymentCreatedEvent.model_validate(outbox.payload).event_id == payment.event_id


async def test_expired_webhook_claim_resumes_same_notification_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        payment = await create_claimed_payment(session, status=PaymentStatus.SUCCEEDED)
        service = ClaimRecoveryService(
            session=session,
            main_routing_key="payments.new",
            batch_size=10,
            gateway_claim_timeout_seconds=30.0,
            webhook_claim_timeout_seconds=30.0,
        )
        assert await service.recover_stale_claims() == 1
        refreshed = await session.get(Payment, payment.id)
        outbox = (await session.execute(select(OutboxEvent))).scalar_one()

    assert refreshed is not None
    assert refreshed.webhook_claimed_at is None
    recovered_event = PaymentCreatedEvent.model_validate(outbox.payload)
    assert recovered_event.event_id == payment.event_id
    assert recovered_event.webhook_attempt == 1
