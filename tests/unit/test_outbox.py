from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.application.outbox import OutboxRelayService
from app.application.payments import PaymentService
from app.db.models import OutboxEvent
from app.domain.enums import Currency
from app.messaging.contracts import PaymentCreatedEvent
from app.schemas.payments import PaymentCreateRequest


@dataclass
class RecordingPublisher:
    events: list[tuple[str, PaymentCreatedEvent]] = field(default_factory=list)

    async def publish(
        self,
        *,
        payload: dict[str, object],
        routing_key: str,
        message_type: str,
        headers: dict[str, object],
    ) -> None:
        del message_type, headers
        self.events.append((routing_key, PaymentCreatedEvent.model_validate(payload)))


async def test_outbox_relay_publishes_and_marks_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        service = PaymentService(session)
        await service.create_payment(
            PaymentCreateRequest.model_validate(
                {
                    "amount": Decimal("10.00"),
                    "currency": Currency.EUR,
                    "description": "Relay test",
                    "metadata": {"invoice_id": "INV-1"},
                    "webhook_url": "https://example.com/webhooks/payments",
                }
            ),
            idempotency_key="relay-key",
        )

    publisher = RecordingPublisher()
    async with session_factory() as session:
        relay = OutboxRelayService(
            session=session,
            publisher=publisher,
            batch_size=10,
            claim_timeout_seconds=30.0,
        )
        published_count = await relay.publish_pending_events()

    assert published_count == 1
    assert len(publisher.events) == 1
    assert publisher.events[0][0] == "payments.new"

    async with session_factory() as session:
        outbox_event = (await session.execute(select(OutboxEvent))).scalar_one()

    assert outbox_event.published_at is not None
    assert outbox_event.last_error is None


async def test_outbox_duplicate_publish_after_commit_failure_is_safe(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        service = PaymentService(session)
        await service.create_payment(
            PaymentCreateRequest.model_validate(
                {
                    "amount": Decimal("10.00"),
                    "currency": Currency.EUR,
                    "description": "Relay duplicate test",
                    "metadata": {"invoice_id": "INV-2"},
                    "webhook_url": "https://example.com/webhooks/payments",
                }
            ),
            idempotency_key="relay-duplicate-key",
        )

    publisher = RecordingPublisher()
    async with session_factory() as session:
        relay = OutboxRelayService(
            session=session,
            publisher=publisher,
            batch_size=10,
            claim_timeout_seconds=0.0,
        )
        original_commit = session.commit
        commit_attempts = 0

        async def failing_commit() -> None:
            nonlocal commit_attempts
            commit_attempts += 1
            if commit_attempts == 2:
                raise RuntimeError("commit failed after publish")
            await original_commit()

        session.commit = failing_commit  # type: ignore[method-assign]
        with pytest.raises(RuntimeError):
            await relay.publish_pending_events()

    async with session_factory() as session:
        relay = OutboxRelayService(
            session=session,
            publisher=publisher,
            batch_size=10,
            claim_timeout_seconds=0.0,
        )
        published_count = await relay.publish_pending_events()

    assert published_count == 1
    assert len(publisher.events) == 2
