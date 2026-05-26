from __future__ import annotations

from typing import Any, Protocol

from app.db.models import Payment
from app.domain.enums import PaymentStatus
from app.messaging.contracts import WebhookNotification


class OutboxPublisher(Protocol):
    async def publish(
        self,
        *,
        payload: dict[str, Any],
        routing_key: str,
        message_type: str,
        headers: dict[str, Any],
    ) -> None:
        """Publish a serialized outbox message."""


class PaymentGateway(Protocol):
    async def process(self, payment: Payment) -> PaymentStatus:
        """Process a payment through the external gateway."""


class WebhookSender(Protocol):
    async def send(self, notification: WebhookNotification) -> None:
        """Send a webhook notification."""
