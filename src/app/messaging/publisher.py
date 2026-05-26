from __future__ import annotations

from typing import Any

from faststream.rabbit import RabbitBroker, RabbitExchange

from app.application.interfaces import OutboxPublisher


class RabbitOutboxPublisher(OutboxPublisher):
    def __init__(self, broker: RabbitBroker, exchange: RabbitExchange) -> None:
        self._broker = broker
        self._exchange = exchange

    async def publish(
        self,
        *,
        payload: dict[str, Any],
        routing_key: str,
        message_type: str,
        headers: dict[str, Any],
    ) -> None:
        await self._broker.publish(
            payload,
            queue=routing_key,
            exchange=self._exchange,
            routing_key=routing_key,
            headers=headers,
            message_type=message_type,
            persist=True,
        )
