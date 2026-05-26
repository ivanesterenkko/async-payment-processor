from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.interfaces import OutboxPublisher
from app.core.logging import log_context
from app.db.models import OutboxEvent, utcnow

logger = logging.getLogger(__name__)


class OutboxRelayService:
    def __init__(
        self,
        session: AsyncSession,
        publisher: OutboxPublisher,
        batch_size: int,
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._batch_size = batch_size

    async def publish_pending_events(self) -> int:
        query = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at.asc())
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(query)
        records = list(result.scalars())
        published_count = 0

        for record in records:
            record.attempts += 1
            try:
                await self._publisher.publish(
                    payload=record.payload,
                    routing_key=record.routing_key,
                    message_type=record.event_type,
                    headers=record.headers,
                )
            except Exception as exc:
                record.last_error = str(exc)
                logger.warning(
                    "Outbox publish failed.",
                    extra=log_context(
                        outbox_event_id=record.id,
                        aggregate_id=record.aggregate_id,
                        routing_key=record.routing_key,
                        event_type=record.event_type,
                        attempts=record.attempts,
                    ),
                )
            else:
                record.published_at = utcnow()
                record.last_error = None
                published_count += 1
                logger.info(
                    "Outbox event published.",
                    extra=log_context(
                        outbox_event_id=record.id,
                        aggregate_id=record.aggregate_id,
                        routing_key=record.routing_key,
                        event_type=record.event_type,
                        attempts=record.attempts,
                    ),
                )

        await self._session.commit()
        return published_count
