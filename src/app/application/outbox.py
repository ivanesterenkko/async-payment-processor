from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import or_, select, update
from sqlalchemy.engine import CursorResult
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
        claim_timeout_seconds: float,
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._batch_size = batch_size
        self._claim_timeout_seconds = claim_timeout_seconds

    async def publish_pending_events(self) -> int:
        records, claimed_at = await self._claim_pending_events()
        published_count = 0

        for record in records:
            try:
                await self._publisher.publish(
                    payload=record.payload,
                    routing_key=record.routing_key,
                    message_type=record.event_type,
                    headers=record.headers,
                )
            except Exception as exc:
                await self._mark_failed(record, claimed_at, exc)
            else:
                if await self._mark_published(record, claimed_at):
                    published_count += 1

        return published_count

    async def _claim_pending_events(self) -> tuple[list[OutboxEvent], datetime]:
        now = utcnow()
        claim_cutoff = now - timedelta(seconds=self._claim_timeout_seconds)
        query = (
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                or_(
                    OutboxEvent.claimed_at.is_(None),
                    OutboxEvent.claimed_at < claim_cutoff,
                ),
            )
            .order_by(OutboxEvent.created_at.asc())
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(query)
        records = list(result.scalars())

        for record in records:
            record.attempts += 1
            record.claimed_at = now

        await self._session.commit()
        return records, now

    async def _mark_published(self, record: OutboxEvent, claimed_at: datetime) -> bool:
        statement = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == record.id,
                OutboxEvent.published_at.is_(None),
                OutboxEvent.claimed_at == claimed_at,
            )
            .values(published_at=utcnow(), claimed_at=None, last_error=None)
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        await self._session.commit()
        marked = (result.rowcount or 0) > 0
        if marked:
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
        return marked

    async def _mark_failed(
        self,
        record: OutboxEvent,
        claimed_at: datetime,
        error: Exception,
    ) -> None:
        statement = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == record.id,
                OutboxEvent.published_at.is_(None),
                OutboxEvent.claimed_at == claimed_at,
            )
            .values(claimed_at=None, last_error=str(error))
        )
        await self._session.execute(statement)
        await self._session.commit()
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
