from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from app.db.models import OutboxEvent, utcnow


def build_outbox_event(
    aggregate_id: UUID,
    *,
    event_type: str,
    routing_key: str,
    payload: dict[str, Any],
    headers: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> OutboxEvent:
    return OutboxEvent(
        aggregate_id=aggregate_id,
        event_type=event_type,
        routing_key=routing_key,
        payload=payload,
        headers=headers or {},
        created_at=created_at or utcnow(),
    )
