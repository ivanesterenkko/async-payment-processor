from __future__ import annotations

import asyncio
import logging

from faststream.rabbit import RabbitBroker

from app.application.outbox import OutboxRelayService
from app.core.config import get_settings
from app.core.heartbeat import Heartbeat
from app.core.logging import configure_logging, log_context
from app.db.session import build_session_factory
from app.messaging.publisher import RabbitOutboxPublisher
from app.messaging.topology import build_rabbit_topology, declare_topology

settings = get_settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)


async def main() -> None:
    topology = build_rabbit_topology(settings)
    session_factory = build_session_factory(settings.database_url)
    broker = RabbitBroker(settings.rabbitmq_url)
    publisher = RabbitOutboxPublisher(broker, topology.exchange)
    heartbeat = Heartbeat(settings.worker_heartbeat_file_outbox_relay)

    async with broker:
        await declare_topology(broker, topology)
        await heartbeat.start(interval_seconds=settings.worker_heartbeat_interval_seconds)

        while True:
            try:
                await heartbeat.beat()
                async with session_factory() as session:
                    relay = OutboxRelayService(
                        session=session,
                        publisher=publisher,
                        batch_size=settings.outbox_batch_size,
                        claim_timeout_seconds=settings.outbox_claim_timeout_seconds,
                    )
                    published_count = await relay.publish_pending_events()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox relay iteration failed.")
                await asyncio.sleep(settings.outbox_poll_interval_seconds)
                continue

            logger.info(
                "Outbox relay iteration completed.",
                extra=log_context(published_count=published_count),
            )
            if published_count == 0:
                await asyncio.sleep(settings.outbox_poll_interval_seconds)


if __name__ == "__main__":
    asyncio.run(main())
