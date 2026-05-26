from __future__ import annotations

import asyncio
import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from app.application.gateway import SimulatedPaymentGateway
from app.application.processing import PaymentEventProcessor
from app.application.webhooks import HttpWebhookSender
from app.core.config import get_settings
from app.core.heartbeat import Heartbeat
from app.core.logging import configure_logging, log_context
from app.db.session import build_session_factory
from app.messaging.contracts import PaymentCreatedEvent
from app.messaging.topology import build_rabbit_topology, declare_topology

settings = get_settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)
topology = build_rabbit_topology(settings)
session_factory = build_session_factory(settings.database_url)
gateway = SimulatedPaymentGateway(
    min_delay_seconds=settings.payment_gateway_min_delay_seconds,
    max_delay_seconds=settings.payment_gateway_max_delay_seconds,
    success_rate=settings.payment_gateway_success_rate,
)
webhook_sender = HttpWebhookSender(timeout_seconds=settings.webhook_timeout_seconds)
heartbeat = Heartbeat(settings.worker_heartbeat_file_consumer)


@app.after_startup
async def bootstrap_topology() -> None:
    await declare_topology(broker, topology)
    await heartbeat.start(interval_seconds=settings.worker_heartbeat_interval_seconds)


@broker.subscriber(queue=topology.main_queue, exchange=topology.exchange)
async def handle_payment_created(event: PaymentCreatedEvent) -> None:
    await heartbeat.beat()
    logger.info(
        "Processing payment event.",
        extra=log_context(
            event_id=event.event_id,
            payment_id=event.payment_id,
            idempotency_key=event.idempotency_key,
            webhook_attempt=event.webhook_attempt,
        ),
    )
    async with session_factory() as session:
        processor = PaymentEventProcessor(
            session=session,
            gateway=gateway,
            webhook_sender=webhook_sender,
            max_delivery_attempts=settings.webhook_max_delivery_attempts,
            retry_routing_keys=tuple(queue.name for queue in topology.retry_queues),
            dlq_routing_key=topology.dlq_queue.name,
            gateway_claim_timeout_seconds=settings.gateway_claim_timeout_seconds,
            webhook_claim_timeout_seconds=settings.webhook_claim_timeout_seconds,
        )
        await processor.process(event)


async def main() -> None:
    try:
        await app.run()
    finally:
        await heartbeat.stop()


if __name__ == "__main__":
    asyncio.run(main())
