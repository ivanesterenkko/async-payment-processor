from __future__ import annotations

import asyncio
import contextlib
import logging

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker

from app.application.gateway import SimulatedPaymentGateway
from app.application.processing import PaymentEventProcessor
from app.application.recovery import ClaimRecoveryService
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
webhook_sender = HttpWebhookSender(
    timeout_seconds=settings.webhook_timeout_seconds,
    allowed_hosts=frozenset(settings.webhook_allowed_hosts),
)
heartbeat = Heartbeat(settings.worker_heartbeat_file_consumer)
recovery_task: asyncio.Task[None] | None = None


@app.after_startup
async def bootstrap_topology() -> None:
    global recovery_task
    await declare_topology(broker, topology)
    await heartbeat.start(interval_seconds=settings.worker_heartbeat_interval_seconds)
    recovery_task = asyncio.create_task(run_claim_recovery(), name="claim-recovery")


@app.on_shutdown
async def stop_claim_recovery() -> None:
    global recovery_task
    if recovery_task is None:
        return
    recovery_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await recovery_task
    recovery_task = None


async def run_claim_recovery() -> None:
    while True:
        try:
            async with session_factory() as session:
                recovery = ClaimRecoveryService(
                    session=session,
                    main_routing_key=topology.main_queue.name,
                    batch_size=settings.claim_recovery_batch_size,
                    gateway_claim_timeout_seconds=settings.gateway_claim_timeout_seconds,
                    webhook_claim_timeout_seconds=settings.webhook_claim_timeout_seconds,
                )
                recovered_count = await recovery.recover_stale_claims()
            if recovered_count:
                logger.warning(
                    "Expired processing claims recovered.",
                    extra=log_context(recovered_count=recovered_count),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Claim recovery iteration failed.")
        await asyncio.sleep(settings.claim_recovery_poll_interval_seconds)


@broker.subscriber(
    queue=topology.main_queue,
    exchange=topology.exchange,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
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
            processing_retry_routing_key=topology.processing_retry_queue.name,
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
