from __future__ import annotations

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import Settings


@dataclass(frozen=True, slots=True)
class RabbitTopology:
    exchange: RabbitExchange
    main_queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]
    dlq_queue: RabbitQueue

    def all_queues(self) -> tuple[RabbitQueue, ...]:
        return (self.main_queue, *self.retry_queues, self.dlq_queue)


def build_rabbit_topology(settings: Settings) -> RabbitTopology:
    exchange = RabbitExchange(
        name=settings.rabbitmq_exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    main_queue = RabbitQueue(
        name=settings.rabbitmq_main_queue,
        durable=True,
        routing_key=settings.rabbitmq_main_queue,
    )
    retry_queues = tuple(
        RabbitQueue(
            name=queue_name,
            durable=True,
            routing_key=queue_name,
            arguments={
                "x-message-ttl": delay_seconds * 1000,
                "x-dead-letter-exchange": settings.rabbitmq_exchange,
                "x-dead-letter-routing-key": settings.rabbitmq_main_queue,
            },
        )
        for queue_name, delay_seconds in zip(
            settings.rabbitmq_retry_queues,
            settings.webhook_retry_delays_seconds,
            strict=True,
        )
    )
    dlq_queue = RabbitQueue(
        name=settings.rabbitmq_dlq,
        durable=True,
        routing_key=settings.rabbitmq_dlq,
    )
    return RabbitTopology(
        exchange=exchange,
        main_queue=main_queue,
        retry_queues=retry_queues,
        dlq_queue=dlq_queue,
    )


async def declare_topology(broker: RabbitBroker, topology: RabbitTopology) -> None:
    exchange = await broker.declare_exchange(topology.exchange)
    for queue in topology.all_queues():
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(exchange, routing_key=queue.routing_key)
