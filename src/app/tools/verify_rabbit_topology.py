from __future__ import annotations

import asyncio
import json

import aio_pika

from app.core.config import get_settings
from app.messaging.topology import build_rabbit_topology


async def main() -> None:
    get_settings.cache_clear()
    settings = get_settings()
    topology = build_rabbit_topology(settings)

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            settings.rabbitmq_exchange,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        queue_summaries: list[dict[str, object]] = []
        for queue_definition in topology.all_queues():
            queue = await channel.declare_queue(
                queue_definition.name,
                durable=True,
                arguments=queue_definition.arguments,
            )
            await queue.bind(exchange, routing_key=queue_definition.routing_key)
            queue_summaries.append(
                {
                    "name": queue_definition.name,
                    "routing_key": queue_definition.routing_key,
                    "arguments": queue_definition.arguments,
                }
            )

    print(
        json.dumps(
            {
                "exchange": settings.rabbitmq_exchange,
                "queues": queue_summaries,
                "status": "verified",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
