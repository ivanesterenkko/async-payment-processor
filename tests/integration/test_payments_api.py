from __future__ import annotations

import asyncio
from typing import cast

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent, Payment


def build_payload() -> dict[str, object]:
    return {
        "amount": "125.50",
        "currency": "USD",
        "description": "Book order",
        "metadata": {"order_id": "A-100"},
        "webhook_url": "https://example.com/webhooks/payments",
    }


def build_headers(idempotency_key: str) -> dict[str, str]:
    return {
        "Idempotency-Key": idempotency_key,
        "X-API-Key": "test-api-key",
    }


async def test_create_payment_creates_outbox_event(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        json=build_payload(),
        headers=build_headers("payment-key-1"),
    )
    body = response.json()

    assert response.status_code == 202
    assert body["status"] == "pending"
    assert body["payment_id"]

    async with session_factory() as session:
        payments = list((await session.execute(select(Payment))).scalars())
        outbox_rows = list((await session.execute(select(OutboxEvent))).scalars())

    assert len(payments) == 1
    assert len(outbox_rows) == 1
    assert outbox_rows[0].aggregate_id == payments[0].id
    assert outbox_rows[0].routing_key == "payments.new"


async def test_idempotent_replay_returns_same_payment_id(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_response = await api_client.post(
        "/api/v1/payments",
        json=build_payload(),
        headers=build_headers("payment-key-2"),
    )
    second_response = await api_client.post(
        "/api/v1/payments",
        json=build_payload(),
        headers=build_headers("payment-key-2"),
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert first_response.json()["payment_id"] == second_response.json()["payment_id"]

    async with session_factory() as session:
        payments = list((await session.execute(select(Payment))).scalars())
        outbox_rows = list((await session.execute(select(OutboxEvent))).scalars())

    assert len(payments) == 1
    assert len(outbox_rows) == 1


async def test_idempotency_conflict_returns_409(api_client: AsyncClient) -> None:
    first_payload = build_payload()
    second_payload = build_payload()
    second_payload["description"] = "Another order"

    first_response = await api_client.post(
        "/api/v1/payments",
        json=first_payload,
        headers=build_headers("payment-key-3"),
    )
    second_response = await api_client.post(
        "/api/v1/payments",
        json=second_payload,
        headers=build_headers("payment-key-3"),
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 409
    assert second_response.json()["error"]["code"] == "idempotency_conflict"


async def test_get_payment_returns_details(api_client: AsyncClient) -> None:
    create_response = await api_client.post(
        "/api/v1/payments",
        json=build_payload(),
        headers=build_headers("payment-key-4"),
    )
    payment_id = create_response.json()["payment_id"]

    get_response = await api_client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": "test-api-key"},
    )
    body = get_response.json()

    assert get_response.status_code == 200
    assert body["payment_id"] == payment_id
    assert body["metadata"] == {"order_id": "A-100"}


async def test_parallel_identical_idempotency_key_creates_single_payment(
    api_client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async def create_once() -> dict[str, object]:
        response = await api_client.post(
            "/api/v1/payments",
            json=build_payload(),
            headers=build_headers("payment-key-parallel"),
        )
        assert response.status_code == 202
        return cast(dict[str, object], response.json())

    results = await asyncio.gather(*(create_once() for _ in range(5)))
    payment_ids = {result["payment_id"] for result in results}

    async with session_factory() as session:
        payments = list((await session.execute(select(Payment))).scalars())
        outbox_rows = list((await session.execute(select(OutboxEvent))).scalars())

    assert len(payment_ids) == 1
    assert len(payments) == 1
    assert len(outbox_rows) == 1
