from __future__ import annotations

import asyncio
from typing import cast

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Payment
from app.tools.verify_rabbit_topology import main as verify_rabbit_topology_main


def build_headers(idempotency_key: str) -> dict[str, str]:
    return {
        "X-API-Key": "local-dev-key",
        "Idempotency-Key": idempotency_key,
    }


@pytest.mark.e2e
async def test_parallel_identical_requests_use_single_payment(
    e2e_api_client: AsyncClient,
    e2e_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async def create_once() -> dict[str, object]:
        response = await e2e_api_client.post(
            "/api/v1/payments",
            headers=build_headers("e2e-payment-parallel"),
            json={
                "amount": "25.00",
                "currency": "USD",
                "description": "Parallel order",
                "metadata": {"scenario": "parallel"},
                "webhook_url": "http://webhook-mock:8080/webhooks/payments?scenario_key=e2e-parallel",
            },
        )
        assert response.status_code == 202
        return cast(dict[str, object], response.json())

    results = await asyncio.gather(*(create_once() for _ in range(5)))
    payment_ids = {result["payment_id"] for result in results}

    async with e2e_session_factory() as session:
        payments = list((await session.execute(select(Payment))).scalars())

    assert len(payment_ids) == 1
    assert len(payments) == 1


@pytest.mark.e2e
async def test_webhook_retry_flow_succeeds_after_two_failures(
    e2e_api_client: AsyncClient,
    e2e_webhook_client: AsyncClient,
    e2e_env: dict[str, str],
) -> None:
    webhook_url = (
        f"{e2e_env['E2E_INTERNAL_WEBHOOK_BASE_URL']}"
        "/webhooks/payments?scenario_key=e2e-retry-success&failures_before_success=2"
    )
    create_response = await e2e_api_client.post(
        "/api/v1/payments",
        headers=build_headers("e2e-payment-retry"),
        json={
            "amount": "30.00",
            "currency": "EUR",
            "description": "Retry order",
            "metadata": {"scenario": "retry-success"},
            "webhook_url": webhook_url,
        },
    )
    assert create_response.status_code == 202
    payment_id = create_response.json()["payment_id"]

    delivered = False
    for _ in range(25):
        await asyncio.sleep(1)
        payment_response = await e2e_api_client.get(
            f"/api/v1/payments/{payment_id}",
            headers={"X-API-Key": "local-dev-key"},
        )
        assert payment_response.status_code == 200
        body = payment_response.json()
        if body["webhook_delivered_at"] is not None:
            delivered = True
            assert body["webhook_attempts"] == 3
            break

    assert delivered
    events_response = await e2e_webhook_client.get("/events")
    events = events_response.json()
    scenario_events = [event for event in events if event["scenario_key"] == "e2e-retry-success"]
    assert len(scenario_events) == 3


@pytest.mark.e2e
async def test_live_topology_matches_expected_configuration(
    monkeypatch: pytest.MonkeyPatch,
    e2e_env: dict[str, str],
) -> None:
    monkeypatch.setenv("RABBITMQ_URL", e2e_env.get("E2E_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
    await verify_rabbit_topology_main()
