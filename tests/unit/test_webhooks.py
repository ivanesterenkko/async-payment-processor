from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.application.webhooks import HttpWebhookSender, WebhookDeliveryError
from app.domain.enums import Currency, PaymentStatus
from app.messaging.contracts import WebhookNotification


def build_notification(url: str) -> WebhookNotification:
    return WebhookNotification(
        event_id=uuid4(),
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="Webhook security",
        metadata={},
        processed_at=datetime.now(UTC),
        webhook_attempt=1,
        webhook_url=url,
    )


async def test_dns_resolution_to_private_address_is_rejected() -> None:
    requests: list[httpx.Request] = []

    async def resolve_private(_: str, __: int) -> tuple[str, ...]:
        return ("169.254.169.254",)

    async def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        sender = HttpWebhookSender(
            timeout_seconds=1.0,
            resolver=resolve_private,
            client=client,
        )
        with pytest.raises(WebhookDeliveryError) as failure:
            await sender.send(build_notification("http://hooks.example/payments"))

    assert failure.value.retryable is False
    assert requests == []


async def test_public_dns_target_is_pinned_with_original_host() -> None:
    received: list[httpx.Request] = []

    async def resolve_public(_: str, __: int) -> tuple[str, ...]:
        return ("8.8.8.8",)

    async def handle(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        sender = HttpWebhookSender(
            timeout_seconds=1.0,
            resolver=resolve_public,
            client=client,
        )
        await sender.send(build_notification("https://hooks.example/payments"))

    assert received[0].url.host == "8.8.8.8"
    assert received[0].headers["Host"] == "hooks.example"
    assert received[0].extensions["sni_hostname"] == "hooks.example"


async def test_explicit_development_host_allowlist_bypasses_public_dns_policy() -> None:
    received: list[httpx.Request] = []

    async def fail_resolution(_: str, __: int) -> tuple[str, ...]:
        raise AssertionError("Allowlisted hosts must not be resolved by SSRF policy.")

    async def handle(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        sender = HttpWebhookSender(
            timeout_seconds=1.0,
            allowed_hosts=frozenset({"webhook-mock"}),
            resolver=fail_resolution,
            client=client,
        )
        await sender.send(build_notification("http://webhook-mock:8080/webhooks/payments"))

    assert received[0].url.host == "webhook-mock"


async def test_dns_resolution_is_bounded_by_delivery_timeout() -> None:
    async def slow_resolution(_: str, __: int) -> tuple[str, ...]:
        await asyncio.sleep(1)
        return ("8.8.8.8",)

    sender = HttpWebhookSender(
        timeout_seconds=0.01,
        resolver=slow_resolution,
    )
    with pytest.raises(WebhookDeliveryError) as failure:
        await sender.send(build_notification("https://hooks.example/payments"))

    assert failure.value.retryable is True
