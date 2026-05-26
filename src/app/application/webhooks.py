from __future__ import annotations

from dataclasses import dataclass

import httpx

from app.application.interfaces import WebhookSender
from app.messaging.contracts import WebhookNotification

RETRYABLE_STATUS_CODES = {408, 425, 429}


@dataclass(slots=True)
class WebhookDeliveryError(Exception):
    message: str
    retryable: bool
    status_code: int | None = None

    def __str__(self) -> str:
        return self.message


class HttpWebhookSender(WebhookSender):
    def __init__(
        self,
        *,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._client = client

    async def send(self, notification: WebhookNotification) -> None:
        client = self._client
        owns_client = client is None
        if client is None:
            client = httpx.AsyncClient(timeout=self._timeout_seconds)

        payload = notification.model_dump(mode="json", exclude={"webhook_url"})

        try:
            response = await client.post(str(notification.webhook_url), json=payload)
            if response.status_code // 100 != 2:
                retryable = (
                    response.status_code >= 500
                    or response.status_code in RETRYABLE_STATUS_CODES
                )
                raise WebhookDeliveryError(
                    f"Webhook responded with unexpected status code {response.status_code}.",
                    retryable=retryable,
                    status_code=response.status_code,
                )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(
                f"Webhook delivery failed: {exc}",
                retryable=True,
            ) from exc
        finally:
            if owns_client:
                await client.aclose()
