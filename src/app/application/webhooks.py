from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from app.application.interfaces import WebhookSender
from app.core.errors import BadRequestError
from app.core.security import HostnameResolver, resolve_public_webhook_address
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
        allowed_hosts: frozenset[str] = frozenset(),
        resolver: HostnameResolver | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._allowed_hosts = allowed_hosts
        self._resolver = resolver
        self._client = client

    async def send(self, notification: WebhookNotification) -> None:
        client = self._client
        owns_client = client is None
        if client is None:
            client = httpx.AsyncClient(
                timeout=self._timeout_seconds,
                follow_redirects=False,
                trust_env=False,
            )

        payload = notification.model_dump(mode="json", exclude={"webhook_url"})

        try:
            async with asyncio.timeout(self._timeout_seconds):
                try:
                    target_url = str(notification.webhook_url)
                    safe_address = await resolve_public_webhook_address(
                        target_url,
                        allowed_hosts=self._allowed_hosts,
                        resolver=self._resolver,
                    )
                    request_url = httpx.URL(target_url)
                    headers: dict[str, str] = {}
                    extensions: dict[str, str] = {}
                    if safe_address is not None:
                        original_host = request_url.host
                        request_url = request_url.copy_with(host=safe_address)
                        headers["Host"] = _build_host_header(
                            original_host,
                            request_url.port,
                            request_url.scheme,
                        )
                        if request_url.scheme == "https":
                            extensions["sni_hostname"] = original_host
                    request = client.build_request(
                        "POST",
                        request_url,
                        json=payload,
                        headers=headers,
                        extensions=extensions,
                    )
                    response = await client.send(request, follow_redirects=False)
                finally:
                    if owns_client:
                        await client.aclose()
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
        except BadRequestError as exc:
            raise WebhookDeliveryError(
                f"Webhook delivery rejected: {exc.message}",
                retryable=False,
            ) from exc
        except (TimeoutError, httpx.HTTPError, OSError) as exc:
            raise WebhookDeliveryError(
                f"Webhook delivery failed: {exc}",
                retryable=True,
            ) from exc


def _build_host_header(host: str, port: int | None, scheme: str) -> str:
    default_port = 443 if scheme == "https" else 80
    if port is None or port == default_port:
        return host
    return f"{host}:{port}"
