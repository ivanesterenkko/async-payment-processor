from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from collections.abc import Awaitable, Callable
from typing import cast
from urllib.parse import urlparse

from app.core.errors import BadRequestError

HostnameResolver = Callable[[str, int], Awaitable[tuple[str, ...]]]
IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
BLOCKED_WEBHOOK_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "0.0.0.0",
    "host.docker.internal",
}
BLOCKED_WEBHOOK_SUFFIXES = (".local", ".internal")


def validate_idempotency_key(value: str) -> str:
    if not IDEMPOTENCY_KEY_PATTERN.fullmatch(value):
        raise BadRequestError(
            (
                "Idempotency-Key must be 8-128 chars and contain only letters, "
                "digits, '.', '_', ':' or '-'."
            ),
            code="invalid_idempotency_key",
        )
    return value


def validate_public_webhook_url(
    value: str,
    *,
    allowed_hosts: frozenset[str] = frozenset(),
) -> str:
    parsed = urlparse(value)
    host = parsed.hostname

    if parsed.scheme not in {"http", "https"}:
        raise BadRequestError(
            "Webhook URL must use http or https.",
            code="invalid_webhook_url_scheme",
        )
    if host is None:
        raise BadRequestError(
            "Webhook URL must include a valid host.",
            code="invalid_webhook_url",
        )

    normalized_host = host.lower()
    if normalized_host in allowed_hosts:
        return value
    if (
        normalized_host in BLOCKED_WEBHOOK_HOSTS
        or normalized_host.endswith(BLOCKED_WEBHOOK_SUFFIXES)
    ):
        raise BadRequestError(
            "Webhook URL points to a blocked host.",
            code="unsafe_webhook_url",
        )

    try:
        address = ipaddress.ip_address(normalized_host)
    except ValueError:
        return value

    _validate_public_address(address)
    return value


async def resolve_public_webhook_address(
    value: str,
    *,
    allowed_hosts: frozenset[str] = frozenset(),
    resolver: HostnameResolver | None = None,
) -> str | None:
    validate_public_webhook_url(value, allowed_hosts=allowed_hosts)
    parsed = urlparse(value)
    host = parsed.hostname
    if host is None or host.lower() in allowed_hosts:
        return None

    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        effective_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        resolve = resolver or _resolve_hostname
        addresses = await resolve(host, effective_port)
        if not addresses:
            raise OSError(f"No address resolved for webhook host {host}.") from None
        for resolved_address in addresses:
            _validate_public_address(ipaddress.ip_address(resolved_address))
        return addresses[0]

    _validate_public_address(address)
    return None


async def _resolve_hostname(host: str, port: int) -> tuple[str, ...]:
    results = await asyncio.to_thread(
        socket.getaddrinfo,
        host,
        port,
        type=socket.SOCK_STREAM,
    )
    return tuple(dict.fromkeys(cast(str, result[4][0]) for result in results))


def _validate_public_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
    if (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        raise BadRequestError(
            "Webhook URL must not target a private or local network address.",
            code="unsafe_webhook_url",
        )
