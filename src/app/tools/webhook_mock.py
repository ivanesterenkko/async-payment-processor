from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Query, Request, Response


def create_app() -> FastAPI:
    app = FastAPI(title="webhook-mock")
    app.state.events = []
    app.state.attempts = defaultdict(int)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/events")
    async def list_events(request: Request) -> list[dict[str, Any]]:
        return list(request.app.state.events)

    @app.delete("/events")
    async def clear_events(request: Request) -> dict[str, str]:
        request.app.state.events.clear()
        request.app.state.attempts.clear()
        return {"status": "cleared"}

    @app.post("/webhooks/payments")
    async def receive_payment_webhook(
        request: Request,
        failures_before_success: int = Query(default=0, ge=0),
        scenario_key: str | None = Query(default=None),
        always_status: int | None = Query(default=None, ge=100, le=599),
    ) -> Response:
        payload = await request.json()
        key = scenario_key or f"{request.url.path}?{request.url.query}"
        request.app.state.attempts[key] += 1
        attempt_number = request.app.state.attempts[key]
        request.app.state.events.append(
            {
                "scenario_key": key,
                "attempt_number": attempt_number,
                "received_at": datetime.now(UTC).isoformat(),
                "payload": payload,
            }
        )

        if always_status is not None:
            return Response(status_code=always_status)
        if attempt_number <= failures_before_success:
            return Response(status_code=500)
        return Response(status_code=200)

    return app
