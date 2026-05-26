# Async Payment Processor

Asynchronous payment processing service built with FastAPI, SQLAlchemy, PostgreSQL, RabbitMQ, FastStream, and Alembic.

## Overview

The service accepts payment requests, stores the payment and broker event in one transaction, relays unpublished outbox messages to RabbitMQ, simulates gateway processing, and delivers the result through a webhook.

Runtime processes:

- `api` exposes HTTP endpoints.
- `outbox-relay` publishes unpublished outbox rows to RabbitMQ.
- `consumer` processes payment events and schedules retry or DLQ messages through the outbox.
- `webhook-mock` is an auxiliary service used for reproducible demos and e2e tests.

## HTTP API

### `POST /api/v1/payments`

Required headers:

- `X-API-Key`
- `Idempotency-Key`

Request body:

```json
{
  "amount": "149.99",
  "currency": "USD",
  "description": "Order #1001",
  "metadata": {"order_id": "1001"},
  "webhook_url": "https://example.com/webhooks/payments"
}
```

Response:

```json
{
  "payment_id": "9cb2f671-6db4-4588-b2cc-e6e3c2d84743",
  "status": "pending",
  "created_at": "2026-05-26T14:30:00.000000Z"
}
```

### `GET /api/v1/payments/{payment_id}`

Returns the current payment state, including webhook attempts and delivery timestamp.

### `GET /health`

Liveness endpoint for the API container.

## Error format

All handled application errors use a single envelope:

```json
{
  "error": {
    "code": "idempotency_conflict",
    "message": "Idempotency key is already bound to another payload.",
    "details": {
      "idempotency_key": "payment-key-1"
    }
  }
}
```

## Architecture

### Persistence

Two tables are used:

- `payments`
  - Business state, idempotency key, gateway claim state, webhook claim state, timestamps, and webhook delivery state.
- `outbox`
  - Serialized broker messages, routing key, publish attempts, and publication status.

### Messaging

- Exchange: `payments`
- Main queue: `payments.new`
- Retry queues:
  - `payments.new.retry.1`
  - `payments.new.retry.2`
- Dead letter queue: `payments.dlq`

Retry queues use TTL plus dead-letter routing back into `payments.new`.

### Delivery guarantees

- Payment creation and primary event creation happen in one database transaction.
- Retry and DLQ scheduling also happen through the outbox, not by direct publish from the consumer.
- If a broker publish succeeds but the relay transaction commit fails, the event may be published again later. The consumer is implemented to tolerate duplicate events.

### Concurrency model

- The consumer no longer keeps a database transaction open during gateway sleep or during webhook HTTP calls.
- Gateway work is guarded by a short-lived database claim.
- Webhook delivery is also guarded by a short-lived database claim.
- Claim timeouts allow another worker to recover a stuck payment if a worker crashes after claiming work.

### Webhook retry policy

- maximum delivery attempts per webhook: `3` total
- `2xx`: success
- `5xx`: retryable
- `408`, `425`, `429`: retryable
- other `4xx`: non-retryable, routed directly to DLQ
- network/transport errors: retryable

### Security hardening

- `Idempotency-Key` must match a strict format and length policy.
- Webhook URLs are validated against obvious SSRF targets such as localhost, private IP literals, and local/internal hostnames.

## Local setup

### 1. Prepare environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

### 2. Run with Docker Compose

```bash
docker compose up --build
```

Available services:

- API: `http://localhost:8000`
- Webhook mock: `http://localhost:8081`
- RabbitMQ UI: `http://localhost:15672`
- PostgreSQL: `localhost:5432`

Default credentials:

- API key: `local-dev-key`
- PostgreSQL user/password/db: `payment` / `payment` / `payments`
- RabbitMQ user/password: `guest` / `guest`

### 3. Run without Docker

Start the API:

```bash
alembic upgrade head
uvicorn app.main:create_app --factory --reload
```

Start workers in separate terminals:

```bash
python -m app.messaging.outbox_relay
python -m app.messaging.consumer
uvicorn app.tools.webhook_mock:create_app --factory --reload --port 8081
```

## Environment variables

| Variable | Description | Default |
| --- | --- | --- |
| `API_KEY` | Required API key for HTTP endpoints | `local-dev-key` |
| `DATABASE_URL` | Async SQLAlchemy database URL | `postgresql+asyncpg://payment:payment@postgres:5432/payments` |
| `RABBITMQ_URL` | RabbitMQ connection URL | `amqp://guest:guest@rabbitmq:5672/` |
| `OUTBOX_BATCH_SIZE` | Max outbox rows per relay iteration | `50` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | Relay sleep when nothing was published | `1.0` |
| `WEBHOOK_TIMEOUT_SECONDS` | HTTP timeout for webhook delivery | `5.0` |
| `WEBHOOK_MAX_DELIVERY_ATTEMPTS` | Total webhook delivery attempts before DLQ | `3` |
| `WEBHOOK_RETRY_DELAYS_SECONDS` | Retry delays in seconds | `[2,4]` |
| `GATEWAY_CLAIM_TIMEOUT_SECONDS` | Claim timeout for gateway processing | `30.0` |
| `WEBHOOK_CLAIM_TIMEOUT_SECONDS` | Claim timeout for webhook delivery | `30.0` |
| `WORKER_HEARTBEAT_INTERVAL_SECONDS` | Health heartbeat interval for workers | `5.0` |
| `PAYMENT_GATEWAY_MIN_DELAY_SECONDS` | Gateway simulation lower bound | `2.0` |
| `PAYMENT_GATEWAY_MAX_DELAY_SECONDS` | Gateway simulation upper bound | `5.0` |
| `PAYMENT_GATEWAY_SUCCESS_RATE` | Gateway success probability | `0.9` |

## Manual verification

### Health checks

```bash
curl http://localhost:8000/health
curl http://localhost:8081/health
```

### Happy path with internal webhook mock

Use the webhook mock service name in the payload so the consumer can reach it from inside Docker:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-dev-key' \
  -H 'Idempotency-Key: payment-001' \
  -d '{
    "amount": "149.99",
    "currency": "USD",
    "description": "Order #1001",
    "metadata": {"order_id": "1001"},
    "webhook_url": "http://webhook-mock:8080/webhooks/payments?scenario_key=happy-path"
  }'
```

Poll the payment:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H 'X-API-Key: local-dev-key'
```

Inspect delivered webhooks:

```bash
curl http://localhost:8081/events
```

### Retry path

Fail the first two webhook attempts, then succeed:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-dev-key' \
  -H 'Idempotency-Key: payment-retry-001' \
  -d '{
    "amount": "20.00",
    "currency": "EUR",
    "description": "Retry scenario",
    "metadata": {"scenario": "retry"},
    "webhook_url": "http://webhook-mock:8080/webhooks/payments?scenario_key=retry-demo&failures_before_success=2"
  }'
```

Expected result:

- payment stays `pending` briefly, then becomes `succeeded` or `failed`
- `webhook_attempts` eventually becomes `3`
- `webhook_delivered_at` becomes non-null
- `http://localhost:8081/events` contains three webhook deliveries for `retry-demo`

### Live RabbitMQ topology verification

```bash
python -m app.tools.verify_rabbit_topology
```

This command declares the exchange and queues with the expected arguments and fails if the live broker topology is incompatible.

## Automated checks

### Fast local checks

```bash
ruff check src tests
mypy src tests
pytest -m 'not e2e'
```

### Full e2e checks

Run Docker Compose first, then export the endpoints used by the e2e suite:

```bash
export E2E_API_BASE_URL=http://localhost:8000
export E2E_WEBHOOK_BASE_URL=http://localhost:8081
export E2E_INTERNAL_WEBHOOK_BASE_URL=http://webhook-mock:8080
export E2E_DATABASE_URL=postgresql+asyncpg://payment:payment@localhost:5432/payments
export E2E_RABBITMQ_URL=amqp://guest:guest@localhost:5672/
pytest -m e2e
```

The e2e suite covers:

- migration-backed database setup through Alembic
- parallel requests with the same `Idempotency-Key`
- real retry flow through RabbitMQ TTL queues and dead-letter routing
- live topology verification against RabbitMQ

## Make targets

```bash
make lint
make typecheck
make test
make test-e2e
make verify-topology
make compose-up
make compose-down
```
