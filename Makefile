PYTHON ?= python3

.PHONY: install lint format typecheck test test-e2e run-api run-consumer run-outbox-relay run-webhook-mock compose-up compose-down verify-topology

install:
	pip install -e '.[dev]'

lint:
	ruff check src tests

format:
	ruff format src tests

typecheck:
	mypy src tests

test:
	pytest

test-e2e:
	pytest -m e2e

run-api:
	uvicorn app.main:create_app --factory --reload

run-consumer:
	$(PYTHON) -m app.messaging.consumer

run-outbox-relay:
	$(PYTHON) -m app.messaging.outbox_relay

run-webhook-mock:
	uvicorn app.tools.webhook_mock:create_app --factory --reload --port 8081

verify-topology:
	$(PYTHON) -m app.tools.verify_rabbit_topology

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
