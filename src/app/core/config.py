from __future__ import annotations

from functools import lru_cache
from typing import cast

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_name: str = "async-payment-processor"
    api_key: str = "local-dev-key"
    database_url: str = "postgresql+asyncpg://payment:payment@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_exchange: str = "payments"
    rabbitmq_main_queue: str = "payments.new"
    rabbitmq_processing_retry_queue: str = "payments.processing.retry"
    rabbitmq_retry_queues: tuple[str, ...] = (
        "payments.new.retry.1",
        "payments.new.retry.2",
    )
    rabbitmq_dlq: str = "payments.dlq"
    outbox_batch_size: int = 50
    outbox_poll_interval_seconds: float = 1.0
    outbox_claim_timeout_seconds: float = 30.0
    processing_retry_delay_seconds: int = 2
    webhook_timeout_seconds: float = 5.0
    webhook_max_delivery_attempts: int = 3
    webhook_retry_delays_seconds: tuple[int, ...] = Field(default=(2, 4))
    webhook_allowed_hosts: tuple[str, ...] = ()
    gateway_claim_timeout_seconds: float = 30.0
    webhook_claim_timeout_seconds: float = 30.0
    claim_recovery_batch_size: int = 50
    claim_recovery_poll_interval_seconds: float = 1.0
    worker_heartbeat_file_consumer: str = "/tmp/consumer-heartbeat"
    worker_heartbeat_file_outbox_relay: str = "/tmp/outbox-relay-heartbeat"
    worker_heartbeat_interval_seconds: float = 5.0
    payment_gateway_min_delay_seconds: float = 2.0
    payment_gateway_max_delay_seconds: float = 5.0
    payment_gateway_success_rate: float = 0.9
    log_level: str = "INFO"

    @field_validator("rabbitmq_retry_queues", mode="before")
    @classmethod
    def parse_retry_queues(
        cls,
        value: str | list[str] | tuple[str, ...],
    ) -> tuple[str, ...]:
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return tuple(value)

        return tuple(item.strip() for item in value.split(",") if item.strip())

    @field_validator("webhook_retry_delays_seconds", mode="before")
    @classmethod
    def parse_retry_delays(
        cls,
        value: str | list[int] | tuple[int, ...],
    ) -> tuple[int, ...]:
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return cast(tuple[int, ...], tuple(value))

        return tuple(int(item.strip()) for item in value.split(",") if item.strip())

    @field_validator("webhook_allowed_hosts", mode="before")
    @classmethod
    def parse_allowed_hosts(
        cls,
        value: str | list[str] | tuple[str, ...],
    ) -> tuple[str, ...]:
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return tuple(host.lower() for host in value)

        return tuple(host.strip().lower() for host in value.split(",") if host.strip())

    @field_validator("payment_gateway_success_rate")
    @classmethod
    def validate_success_rate(cls, value: float) -> float:
        if not 0 <= value <= 1:
            raise ValueError("PAYMENT_GATEWAY_SUCCESS_RATE must be between 0 and 1.")
        return value

    @field_validator("webhook_max_delivery_attempts")
    @classmethod
    def validate_max_delivery_attempts(cls, value: int) -> int:
        if value < 1:
            raise ValueError("WEBHOOK_MAX_DELIVERY_ATTEMPTS must be at least 1.")
        return value

    @field_validator("payment_gateway_max_delay_seconds")
    @classmethod
    def validate_max_delay(cls, value: float, info: ValidationInfo) -> float:
        min_delay = float(info.data.get("payment_gateway_min_delay_seconds", 0.0))
        if value < float(min_delay):
            raise ValueError("PAYMENT_GATEWAY_MAX_DELAY_SECONDS must be >= min delay.")
        return value

    @model_validator(mode="after")
    def validate_retry_settings(self) -> Settings:
        expected_retry_count = max(self.webhook_max_delivery_attempts - 1, 0)
        if len(self.rabbitmq_retry_queues) != expected_retry_count:
            raise ValueError(
                "RABBITMQ_RETRY_QUEUES must contain exactly "
                f"{expected_retry_count} queue names for the configured max delivery attempts."
            )
        if len(self.webhook_retry_delays_seconds) != expected_retry_count:
            raise ValueError(
                "WEBHOOK_RETRY_DELAYS_SECONDS must contain exactly "
                f"{expected_retry_count} values for the configured max delivery attempts."
            )
        if any(delay <= 0 for delay in self.webhook_retry_delays_seconds):
            raise ValueError("WEBHOOK_RETRY_DELAYS_SECONDS values must be positive.")
        if self.processing_retry_delay_seconds <= 0:
            raise ValueError("PROCESSING_RETRY_DELAY_SECONDS must be positive.")
        if self.outbox_claim_timeout_seconds <= 0:
            raise ValueError("OUTBOX_CLAIM_TIMEOUT_SECONDS must be positive.")
        if self.claim_recovery_poll_interval_seconds <= 0:
            raise ValueError("CLAIM_RECOVERY_POLL_INTERVAL_SECONDS must be positive.")
        if self.claim_recovery_batch_size < 1:
            raise ValueError("CLAIM_RECOVERY_BATCH_SIZE must be at least 1.")
        if self.gateway_claim_timeout_seconds <= self.payment_gateway_max_delay_seconds:
            raise ValueError(
                "GATEWAY_CLAIM_TIMEOUT_SECONDS must exceed PAYMENT_GATEWAY_MAX_DELAY_SECONDS."
            )
        if self.webhook_claim_timeout_seconds <= self.webhook_timeout_seconds:
            raise ValueError(
                "WEBHOOK_CLAIM_TIMEOUT_SECONDS must exceed WEBHOOK_TIMEOUT_SECONDS."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
