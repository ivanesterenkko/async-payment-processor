"""create payments and outbox tables"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "202605261430"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    currency_enum = postgresql.ENUM(
        "RUB",
        "USD",
        "EUR",
        name="currency_enum",
        create_type=False,
    )
    payment_status_enum = postgresql.ENUM(
        "pending",
        "succeeded",
        "failed",
        name="payment_status_enum",
        create_type=False,
    )

    bind = op.get_bind()
    currency_enum.create(bind, checkfirst=True)
    payment_status_enum.create(bind, checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", currency_enum, nullable=False),
        sa.Column("description", sa.String(length=255), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", payment_status_enum, nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("gateway_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("webhook_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_index("ix_payments_status", "payments", ["status"], unique=False)
    op.create_index("ix_payments_gateway_claimed_at", "payments", ["gateway_claimed_at"], unique=False)
    op.create_index("ix_payments_webhook_claimed_at", "payments", ["webhook_claimed_at"], unique=False)

    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("routing_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("headers", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"], unique=False)
    op.create_index("ix_outbox_routing_key", "outbox", ["routing_key"], unique=False)
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["published_at", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished_created_at", table_name="outbox")
    op.drop_index("ix_outbox_routing_key", table_name="outbox")
    op.drop_index("ix_outbox_aggregate_id", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_webhook_claimed_at", table_name="payments")
    op.drop_index("ix_payments_gateway_claimed_at", table_name="payments")
    op.drop_index("ix_payments_status", table_name="payments")
    op.drop_table("payments")

    bind = op.get_bind()
    sa.Enum(name="payment_status_enum").drop(bind, checkfirst=True)
    sa.Enum(name="currency_enum").drop(bind, checkfirst=True)
