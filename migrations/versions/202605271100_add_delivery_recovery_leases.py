"""add stable event identity and outbox leases"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "202605271100"
down_revision = "202605261430"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("event_id", sa.Uuid(), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE payments AS payment
            SET event_id = (
                SELECT CAST(event.payload ->> 'event_id' AS uuid)
                FROM outbox AS event
                WHERE event.aggregate_id = payment.id
                  AND event.payload ? 'event_id'
                ORDER BY event.created_at ASC
                LIMIT 1
            )
            """
        )
    )
    op.alter_column("payments", "event_id", nullable=False)
    op.create_unique_constraint("uq_payments_event_id", "payments", ["event_id"])

    op.add_column("outbox", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_outbox_claimed_at", "outbox", ["claimed_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_outbox_claimed_at", table_name="outbox")
    op.drop_column("outbox", "claimed_at")
    op.drop_constraint("uq_payments_event_id", "payments", type_="unique")
    op.drop_column("payments", "event_id")
