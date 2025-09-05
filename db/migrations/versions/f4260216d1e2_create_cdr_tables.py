"""create cdr tables

Revision ID: f4260216d1e2
Revises:
Create Date: 2025-09-05 03:05:04.468289

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f4260216d1e2"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema: create CDR tables."""
    op.create_table(
        "cdr_flattened",
        sa.Column("call_id", sa.Text, primary_key=True),
        sa.Column("caller_id", sa.Text),
        sa.Column("caller_location", sa.Text),
        sa.Column("callee_id", sa.Text),
        sa.Column("callee_location", sa.Text),
        sa.Column("start_time", sa.TIMESTAMP(timezone=True)),
        sa.Column("end_time", sa.TIMESTAMP(timezone=True)),
        sa.Column("duration_sec", sa.Integer),
        sa.Column("bytes_used", sa.BigInteger),
        sa.Column("service_type", sa.Text),
        sa.Column("cell_id", sa.Text),
        sa.Column("quality", sa.Text),
        sa.Column("generated_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("source", sa.Text),
    )
    op.create_table(
        "cdr_usage_summary",
        sa.Column("caller_id", sa.Text, nullable=False),
        sa.Column("window_start", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("window_end", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("call_count", sa.BigInteger, nullable=False),
        sa.Column("total_duration_s", sa.BigInteger, nullable=False),
        sa.Column("avg_duration_s", sa.Float, nullable=False),
        sa.Column("total_bytes", sa.BigInteger, nullable=False),
        sa.Column(
            "last_updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("caller_id", "window_start", "window_end"),
    )


def downgrade() -> None:
    """Downgrade schema: drop CDR tables."""
    op.drop_table("cdr_usage_summary")
    op.drop_table("cdr_flattened")
