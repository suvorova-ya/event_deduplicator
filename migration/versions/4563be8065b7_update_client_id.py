"""update client_id

Revision ID: 4563be8065b7
Revises: d36b63a958d6
Create Date: 2025-05-28 11:55:06.552584

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4563be8065b7'
down_revision: Union[str, None] = 'd36b63a958d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_events_client_id', table_name='events')
    op.create_index(op.f('ix_events_client_id'), 'events', ['client_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_events_client_id'), table_name='events')
    op.create_index('ix_events_client_id', 'events', ['client_id'], unique=True)
    # ### end Alembic commands ###
