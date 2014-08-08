"""reorder_transaction_columns

Revision ID: 205f3b199c8c
Revises:2b89164aa9cd
Create Date: 2014-09-11 23:45:02.028163

"""

# revision identifiers, used by Alembic.
revision = '205f3b199c8c'
down_revision = '2b89164aa9cd'

from alembic import op
from sqlalchemy.sql import text


def upgrade():
    # Reorder transaction columns so that we can run migration 092 on a read
    # replica.
    conn = op.get_bind()
    conn.execute(text('''
        ALTER TABLE transaction
            MODIFY object_public_id VARCHAR(191) AFTER private_snapshot,
            MODIFY delta LONGTEXT AFTER private_snapshot
        '''))


def downgrade():
    # Nothing to do here.
    pass
