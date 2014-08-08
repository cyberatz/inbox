"""simplify transaction log

Revision ID: 8c2406df6f8
Revises:205f3b199c8c
Create Date: 2014-08-08 01:57:17.144405

"""

# revision identifiers, used by Alembic.
revision = '8c2406df6f8'
down_revision = '205f3b199c8c'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import text


def upgrade():
    conn = op.get_bind()
    conn.execute(text('''
        ALTER TABLE transaction
            CHANGE public_snapshot snapshot LONGTEXT,
            DROP COLUMN private_snapshot,
            DROP COLUMN object_public_id,
            DROP COLUMN delta
        '''))


def downgrade():
    raise Exception("You shouldn't want to roll back from this one, but if "
                    "you do, comment this out")
    op.alter_column('transaction', 'snapshot',
                    new_column_name='public_snapshot')
    op.add_column('transaction', sa.Column('delta', mysql.LONGTEXT(),
                                           nullable=True))
    op.add_column('transaction', sa.Column('private_snapshot',
                                           mysql.LONGTEXT(), nullable=True))
    op.add_column('transaction', sa.Column('object_public_id', sa.String(191),
                                           nullable=True))
