#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Add trigger table and task info

Revision ID: 54bebd308c5f
Revises: a13f7613ad25
Create Date: 2021-04-14 12:56:40.688260

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '54bebd308c5f'
down_revision = 'a13f7613ad25'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add trigger table and task info"""
    op.create_table(
        'trigger',
        sa.Column('id', sa.BigInteger(), primary_key=True, nullable=False),
        sa.Column('classpath', sa.String(length=1000), nullable=False),
        sa.Column('kwargs', sa.JSON(), nullable=False),
        sa.Column('created_date', sa.DateTime(), nullable=False),
    )
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.add_column(sa.Column('trigger_id', sa.BigInteger()))
        batch_op.add_column(sa.Column('trigger_timeout', sa.DateTime()))
        batch_op.add_column(sa.Column('next_method', sa.String(length=1000)))
        batch_op.add_column(sa.Column('next_kwargs', sa.JSON()))
        batch_op.create_foreign_key('task_instance_trigger_id_fkey', 'trigger', ['trigger_id'], ['id'])
        batch_op.create_index('ti_trigger_id', ['trigger_id'])


def downgrade():
    """Unapply Add trigger table and task info"""
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.drop_index('ti_trigger_id')
        batch_op.drop_constraint('task_instance_trigger_id_fkey')
        batch_op.drop_column('trigger_id')
        batch_op.drop_column('trigger_timeout')
        batch_op.drop_column('next_method')
        batch_op.drop_column('next_kwargs')
    op.drop_table('trigger')
