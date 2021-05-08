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

"""Increase length of pool name

Revision ID: b25a55525161
Revises: bbf4a7ad0465
Create Date: 2020-03-09 08:48:14.534700

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = 'b25a55525161'
down_revision = 'bbf4a7ad0465'
branch_labels = None
depends_on = None


def upgrade():
    """Increase column length of pool name from 50 to 256 characters"""
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table('slot_pool', table_args=sa.UniqueConstraint('pool')) as batch_op:
        batch_op.alter_column('pool', type_=sa.String(250, **COLLATION_ARGS))


def downgrade():
    """Revert Increased length of pool name from 256 to 50 characters"""
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table('slot_pool', table_args=sa.UniqueConstraint('pool')) as batch_op:
        batch_op.alter_column('pool', type_=sa.String(50))
