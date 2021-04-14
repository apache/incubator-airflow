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

"""Increase size of connection.extra field to handle multiple RSA keys

Revision ID: 449b4072c2da
Revises: 82b7c48c147f
Create Date: 2020-03-16 19:02:55.337710

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '449b4072c2da'
down_revision = '82b7c48c147f'
branch_labels = None
depends_on = None


def upgrade():
    """Apply increase_length_for_connection_password"""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column(
            'extra',
            existing_type=sa.VARCHAR(length=5000),
            type_=sa.TEXT(),
            existing_nullable=True,
        )


def downgrade():
    """Unapply increase_length_for_connection_password"""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column(
            'extra',
            existing_type=sa.TEXT(),
            type_=sa.VARCHAR(length=5000),
            existing_nullable=True,
        )
