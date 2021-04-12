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

"""add description field to variable

Revision ID: e165e7455d70
Revises: 90d1635d7b86
Create Date: 2021-04-11 22:28:02.107290

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'e165e7455d70'
down_revision = '90d1635d7b86'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add description field to variable"""
    with op.batch_alter_table('variable', schema=None) as batch_op:
        batch_op.add_column(sa.Column('description', sa.Text(), nullable=True))


def downgrade():
    """Unapply add description field to variable"""
    with op.batch_alter_table('variable', schema=None) as batch_op:
        batch_op.drop_column('description')
