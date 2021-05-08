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

"""add dag_stats table

Revision ID: f2ca10b85618
Revises: 64de9cddf6c9
Create Date: 2016-07-20 15:08:28.247537

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = 'f2ca10b85618'
down_revision = '64de9cddf6c9'
branch_labels = None
depends_on = None


def upgrade():  # noqa: D103
    op.create_table(
        'dag_stats',
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('state', sa.String(length=50), nullable=False),
        sa.Column('count', sa.Integer(), nullable=False, default=0),
        sa.Column('dirty', sa.Boolean(), nullable=False, default=False),
        sa.PrimaryKeyConstraint('dag_id', 'state'),
    )


def downgrade():  # noqa: D103
    op.drop_table('dag_stats')
