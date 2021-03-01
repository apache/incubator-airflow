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

"""Add DagTags table

Revision ID: 7939bcff74ba
Revises: fe461863935f
Create Date: 2020-01-07 19:39:01.247442

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = '7939bcff74ba'
down_revision = 'fe461863935f'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add DagTags table"""
    op.create_table(
        'dag_tag',
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.ForeignKeyConstraint(
            ['dag_id'],
            ['dag.dag_id'],
        ),
        sa.PrimaryKeyConstraint('name', 'dag_id'),
    )


def downgrade():
    """Unapply Add DagTags table"""
    op.drop_table('dag_tag')
