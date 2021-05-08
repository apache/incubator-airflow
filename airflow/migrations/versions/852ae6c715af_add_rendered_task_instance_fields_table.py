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

"""Add RenderedTaskInstanceFields table

Revision ID: 852ae6c715af
Revises: a4c2fd67d16b
Create Date: 2020-03-10 22:19:18.034961

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = '852ae6c715af'
down_revision = 'a4c2fd67d16b'
branch_labels = None
depends_on = None

TABLE_NAME = 'rendered_task_instance_fields'


def upgrade():
    """Apply Add RenderedTaskInstanceFields table"""
    json_type = sa.JSON
    conn = op.get_bind()  # pylint: disable=no-member

    if conn.dialect.name != "postgresql":
        # Mysql 5.7+/MariaDB 10.2.3 has JSON support. Rather than checking for
        # versions, check for the function existing.
        try:
            conn.execute("SELECT JSON_VALID(1)").fetchone()
        except (sa.exc.OperationalError, sa.exc.ProgrammingError):
            json_type = sa.Text

    op.create_table(
        TABLE_NAME,  # pylint: disable=no-member
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('task_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('execution_date', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('rendered_fields', json_type(), nullable=False),
        sa.PrimaryKeyConstraint('dag_id', 'task_id', 'execution_date'),
    )


def downgrade():
    """Drop RenderedTaskInstanceFields table"""
    op.drop_table(TABLE_NAME)  # pylint: disable=no-member
