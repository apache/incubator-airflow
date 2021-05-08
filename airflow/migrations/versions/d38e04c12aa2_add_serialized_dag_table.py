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

"""add serialized_dag table

Revision ID: d38e04c12aa2
Revises: 6e96a59344a4
Create Date: 2019-08-01 14:39:35.616417

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'd38e04c12aa2'
down_revision = '6e96a59344a4'
branch_labels = None
depends_on = None


def upgrade():
    """Upgrade version."""
    json_type = sa.JSON
    conn = op.get_bind()  # pylint: disable=no-member

    if conn.dialect.name != "postgresql":
        # Mysql 5.7+/MariaDB 10.2.3 has JSON support. Rather than checking for
        # versions, check for the function existing.
        try:
            conn.execute("SELECT JSON_VALID(1)").fetchone()
        except (sa.exc.OperationalError, sa.exc.ProgrammingError):
            json_type = sa.LargeBinary

    op.create_table(
        'serialized_dag',  # pylint: disable=no-member
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('fileloc', sa.String(length=2000), nullable=False),
        sa.Column('fileloc_hash', sa.Integer(), nullable=False),
        sa.Column('data', json_type(), nullable=False),
        sa.Column('last_updated', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('dag_id'),
    )
    op.create_index('idx_fileloc_hash', 'serialized_dag', ['fileloc_hash'])  # pylint: disable=no-member

    if conn.dialect.name == "mysql":
        conn.execute("SET time_zone = '+00:00'")
        cur = conn.execute("SELECT @@explicit_defaults_for_timestamp")
        res = cur.fetchall()
        if res[0][0] == 0:
            raise Exception("Global variable explicit_defaults_for_timestamp needs to be on (1) for mysql")

        op.alter_column(  # pylint: disable=no-member
            table_name="serialized_dag",
            column_name="last_updated",
            type_=mysql.TIMESTAMP(fsp=6),
            nullable=False,
        )
    else:
        # sqlite and mssql datetime are fine as is.  Therefore, not converting
        if conn.dialect.name in ("sqlite", "mssql"):
            return

        # we try to be database agnostic, but not every db (e.g. sqlserver)
        # supports per session time zones
        if conn.dialect.name == "postgresql":
            conn.execute("set timezone=UTC")

        op.alter_column(  # pylint: disable=no-member
            table_name="serialized_dag",
            column_name="last_updated",
            type_=sa.TIMESTAMP(timezone=True),
        )


def downgrade():
    """Downgrade version."""
    op.drop_table('serialized_dag')  # pylint: disable=no-member
