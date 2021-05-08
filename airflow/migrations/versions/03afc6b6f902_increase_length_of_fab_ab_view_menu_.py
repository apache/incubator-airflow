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

"""Increase length of FAB ab_view_menu.name column

Revision ID: 03afc6b6f902
Revises: 92c57b58940d
Create Date: 2020-11-13 22:21:41.619565

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = '03afc6b6f902'
down_revision = '92c57b58940d'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Increase length of FAB ab_view_menu.name column"""
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()

    if "ab_view_menu" in tables:
        if conn.dialect.name == "sqlite":
            op.execute("PRAGMA foreign_keys=off")
            op.execute(
                """
            CREATE TABLE IF NOT EXISTS ab_view_menu_dg_tmp
            (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(250) NOT NULL UNIQUE
            );
            """
            )
            op.execute("INSERT INTO ab_view_menu_dg_tmp(id, name) select id, name from ab_view_menu;")
            op.execute("DROP TABLE ab_view_menu")
            op.execute("ALTER TABLE ab_view_menu_dg_tmp rename to ab_view_menu;")
            op.execute("PRAGMA foreign_keys=on")
        else:
            op.alter_column(
                table_name='ab_view_menu', column_name='name', type_=sa.String(length=250, **COLLATION_ARGS), nullable=False
            )


def downgrade():
    """Unapply Increase length of FAB ab_view_menu.name column"""
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if "ab_view_menu" in tables:
        if conn.dialect.name == "sqlite":
            op.execute("PRAGMA foreign_keys=off")
            op.execute(
                """
                CREATE TABLE IF NOT EXISTS ab_view_menu_dg_tmp
                (
                    id INTEGER NOT NULL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE
                );
                """
            )
            op.execute("INSERT INTO ab_view_menu_dg_tmp(id, name) select id, name from ab_view_menu;")
            op.execute("DROP TABLE ab_view_menu")
            op.execute("ALTER TABLE ab_view_menu_dg_tmp rename to ab_view_menu;")
            op.execute("PRAGMA foreign_keys=on")
        else:
            op.alter_column(
                table_name='ab_view_menu', column_name='name', type_=sa.String(length=100), nullable=False
            )
