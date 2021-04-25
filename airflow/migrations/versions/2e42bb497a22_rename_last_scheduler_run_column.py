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

"""rename last_scheduler_run column

Revision ID: 2e42bb497a22
Revises: 8646922c8a04
Create Date: 2021-03-04 19:50:38.880942

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mssql

# revision identifiers, used by Alembic.
revision = '2e42bb497a22'
down_revision = '8646922c8a04'
branch_labels = None
depends_on = None


def upgrade():
    """Apply rename last_scheduler_run column"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table('dag') as batch_op:
            batch_op.alter_column(
                'last_scheduler_run', new_column_name='last_parsed_time', type_=mssql.DATETIME2(precision=6)
            )
    else:
        with op.batch_alter_table('dag') as batch_op:
            batch_op.alter_column(
                'last_scheduler_run', new_column_name='last_parsed_time', type_=sa.TIMESTAMP(timezone=True)
            )


def downgrade():
    """Unapply rename last_scheduler_run column"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table('dag') as batch_op:
            batch_op.alter_column(
                'last_parsed_time', new_column_name='last_scheduler_run', type_=mssql.DATETIME2(precision=6)
            )
    else:
        with op.batch_alter_table('dag') as batch_op:
            batch_op.alter_column(
                'last_parsed_time', new_column_name='last_scheduler_run', type_=sa.TIMESTAMP(timezone=True)
            )
