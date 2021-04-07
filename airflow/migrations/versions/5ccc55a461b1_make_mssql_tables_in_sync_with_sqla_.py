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

"""make mssql tables in sync with SQLA models

Revision ID: 5ccc55a461b1
Revises: e9304a3141f0
Create Date: 2021-04-06 14:22:02.197726

"""

from alembic import op
from sqlalchemy.dialects import mssql

# revision identifiers, used by Alembic.
revision = '5ccc55a461b1'
down_revision = 'e9304a3141f0'
branch_labels = None
depends_on = None


def __get_timestamp(conn):
    result = conn.execute(
        """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion"""
    ).fetchone()
    mssql_version = result[0]
    if mssql_version not in ("2000", "2005"):
        return mssql.DATETIME2(precision=6)
    else:
        return mssql.DATETIME


def upgrade():
    """Apply make mssql tables in sync with SQLA models"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.alter_column(
            table_name="xcom", column_name="timestamp", type_=__get_timestamp(conn), nullable=False
        )
        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.alter_column(
                column_name='end_date', type_=__get_timestamp(conn), nullable=False
            )
            task_reschedule_batch_op.alter_column(
                column_name='reschedule_date', type_=__get_timestamp(conn), nullable=False
            )
            task_reschedule_batch_op.alter_column(
                column_name='start_date', type_=__get_timestamp(conn), nullable=False
            )
        with op.batch_alter_table('task_fail') as task_fail_batch_op:
            task_fail_batch_op.drop_index('idx_task_fail_dag_task_date')
            task_fail_batch_op.alter_column(
                column_name="execution_date", type_=__get_timestamp(conn), nullable=False
            )
            task_fail_batch_op.create_index(
                'idx_task_fail_dag_task_date', ['dag_id', 'task_id', 'execution_date'], unique=False
            )
        with op.batch_alter_table('task_instance') as task_instance_batch_op:
            task_instance_batch_op.drop_index('ti_state_lkp')
            task_instance_batch_op.create_index(
                'ti_state_lkp', ['dag_id', 'task_id', 'execution_date', 'state'], unique=False
            )


def downgrade():
    """Unapply make mssql tables in sync with SQLA models"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.alter_column(
            table_name="xcom", column_name="timestamp", type_=__get_timestamp(conn), nullable=True
        )
        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.alter_column(
                column_name='end_date', type_=__get_timestamp(conn), nullable=True
            )
            task_reschedule_batch_op.alter_column(
                column_name='reschedule_date', type_=__get_timestamp(conn), nullable=True
            )
            task_reschedule_batch_op.alter_column(
                column_name='start_date', type_=__get_timestamp(conn), nullable=True
            )
        with op.batch_alter_table('task_fail') as task_fail_batch_op:
            task_fail_batch_op.drop_index('idx_task_fail_dag_task_date')
            task_fail_batch_op.alter_column(
                column_name="execution_date", type_=__get_timestamp(conn), nullable=False
            )
            task_fail_batch_op.create_index(
                'idx_task_fail_dag_task_date', ['dag_id', 'task_id', 'execution_date'], unique=False
            )
        with op.batch_alter_table('task_instance') as task_instance_batch_op:
            task_instance_batch_op.drop_index('ti_state_lkp')
            task_instance_batch_op.create_index(
                'ti_state_lkp', ['dag_id', 'task_id', 'execution_date'], unique=False
            )
