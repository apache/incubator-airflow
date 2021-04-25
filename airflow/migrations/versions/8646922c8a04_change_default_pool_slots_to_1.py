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

"""Change default pool_slots to 1

Revision ID: 8646922c8a04
Revises: 449b4072c2da
Create Date: 2021-02-23 23:19:22.409973

"""

import dill
import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column, Float, Integer, PickleType, String

# revision identifiers, used by Alembic.
from sqlalchemy.ext.declarative import declarative_base

from airflow.models.base import COLLATION_ARGS
from airflow.utils.sqlalchemy import UtcDateTime

revision = '8646922c8a04'
down_revision = '449b4072c2da'
branch_labels = None
depends_on = None

Base = declarative_base()
BATCH_SIZE = 5000
ID_LEN = 250


class TaskInstance(Base):  # noqa: D101  # type: ignore
    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50), nullable=False)
    pool_slots = Column(Integer, default=1)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    queued_by_job_id = Column(Integer)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))
    external_executor_id = Column(String(ID_LEN, **COLLATION_ARGS))


def upgrade():
    """Change default pool_slots to 1 and make pool_slots not nullable"""
    connection = op.get_bind()
    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=connection)

    session.query(TaskInstance).filter(TaskInstance.pool_slots.is_(None)).update(
        {TaskInstance.pool_slots: 1}, synchronize_session=False
    )
    session.commit()

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("pool_slots", existing_type=sa.Integer, nullable=False)


def downgrade():
    """Unapply Change default pool_slots to 1"""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("pool_slots", existing_type=sa.Integer, nullable=True)
