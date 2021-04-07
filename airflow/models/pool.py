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

from sys import maxsize
from typing import Dict, Iterable, Optional, Tuple

from sqlalchemy import Column, Integer, String, Text, func
from sqlalchemy.orm.session import Session

from airflow.exceptions import AirflowException
from airflow.models.base import Base
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.typing_compat import TypedDict
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import nowait, with_row_locks
from airflow.utils.state import State


class PoolStats(TypedDict):
    """Dictionary containing Pool Stats"""

    total: int
    running: int
    queued: int
    open: int


class Pool(Base):
    """the class to get Pool info."""

    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(256), unique=True)
    # -1 for infinite
    slots = Column(Integer, default=0)
    description = Column(Text)

    DEFAULT_POOL_NAME = 'default_pool'

    def __repr__(self):
        return str(self.pool)  # pylint: disable=E0012

    @staticmethod
    @provide_session
    def get_pool(pool_name, session: Session = None):
        """
        Get the Pool with specific pool name from the Pools.

        :param pool_name: The pool name of the Pool to get.
        :param session: SQLAlchemy ORM Session
        :return: the pool object
        """
        return session.query(Pool).filter(Pool.pool == pool_name).first()

    @staticmethod
    @provide_session
    def get_default_pool(session: Session = None):
        """
        Get the Pool of the default_pool from the Pools.

        :param session: SQLAlchemy ORM Session
        :return: the pool object
        """
        return Pool.get_pool(Pool.DEFAULT_POOL_NAME, session=session)

    @staticmethod
    @provide_session
    def slots_stats(
        *,
        lock_rows: bool = False,
        session: Session = None,
    ) -> Dict[str, PoolStats]:
        """
        Get Pool stats (Number of Running, Queued, Open & Total tasks)

        If ``lock_rows`` is True, and the database engine in use supports the ``NOWAIT`` syntax, then a
        non-blocking lock will be attempted -- if the lock is not available then SQLAlchemy will throw an
        OperationalError.

        :param lock_rows: Should we attempt to obtain a row-level lock on all the Pool rows returns
        :param session: SQLAlchemy ORM Session
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        pools: Dict[str, PoolStats] = {}

        query = session.query(Pool.pool, Pool.slots)

        if lock_rows:
            query = with_row_locks(query, session=session, **nowait(session))

        pool_rows: Iterable[Tuple[str, int]] = query.all()
        for (pool_name, total_slots) in pool_rows:
            if total_slots == -1:
                total_slots = maxsize
            pools[pool_name] = PoolStats(total=total_slots, running=0, queued=0, open=0)

        state_count_by_pool = (
            session.query(TaskInstance.pool, TaskInstance.state, func.count())
            .filter(TaskInstance.state.in_(list(EXECUTION_STATES)))
            .group_by(TaskInstance.pool, TaskInstance.state)
        ).all()

        # calculate queued and running metrics
        count: int
        for (pool_name, state, count) in state_count_by_pool:
            stats_dict: Optional[PoolStats] = pools.get(pool_name)
            if not stats_dict:
                continue
            # TypedDict key must be a string literal, so we use if-statements to set value
            if state == "running":
                stats_dict["running"] = count
            elif state == "queued":
                stats_dict["queued"] = count
            else:
                raise AirflowException(f"Unexpected state. Expected values: {EXECUTION_STATES}.")

        # calculate open metric
        for pool_name, stats_dict in pools.items():
            if stats_dict["total"] == -1:
                # -1 means infinite
                stats_dict["open"] = -1
            else:
                stats_dict["open"] = stats_dict["total"] - stats_dict["running"] - stats_dict["queued"]

        return pools

    def to_json(self):
        """
        Get the Pool in a json structure

        :return: the pool object in json format
        """
        return {
            'id': self.id,
            'pool': self.pool,
            'slots': self.slots,
            'description': self.description,
        }

    @provide_session
    def occupied_slots(self, session: Session):
        """
        Get the number of slots used by running/queued tasks at the moment.

        :param session: SQLAlchemy ORM Session
        :return: the used number of slots
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        return (
            session.query(func.sum(TaskInstance.pool_slots))
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state.in_(list(EXECUTION_STATES)))
            .scalar()
        ) or 0

    @provide_session
    def running_slots(self, session: Session):
        """
        Get the number of slots used by running tasks at the moment.

        :param session: SQLAlchemy ORM Session
        :return: the used number of slots
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        return (
            session.query(func.sum(TaskInstance.pool_slots))
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.RUNNING)
            .scalar()
        ) or 0

    @provide_session
    def queued_slots(self, session: Session):
        """
        Get the number of slots used by queued tasks at the moment.

        :param session: SQLAlchemy ORM Session
        :return: the used number of slots
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        return (
            session.query(func.sum(TaskInstance.pool_slots))
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.QUEUED)
            .scalar()
        ) or 0

    @provide_session
    def open_slots(self, session: Session) -> float:
        """
        Get the number of slots open at the moment.

        :param session: SQLAlchemy ORM Session
        :return: the number of slots
        """
        if self.slots == -1:
            return float('inf')
        else:
            return self.slots - self.occupied_slots(session)
