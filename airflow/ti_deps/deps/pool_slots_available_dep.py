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

"""This module defines dep for pool slots availability"""

from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session


class PoolSlotsAvailableDep(BaseTIDep):
    """Dep for pool slots availability."""

    NAME = "Pool Slots Available"
    IGNORABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        """
        Determines if the pool task instance is in has available slots

        :param ti: the task instance to get the dependency status for
        :type ti: airflow.models.TaskInstance
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        :return: True if there are available slots in the pool.
        """
        from airflow.models.pool import Pool  # To avoid a circular dependency

        pool_name = ti.pool

        pools = session.query(Pool).filter(Pool.pool == pool_name).all()
        if not pools:
            yield self._failing_status(
                reason=("Tasks using non-existent pool '%s' will not be scheduled", pool_name)
            )
            return
        else:
            # Controlled by UNIQUE key in slot_pool table,
            # only one result can be returned.
            open_slots = pools[0].open_slots()

        if ti.state in EXECUTION_STATES:
            open_slots += ti.pool_slots

        if open_slots <= (ti.pool_slots - 1):
            yield self._failing_status(
                reason=(
                    "Not scheduling since there are %s open slots in pool %s and require %s pool slots",
                    open_slots,
                    pool_name,
                    ti.pool_slots,
                )
            )
        else:
            yield self._passing_status(
                reason=("There are enough open slots in %s to execute the task", pool_name)
            )
