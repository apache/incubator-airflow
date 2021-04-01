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

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State


class NotInRetryPeriodDep(BaseTIDep):
    """Determines whether a task is not in retry period."""

    NAME = "Not In Retry Period"
    IGNORABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if dep_context.ignore_in_retry_period:
            yield self._passing_status(
                reason="The context specified that being in a retry period was permitted."
            )
            return

        if ti.state != State.UP_FOR_RETRY:
            yield self._passing_status(reason="The task instance was not marked for retrying.")
            return

        # Calculate the date first so that it is always smaller than the timestamp used by
        # ready_for_retry
        cur_date = timezone.utcnow()
        next_task_retry_date = ti.next_retry_datetime()
        if ti.is_premature:
            yield self._failing_status(
                reason="Task is not ready for retry yet but will be retried "
                "automatically. Current date is {} and task will be retried "
                "at {}.".format(cur_date.isoformat(), next_task_retry_date.isoformat())
            )
