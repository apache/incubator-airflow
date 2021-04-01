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

from airflow.models.taskreschedule import TaskReschedule
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State


class ReadyToRescheduleDep(BaseTIDep):
    """Determines whether a task is ready to be rescheduled."""

    NAME = "Ready To Reschedule"
    IGNORABLE = True
    IS_TASK_DEP = True
    RESCHEDULEABLE_STATES = {State.UP_FOR_RESCHEDULE, State.NONE}

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        """
        Determines whether a task is ready to be rescheduled. Only tasks in
        NONE state with at least one row in task_reschedule table are
        handled by this dependency class, otherwise this dependency is
        considered as passed. This dependency fails if the latest reschedule
        request's reschedule date is still in future.
        """
        if dep_context.ignore_in_reschedule_period:
            yield self._passing_status(
                reason="The context specified that being in a reschedule period was permitted."
            )
            return

        if ti.state not in self.RESCHEDULEABLE_STATES:
            yield self._passing_status(
                reason="The task instance is not in State_UP_FOR_RESCHEDULE or NONE state."
            )
            return

        task_reschedule = (
            TaskReschedule.query_for_task_instance(task_instance=ti, descending=True, session=session)
            .with_entities(TaskReschedule.reschedule_date)
            .first()
        )
        if not task_reschedule:
            yield self._passing_status(reason="There is no reschedule request for this task instance.")
            return

        now = timezone.utcnow()
        next_reschedule_date = task_reschedule.reschedule_date
        if now >= next_reschedule_date:
            yield self._passing_status(reason="Task instance id ready for reschedule.")
            return

        yield self._failing_status(
            reason="Task is not ready for reschedule yet but will be rescheduled "
            "automatically. Current date is {} and task will be rescheduled "
            "at {}.".format(now.isoformat(), next_reschedule_date.isoformat())
        )
