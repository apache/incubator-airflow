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
from airflow.utils.session import provide_session
from airflow.utils.state import State


class PrevDagrunDep(BaseTIDep):
    """
    Is the past dagrun in a state that allows this task instance to run, e.g. did this
    task instance's task in the previous dagrun complete if we are depending on past.
    """

    NAME = "Previous Dagrun State"
    IGNORABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if dep_context.ignore_depends_on_past:
            yield self._passing_status(
                reason="The context specified that the state of past DAGs could be ignored."
            )
            return

        if not ti.task.depends_on_past:
            yield self._passing_status(reason="The task did not have depends_on_past set.")
            return

        # Don't depend on the previous task instance if we are the first task
        dag = ti.task.dag
        if dag.catchup:
            if dag.previous_schedule(ti.execution_date) is None:
                yield self._passing_status(reason="This task does not have a schedule or is @once")
                return
            if dag.previous_schedule(ti.execution_date) < ti.task.start_date:
                yield self._passing_status(
                    reason="This task instance was the first task instance for its task."
                )
                return
        else:
            dr = ti.get_dagrun(session=session)
            last_dagrun = dr.get_previous_dagrun(session=session) if dr else None

            if not last_dagrun:
                yield self._passing_status(
                    reason="This task instance was the first task instance for its task."
                )
                return

        previous_ti = ti.get_previous_ti(session=session)
        if not previous_ti:
            yield self._failing_status(
                reason="depends_on_past is true for this task's DAG, but the previous "
                "task instance has not run yet."
            )
            return

        if previous_ti.state not in {State.SKIPPED, State.SUCCESS}:
            yield self._failing_status(
                reason="depends_on_past is true for this task, but the previous task "
                "instance {} is in the state '{}' which is not a successful "
                "state.".format(previous_ti, previous_ti.state)
            )

        previous_ti.task = ti.task
        if ti.task.wait_for_downstream and not previous_ti.are_dependents_done(session=session):
            yield self._failing_status(
                reason="The tasks downstream of the previous task instance {} haven't "
                "completed (and wait_for_downstream is True).".format(previous_ti)
            )
