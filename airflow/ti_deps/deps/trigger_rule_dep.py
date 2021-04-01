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

from collections import Counter

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule as TR


class TriggerRuleDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """

    NAME = "Trigger Rule"
    IGNORABLE = True
    IS_TASK_DEP = True

    @staticmethod
    def _get_states_count_upstream_ti(ti, finished_tasks):
        """
        This function returns the states of the upstream tis for a specific ti in order to determine
        whether this ti can run in this iteration

        :param ti: the ti that we want to calculate deps for
        :type ti: airflow.models.TaskInstance
        :param finished_tasks: all the finished tasks of the dag_run
        :type finished_tasks: list[airflow.models.TaskInstance]
        """
        counter = Counter(task.state for task in finished_tasks if task.task_id in ti.task.upstream_task_ids)
        return (
            counter.get(State.SUCCESS, 0),
            counter.get(State.SKIPPED, 0),
            counter.get(State.FAILED, 0),
            counter.get(State.UPSTREAM_FAILED, 0),
            sum(counter.values()),
        )

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list:
            yield self._passing_status(reason="The task instance did not have any upstream tasks.")
            return

        if ti.task.trigger_rule == TR.DUMMY:
            yield self._passing_status(reason="The task had a dummy trigger rule set.")
            return
        # see if the task name is in the task upstream for our task
        successes, skipped, failed, upstream_failed, done = self._get_states_count_upstream_ti(
            ti=ti, finished_tasks=dep_context.ensure_finished_tasks(ti.task.dag, ti.execution_date, session)
        )

        yield from self._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            done=done,
            flag_upstream_failed=dep_context.flag_upstream_failed,
            session=session,
        )

    @provide_session
    def _evaluate_trigger_rule(  # pylint: disable=too-many-branches
        self, ti, successes, skipped, failed, upstream_failed, done, flag_upstream_failed, session
    ):
        """
        Yields a dependency status that indicate whether the given task instance's trigger
        rule was met.

        :param ti: the task instance to evaluate the trigger rule of
        :type ti: airflow.models.TaskInstance
        :param successes: Number of successful upstream tasks
        :type successes: int
        :param skipped: Number of skipped upstream tasks
        :type skipped: int
        :param failed: Number of failed upstream tasks
        :type failed: int
        :param upstream_failed: Number of upstream_failed upstream tasks
        :type upstream_failed: int
        :param done: Number of completed upstream tasks
        :type done: int
        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :type flag_upstream_failed: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        task = ti.task
        upstream = len(task.upstream_task_ids)
        trigger_rule = task.trigger_rule
        upstream_done = done >= upstream
        upstream_tasks_state = {
            "total": upstream,
            "successes": successes,
            "skipped": skipped,
            "failed": failed,
            "upstream_failed": upstream_failed,
            "done": done,
        }
        # TODO(aoen): Ideally each individual trigger rules would be its own class, but
        # this isn't very feasible at the moment since the database queries need to be
        # bundled together for efficiency.
        # handling instant state assignment based on trigger rules
        if flag_upstream_failed:
            if trigger_rule == TR.ALL_SUCCESS:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped:
                    ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.ALL_FAILED:
                if successes or skipped:
                    ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.ONE_SUCCESS:
                if upstream_done and not successes:
                    ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.ONE_FAILED:
                if upstream_done and not (failed or upstream_failed):
                    ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.NONE_FAILED:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
            elif trigger_rule == TR.NONE_FAILED_OR_SKIPPED:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped == upstream:
                    ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.NONE_SKIPPED:
                if skipped:
                    ti.set_state(State.SKIPPED, session)

        if trigger_rule == TR.ONE_SUCCESS:
            if successes <= 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires one upstream "
                    "task success, but none were found. "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.ONE_FAILED:
            if not failed and not upstream_failed:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires one upstream "
                    "task failure, but none were found. "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.ALL_SUCCESS:
            num_failures = upstream - successes
            if num_failures > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires all upstream "
                    "tasks to have succeeded, but found {} non-success(es). "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, num_failures, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.ALL_FAILED:
            num_successes = upstream - failed - upstream_failed
            if num_successes > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires all upstream "
                    "tasks to have failed, but found {} non-failure(s). "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, num_successes, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.ALL_DONE:
            if not upstream_done:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires all upstream "
                    "tasks to have completed, but found {} task(s) that "
                    "were not done. upstream_tasks_state={}, "
                    "upstream_task_ids={}".format(
                        trigger_rule, upstream_done, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.NONE_FAILED:
            num_failures = upstream - successes - skipped
            if num_failures > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires all upstream "
                    "tasks to have succeeded or been skipped, but found {} non-success(es). "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, num_failures, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.NONE_FAILED_OR_SKIPPED:
            num_failures = upstream - successes - skipped
            if num_failures > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires all upstream "
                    "tasks to have succeeded or been skipped, but found {} non-success(es). "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, num_failures, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        elif trigger_rule == TR.NONE_SKIPPED:
            if not upstream_done or (skipped > 0):
                yield self._failing_status(
                    reason="Task's trigger rule '{}' requires all upstream "
                    "tasks to not have been skipped, but found {} task(s) skipped. "
                    "upstream_tasks_state={}, upstream_task_ids={}".format(
                        trigger_rule, skipped, upstream_tasks_state, task.upstream_task_ids
                    )
                )
        else:
            yield self._failing_status(reason=f"No strategy to evaluate trigger rule '{trigger_rule}'.")
