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

import pendulum
from sqlalchemy.orm.session import Session

from airflow.utils.state import State


class DepContext:
    """
    A base class for contexts that specifies which dependencies should be evaluated in
    the context for a task instance to satisfy the requirements of the context. Also
    stores state related to the context that can be used by dependency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:

    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param deps: The context-specific dependencies that need to be evaluated for a
        task instance to run in this execution context.
    :type deps: set(airflow.ti_deps.deps.base_ti_dep.BaseTIDep)
    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :type flag_upstream_failed: bool
    :param ignore_all_deps: Whether or not the context should ignore all ignorable
        dependencies. Overrides the other ignore_* parameters
    :type ignore_all_deps: bool
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
        Backfills)
    :type ignore_depends_on_past: bool
    :param ignore_in_retry_period: Ignore the retry period for task instances
    :type ignore_in_retry_period: bool
    :param ignore_in_reschedule_period: Ignore the reschedule period for task instances
    :type ignore_in_reschedule_period: bool
    :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past and
        trigger rule
    :type ignore_task_deps: bool
    :param ignore_ti_state: Ignore the task instance's previous failure/success
    :type ignore_ti_state: bool
    :param finished_tasks: A list of all the finished tasks of this run
    :type finished_tasks: list[airflow.models.TaskInstance]
    """

    def __init__(
        self,
        deps=None,
        flag_upstream_failed: bool = False,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_in_retry_period: bool = False,
        ignore_in_reschedule_period: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        finished_tasks=None,
    ):
        self.deps = deps or set()
        self.flag_upstream_failed = flag_upstream_failed
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_in_retry_period = ignore_in_retry_period
        self.ignore_in_reschedule_period = ignore_in_reschedule_period
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.finished_tasks = finished_tasks

    def ensure_finished_tasks(self, dag, execution_date: pendulum.DateTime, session: Session):
        """
        This method makes sure finished_tasks is populated if it's currently None.
        This is for the strange feature of running tasks without dag_run.

        :param dag: The DAG for which to find finished tasks
        :type dag: airflow.models.DAG
        :param execution_date: The execution_date to look for
        :param session: Database session to use
        :return: A list of all the finished tasks of this DAG and execution_date
        :rtype: list[airflow.models.TaskInstance]
        """
        if self.finished_tasks is None:
            self.finished_tasks = dag.get_task_instances(
                start_date=execution_date,
                end_date=execution_date,
                state=State.finished,
                session=session,
            )
        return self.finished_tasks
