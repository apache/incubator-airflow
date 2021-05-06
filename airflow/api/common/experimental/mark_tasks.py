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
"""Marks tasks APIs."""

import datetime
from typing import Iterable

from sqlalchemy import or_

from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.subdag import SubDagOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType


def _create_dagruns(dag, execution_dates, state, run_type):
    """
    Infers from the dates which dag runs need to be created and does so.

    :param dag: the dag to create dag runs for
    :param execution_dates: list of execution dates to evaluate
    :param state: the state to set the dag run to
    :param run_type: The prefix will be used to construct dag run id: {run_id_prefix}__{execution_date}
    :return: newly created and existing dag runs for the execution dates supplied
    """
    # find out if we need to create any dag runs
    dag_runs = DagRun.find(dag_id=dag.dag_id, execution_date=execution_dates)
    dates_to_create = list(set(execution_dates) - {dag_run.execution_date for dag_run in dag_runs})

    for date in dates_to_create:
        dag_run = dag.create_dagrun(
            execution_date=date,
            start_date=timezone.utcnow(),
            external_trigger=False,
            state=state,
            run_type=run_type,
        )
        dag_runs.append(dag_run)

    return dag_runs


@provide_session
def set_state(
    tasks: Iterable[BaseOperator],
    execution_date: datetime.datetime,
    upstream: bool = False,
    downstream: bool = False,
    future: bool = False,
    past: bool = False,
    state: str = State.SUCCESS,
    commit: bool = False,
    session=None,
):  # pylint: disable=too-many-arguments,too-many-locals
    """
    Set the state of a task instance and if needed its relatives. Can set state
    for future tasks (calculated from execution_date) and retroactively
    for past tasks. Will verify integrity of past dag runs in order to create
    tasks that did not exist. It will not create dag runs that are missing
    on the schedule (but it will as for subdag dag runs if needed).

    :param tasks: the iterable of tasks from which to work. task.task.dag needs to be set
    :param execution_date: the execution date from which to start looking
    :param upstream: Mark all parents (upstream tasks)
    :param downstream: Mark all siblings (downstream tasks) of task_id, including SubDags
    :param future: Mark all future tasks on the interval of the dag up until
        last execution date.
    :param past: Retroactively mark all tasks starting from start_date of the DAG
    :param state: State to which the tasks need to be set
    :param commit: Commit tasks to be altered to the database
    :param session: database session
    :return: list of tasks that have been created and updated
    """
    if not tasks:
        return []

    if not timezone.is_localized(execution_date):
        raise ValueError(f"Received non-localized date {execution_date}")

    task_dags = {task.dag for task in tasks}
    if len(task_dags) > 1:
        raise ValueError(f"Received tasks from multiple DAGs: {task_dags}")
    dag = next(iter(task_dags))
    if dag is None:
        raise ValueError("Received tasks with no DAG")

    dates = get_execution_dates(dag, execution_date, future, past)

    task_ids = list(find_task_relatives(tasks, downstream, upstream))

    confirmed_dates = verify_dag_run_integrity(dag, dates)

    sub_dag_run_ids = get_subdag_runs(dag, session, state, task_ids, commit, confirmed_dates)

    # now look for the task instances that are affected

    qry_dag = get_all_dag_task_query(dag, session, state, task_ids, confirmed_dates)

    if commit:
        tis_altered = qry_dag.with_for_update().all()
        if sub_dag_run_ids:
            qry_sub_dag = all_subdag_tasks_query(sub_dag_run_ids, session, state, confirmed_dates)
            tis_altered += qry_sub_dag.with_for_update().all()
        for task_instance in tis_altered:
            task_instance.state = state
            if state in State.finished:
                task_instance.end_date = timezone.utcnow()
                task_instance.set_duration()
    else:
        tis_altered = qry_dag.all()
        if sub_dag_run_ids:
            qry_sub_dag = all_subdag_tasks_query(sub_dag_run_ids, session, state, confirmed_dates)
            tis_altered += qry_sub_dag.all()

    return tis_altered


# Flake and pylint disagree about correct indents here
def all_subdag_tasks_query(sub_dag_run_ids, session, state, confirmed_dates):  # noqa: E123
    """Get *all* tasks of the sub dags"""
    qry_sub_dag = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id.in_(sub_dag_run_ids), TaskInstance.execution_date.in_(confirmed_dates))
        .filter(or_(TaskInstance.state.is_(None), TaskInstance.state != state))
    )  # noqa: E123
    return qry_sub_dag


def get_all_dag_task_query(dag, session, state, task_ids, confirmed_dates):
    """Get all tasks of the main dag that will be affected by a state change"""
    qry_dag = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.execution_date.in_(confirmed_dates),
            TaskInstance.task_id.in_(task_ids),  # noqa: E123
        )
        .filter(or_(TaskInstance.state.is_(None), TaskInstance.state != state))
    )
    return qry_dag


def get_subdag_runs(dag, session, state, task_ids, commit, confirmed_dates):
    """Go through subdag operators and create dag runs. We will only work
    within the scope of the subdag. We won't propagate to the parent dag,
    but we will propagate from parent to subdag.
    """
    dags = [dag]
    sub_dag_ids = []
    while dags:
        current_dag = dags.pop()
        for task_id in task_ids:
            if not current_dag.has_task(task_id):
                continue

            current_task = current_dag.get_task(task_id)
            if isinstance(current_task, SubDagOperator) or current_task.task_type == "SubDagOperator":
                # this works as a kind of integrity check
                # it creates missing dag runs for subdag operators,
                # maybe this should be moved to dagrun.verify_integrity
                dag_runs = _create_dagruns(
                    current_task.subdag,
                    execution_dates=confirmed_dates,
                    state=State.RUNNING,
                    run_type=DagRunType.BACKFILL_JOB,
                )

                verify_dagruns(dag_runs, commit, state, session, current_task)

                dags.append(current_task.subdag)
                sub_dag_ids.append(current_task.subdag.dag_id)
    return sub_dag_ids


def verify_dagruns(dag_runs, commit, state, session, current_task):
    """Verifies integrity of dag_runs.

    :param dag_runs: dag runs to verify
    :param commit: whether dag runs state should be updated
    :param state: state of the dag_run to set if commit is True
    :param session: session to use
    :param current_task: current task
    :return:
    """
    for dag_run in dag_runs:
        dag_run.dag = current_task.subdag
        dag_run.verify_integrity()
        if commit:
            dag_run.state = state
            session.merge(dag_run)


def verify_dag_run_integrity(dag, dates):
    """
    Verify the integrity of the dag runs in case a task was added or removed
    set the confirmed execution dates as they might be different
    from what was provided
    """
    confirmed_dates = []
    dag_runs = DagRun.find(dag_id=dag.dag_id, execution_date=dates)
    for dag_run in dag_runs:
        dag_run.dag = dag
        dag_run.verify_integrity()
        confirmed_dates.append(dag_run.execution_date)
    return confirmed_dates


def find_task_relatives(tasks, downstream, upstream):
    """Yield task ids and optionally ancestor and descendant ids."""
    for task in tasks:
        yield task.task_id
        if downstream:
            for relative in task.get_flat_relatives(upstream=False):
                yield relative.task_id
        if upstream:
            for relative in task.get_flat_relatives(upstream=True):
                yield relative.task_id


def get_execution_dates(dag, execution_date, future, past):
    """Returns dates of DAG execution"""
    latest_execution_date = dag.get_latest_execution_date()
    if latest_execution_date is None:
        raise ValueError(f"Received non-localized date {execution_date}")
    # determine date range of dag runs and tasks to consider
    end_date = latest_execution_date if future else execution_date
    if 'start_date' in dag.default_args:
        start_date = dag.default_args['start_date']
    elif dag.start_date:
        start_date = dag.start_date
    else:
        start_date = execution_date
    start_date = execution_date if not past else start_date
    if dag.schedule_interval == '@once':
        dates = [start_date]
    elif not dag.schedule_interval:
        # If schedule_interval is None, need to look at existing DagRun if the user wants future or
        # past runs.
        dag_runs = dag.get_dagruns_between(start_date=start_date, end_date=end_date)
        dates = sorted({d.execution_date for d in dag_runs})
    else:
        dates = dag.date_range(start_date=start_date, end_date=end_date)
    return dates


@provide_session
def _set_dag_run_state(dag_id, execution_date, state, session=None):
    """
    Helper method that set dag run state in the DB.

    :param dag_id: dag_id of target dag run
    :param execution_date: the execution date from which to start looking
    :param state: target state
    :param session: database session
    """
    dag_run = (
        session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date).one()
    )
    dag_run.state = state
    if state == State.RUNNING:
        dag_run.start_date = timezone.utcnow()
        dag_run.end_date = None
    else:
        dag_run.end_date = timezone.utcnow()
    session.merge(dag_run)


@provide_session
def set_dag_run_state_to_success(dag, execution_date, commit=False, session=None):
    """
    Set the dag run for a specific execution date and its task instances
    to success.

    :param dag: the DAG of which to alter state
    :param execution_date: the execution date from which to start looking
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    :raises: ValueError if dag or execution_date is invalid
    """
    if not dag or not execution_date:
        return []

    # Mark the dag run to success.
    if commit:
        _set_dag_run_state(dag.dag_id, execution_date, State.SUCCESS, session)

    # Mark all task instances of the dag run to success.
    for task in dag.tasks:
        task.dag = dag
    return set_state(
        tasks=dag.tasks, execution_date=execution_date, state=State.SUCCESS, commit=commit, session=session
    )


@provide_session
def set_dag_run_state_to_failed(dag, execution_date, commit=False, session=None):
    """
    Set the dag run for a specific execution date and its running task instances
    to failed.

    :param dag: the DAG of which to alter state
    :param execution_date: the execution date from which to start looking
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    :raises: AssertionError if dag or execution_date is invalid
    """
    if not dag or not execution_date:
        return []

    # Mark the dag run to failed.
    if commit:
        _set_dag_run_state(dag.dag_id, execution_date, State.FAILED, session)

    # Mark only RUNNING task instances.
    task_ids = [task.task_id for task in dag.tasks]
    tis = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.execution_date == execution_date,
            TaskInstance.task_id.in_(task_ids),
        )
        .filter(TaskInstance.state == State.RUNNING)
    )
    task_ids_of_running_tis = [task_instance.task_id for task_instance in tis]

    tasks = []
    for task in dag.tasks:
        if task.task_id not in task_ids_of_running_tis:
            continue
        task.dag = dag
        tasks.append(task)

    return set_state(
        tasks=tasks, execution_date=execution_date, state=State.FAILED, commit=commit, session=session
    )


@provide_session
def set_dag_run_state_to_running(dag, execution_date, commit=False, session=None):
    """
    Set the dag run for a specific execution date to running.

    :param dag: the DAG of which to alter state
    :param execution_date: the execution date from which to start looking
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    """
    res = []
    if not dag or not execution_date:
        return res

    # Mark the dag run to running.
    if commit:
        _set_dag_run_state(dag.dag_id, execution_date, State.RUNNING, session)

    # To keep the return type consistent with the other similar functions.
    return res
