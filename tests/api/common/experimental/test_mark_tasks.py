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

import unittest
from datetime import timedelta

import pytest

from airflow import models
from airflow.api.common.experimental.mark_tasks import (
    _create_dagruns,
    set_dag_run_state_to_failed,
    set_dag_run_state_to_running,
    set_dag_run_state_to_success,
    set_state,
)
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs

DEV_NULL = "/dev/null"


class TestMarkTasks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        models.DagBag(include_examples=True, read_dags_from_db=False).sync_to_db()
        dagbag = models.DagBag(include_examples=False, read_dags_from_db=True)
        cls.dag1 = dagbag.get_dag('miscellaneous_test_dag')
        cls.dag2 = dagbag.get_dag('example_subdag_operator')
        cls.dag3 = dagbag.get_dag('example_trigger_target_dag')
        cls.execution_dates = [days_ago(2), days_ago(1)]
        start_date3 = cls.dag3.start_date
        cls.dag3_execution_dates = [
            start_date3,
            start_date3 + timedelta(days=1),
            start_date3 + timedelta(days=2),
        ]

    def setUp(self):
        clear_db_runs()
        drs = _create_dagruns(
            self.dag1, self.execution_dates, state=State.RUNNING, run_type=DagRunType.SCHEDULED
        )
        for dr in drs:
            dr.dag = self.dag1

        drs = _create_dagruns(
            self.dag2, [self.dag2.start_date], state=State.RUNNING, run_type=DagRunType.SCHEDULED
        )

        for dr in drs:
            dr.dag = self.dag2

        drs = _create_dagruns(
            self.dag3, self.dag3_execution_dates, state=State.SUCCESS, run_type=DagRunType.MANUAL
        )
        for dr in drs:
            dr.dag = self.dag3

    def tearDown(self):
        clear_db_runs()

    @staticmethod
    def snapshot_state(dag, execution_dates):
        TI = models.TaskInstance
        with create_session() as session:
            return (
                session.query(TI)
                .filter(TI.dag_id == dag.dag_id, TI.execution_date.in_(execution_dates))
                .all()
            )

    @provide_session
    def verify_state(self, dag, task_ids, execution_dates, state, old_tis, session=None):
        TI = models.TaskInstance

        tis = session.query(TI).filter(TI.dag_id == dag.dag_id, TI.execution_date.in_(execution_dates)).all()

        assert len(tis) > 0

        for ti in tis:  # pylint: disable=too-many-nested-blocks
            assert ti.operator == dag.get_task(ti.task_id).task_type
            if ti.task_id in task_ids and ti.execution_date in execution_dates:
                assert ti.state == state
                if state in State.finished:
                    assert ti.end_date is not None
            else:
                for old_ti in old_tis:
                    if old_ti.task_id == ti.task_id and old_ti.execution_date == ti.execution_date:
                        assert ti.state == old_ti.state

    def test_mark_tasks_now(self):
        # set one task to success but do not commit
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=False,
        )
        assert len(altered) == 1
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]], None, snapshot)

        # set one and only one task to success
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 1
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]], State.SUCCESS, snapshot)

        # set no tasks
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 0
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]], State.SUCCESS, snapshot)

        # set task to other than success
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.FAILED,
            commit=True,
        )
        assert len(altered) == 1
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]], State.FAILED, snapshot)

        # don't alter other tasks
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_0")
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 1
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]], State.SUCCESS, snapshot)

        # set one task as FAILED. dag3 has schedule_interval None
        snapshot = TestMarkTasks.snapshot_state(self.dag3, self.dag3_execution_dates)
        task = self.dag3.get_task("run_this")
        altered = set_state(
            tasks=[task],
            execution_date=self.dag3_execution_dates[1],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.FAILED,
            commit=True,
        )
        # exactly one TaskInstance should have been altered
        assert len(altered) == 1
        # task should have been marked as failed
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[1]], State.FAILED, snapshot)
        # tasks on other days should be unchanged
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[0]], None, snapshot)
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[2]], None, snapshot)

    def test_mark_downstream(self):
        # test downstream
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        relatives = task.get_flat_relatives(upstream=False)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=True,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 3
        self.verify_state(self.dag1, task_ids, [self.execution_dates[0]], State.SUCCESS, snapshot)

    def test_mark_upstream(self):
        # test upstream
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("run_after_loop")
        relatives = task.get_flat_relatives(upstream=True)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=True,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 4
        self.verify_state(self.dag1, task_ids, [self.execution_dates[0]], State.SUCCESS, snapshot)

    def test_mark_tasks_future(self):
        # set one task to success towards end of scheduled dag runs
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=True,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 2
        self.verify_state(self.dag1, [task.task_id], self.execution_dates, State.SUCCESS, snapshot)

        snapshot = TestMarkTasks.snapshot_state(self.dag3, self.dag3_execution_dates)
        task = self.dag3.get_task("run_this")
        altered = set_state(
            tasks=[task],
            execution_date=self.dag3_execution_dates[1],
            upstream=False,
            downstream=False,
            future=True,
            past=False,
            state=State.FAILED,
            commit=True,
        )
        assert len(altered) == 2
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[0]], None, snapshot)
        self.verify_state(self.dag3, [task.task_id], self.dag3_execution_dates[1:], State.FAILED, snapshot)

    def test_mark_tasks_past(self):
        # set one task to success towards end of scheduled dag runs
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[1],
            upstream=False,
            downstream=False,
            future=False,
            past=True,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 2
        self.verify_state(self.dag1, [task.task_id], self.execution_dates, State.SUCCESS, snapshot)

        snapshot = TestMarkTasks.snapshot_state(self.dag3, self.dag3_execution_dates)
        task = self.dag3.get_task("run_this")
        altered = set_state(
            tasks=[task],
            execution_date=self.dag3_execution_dates[1],
            upstream=False,
            downstream=False,
            future=False,
            past=True,
            state=State.FAILED,
            commit=True,
        )
        assert len(altered) == 2
        self.verify_state(self.dag3, [task.task_id], self.dag3_execution_dates[:2], State.FAILED, snapshot)
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[2]], None, snapshot)

    def test_mark_tasks_multiple(self):
        # set multiple tasks to success
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        tasks = [self.dag1.get_task("runme_1"), self.dag1.get_task("runme_2")]
        altered = set_state(
            tasks=tasks,
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 2
        self.verify_state(
            self.dag1, [task.task_id for task in tasks], [self.execution_dates[0]], State.SUCCESS, snapshot
        )

    # TODO: this backend should be removed once a fixing solution is found later
    #       We skip it here because this test case is working with Postgres & SQLite
    #       but not with MySQL
    @pytest.mark.backend("sqlite", "postgres")
    def test_mark_tasks_subdag(self):
        # set one task to success towards end of scheduled dag runs
        task = self.dag2.get_task("section-1")
        relatives = task.get_flat_relatives(upstream=False)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(
            tasks=[task],
            execution_date=self.execution_dates[0],
            upstream=False,
            downstream=True,
            future=False,
            past=False,
            state=State.SUCCESS,
            commit=True,
        )
        assert len(altered) == 14

        # cannot use snapshot here as that will require drilling down the
        # sub dag tree essentially recreating the same code as in the
        # tested logic.
        self.verify_state(self.dag2, task_ids, [self.execution_dates[0]], State.SUCCESS, [])


class TestMarkDAGRun(unittest.TestCase):
    INITIAL_TASK_STATES = {
        'runme_0': State.SUCCESS,
        'runme_1': State.SKIPPED,
        'runme_2': State.UP_FOR_RETRY,
        'also_run_this': State.QUEUED,
        'run_after_loop': State.RUNNING,
        'run_this_last': State.FAILED,
    }

    @classmethod
    def setUpClass(cls):
        dagbag = models.DagBag(include_examples=True, read_dags_from_db=False)
        cls.dag1 = dagbag.dags['miscellaneous_test_dag']
        cls.dag1.sync_to_db()
        cls.dag2 = dagbag.dags['example_subdag_operator']
        cls.dag2.sync_to_db()
        cls.execution_dates = [days_ago(2), days_ago(1), days_ago(0)]

    def setUp(self):
        clear_db_runs()

    def _get_num_tasks_with_starting_state(self, state: State, inclusion: bool):
        """
        If ``inclusion=True``, get num tasks with initial state ``state``.
        Otherwise, get number tasks with initial state not equal to ``state``
        :param state: State to compare against
        :param inclusion: whether to look for inclusion or exclusion
        :return: number of tasks meeting criteria
        """
        states = self.INITIAL_TASK_STATES.values()

        def compare(x, y):
            return x == y if inclusion else x != y

        return len([s for s in states if compare(s, state)])

    def _set_default_task_instance_states(self, dr):
        for task_id, state in self.INITIAL_TASK_STATES.items():
            dr.get_task_instance(task_id).set_state(state)

    def _verify_task_instance_states_remain_default(self, dr):
        for task_id, state in self.INITIAL_TASK_STATES.items():
            assert dr.get_task_instance(task_id).state == state

    @provide_session
    def _verify_task_instance_states(self, dag, date, state, session=None):
        TI = models.TaskInstance
        tis = session.query(TI).filter(TI.dag_id == dag.dag_id, TI.execution_date == date)
        for ti in tis:
            assert ti.state == state

    def _create_test_dag_run(self, state, date):
        return self.dag1.create_dagrun(run_type=DagRunType.MANUAL, state=state, execution_date=date)

    def _verify_dag_run_state(self, dag, date, state):
        drs = models.DagRun.find(dag_id=dag.dag_id, execution_date=date)
        dr = drs[0]

        assert dr.get_state() == state

    @provide_session
    def _verify_dag_run_dates(self, dag, date, state, middle_time, session=None):
        # When target state is RUNNING, we should set start_date,
        # otherwise we should set end_date.
        DR = DagRun
        dr = session.query(DR).filter(DR.dag_id == dag.dag_id, DR.execution_date == date).one()
        if state == State.RUNNING:
            # Since the DAG is running, the start_date must be updated after creation
            assert dr.start_date > middle_time
            # If the dag is still running, we don't have an end date
            assert dr.end_date is None
        else:
            # If the dag is not running, there must be an end time
            assert dr.start_date < middle_time
            assert dr.end_date > middle_time

    def test_set_running_dag_run_to_success(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_success(self.dag1, date, commit=True)

        # All except the SUCCESS task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.SUCCESS, inclusion=False)
        assert len(altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.SUCCESS)
        self._verify_task_instance_states(self.dag1, date, State.SUCCESS)
        self._verify_dag_run_dates(self.dag1, date, State.SUCCESS, middle_time)

    def test_set_running_dag_run_to_failed(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_failed(self.dag1, date, commit=True)
        # Only running task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.RUNNING, inclusion=True)
        assert len(altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.FAILED)
        assert dr.get_task_instance('run_after_loop').state == State.FAILED
        self._verify_dag_run_dates(self.dag1, date, State.FAILED, middle_time)

    def test_set_running_dag_run_to_running(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_running(self.dag1, date, commit=True)

        # None of the tasks should be altered, only the dag itself
        assert len(altered) == 0
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)
        self._verify_dag_run_dates(self.dag1, date, State.RUNNING, middle_time)

    def test_set_success_dag_run_to_success(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_success(self.dag1, date, commit=True)

        # All except the SUCCESS task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.SUCCESS, inclusion=False)
        assert len(altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.SUCCESS)
        self._verify_task_instance_states(self.dag1, date, State.SUCCESS)
        self._verify_dag_run_dates(self.dag1, date, State.SUCCESS, middle_time)

    def test_set_success_dag_run_to_failed(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_failed(self.dag1, date, commit=True)
        # Only running task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.RUNNING, inclusion=True)
        assert len(altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.FAILED)
        assert dr.get_task_instance('run_after_loop').state == State.FAILED
        self._verify_dag_run_dates(self.dag1, date, State.FAILED, middle_time)

    def test_set_success_dag_run_to_running(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_running(self.dag1, date, commit=True)

        # None of the tasks should be altered, but only the dag object should be changed
        assert len(altered) == 0
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)
        self._verify_dag_run_dates(self.dag1, date, State.RUNNING, middle_time)

    def test_set_failed_dag_run_to_success(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_success(self.dag1, date, commit=True)

        # All except the SUCCESS task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.SUCCESS, inclusion=False)
        assert len(altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.SUCCESS)
        self._verify_task_instance_states(self.dag1, date, State.SUCCESS)
        self._verify_dag_run_dates(self.dag1, date, State.SUCCESS, middle_time)

    def test_set_failed_dag_run_to_failed(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_failed(self.dag1, date, commit=True)

        # Only running task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.RUNNING, inclusion=True)
        assert len(altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.FAILED)
        assert dr.get_task_instance('run_after_loop').state == State.FAILED
        self._verify_dag_run_dates(self.dag1, date, State.FAILED, middle_time)

    def test_set_failed_dag_run_to_running(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_running(self.dag1, date, commit=True)

        # None of the tasks should be altered, since we've only altered the DAG itself
        assert len(altered) == 0
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)
        self._verify_dag_run_dates(self.dag1, date, State.RUNNING, middle_time)

    def test_set_state_without_commit(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        self._set_default_task_instance_states(dr)

        will_be_altered = set_dag_run_state_to_running(self.dag1, date, commit=False)

        # None of the tasks will be altered.
        assert len(will_be_altered) == 0
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)

        will_be_altered = set_dag_run_state_to_failed(self.dag1, date, commit=False)

        # Only the running task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.RUNNING, inclusion=True)
        assert len(will_be_altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)

        will_be_altered = set_dag_run_state_to_success(self.dag1, date, commit=False)

        # All except the SUCCESS task should be altered.
        expected = self._get_num_tasks_with_starting_state(State.SUCCESS, inclusion=False)
        assert len(will_be_altered) == expected
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)

    @provide_session
    def test_set_state_with_multiple_dagruns(self, session=None):
        self.dag2.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.FAILED,
            execution_date=self.execution_dates[0],
            session=session,
        )
        self.dag2.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.FAILED,
            execution_date=self.execution_dates[1],
            session=session,
        )
        self.dag2.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=self.execution_dates[2],
            session=session,
        )

        altered = set_dag_run_state_to_success(self.dag2, self.execution_dates[1], commit=True)

        # Recursively count number of tasks in the dag
        def count_dag_tasks(dag):
            count = len(dag.tasks)
            subdag_counts = [count_dag_tasks(subdag) for subdag in dag.subdags]
            count += sum(subdag_counts)
            return count

        assert len(altered) == count_dag_tasks(self.dag2)
        self._verify_dag_run_state(self.dag2, self.execution_dates[1], State.SUCCESS)

        # Make sure other dag status are not changed
        models.DagRun.find(dag_id=self.dag2.dag_id, execution_date=self.execution_dates[0])
        self._verify_dag_run_state(self.dag2, self.execution_dates[0], State.FAILED)
        models.DagRun.find(dag_id=self.dag2.dag_id, execution_date=self.execution_dates[2])
        self._verify_dag_run_state(self.dag2, self.execution_dates[2], State.RUNNING)

    def test_set_dag_run_state_edge_cases(self):
        # Dag does not exist
        altered = set_dag_run_state_to_success(None, self.execution_dates[0])
        assert len(altered) == 0
        altered = set_dag_run_state_to_failed(None, self.execution_dates[0])
        assert len(altered) == 0
        altered = set_dag_run_state_to_running(None, self.execution_dates[0])
        assert len(altered) == 0

        # Invalid execution date
        altered = set_dag_run_state_to_success(self.dag1, None)
        assert len(altered) == 0
        altered = set_dag_run_state_to_failed(self.dag1, None)
        assert len(altered) == 0
        altered = set_dag_run_state_to_running(self.dag1, None)
        assert len(altered) == 0

        # This will throw ValueError since dag.latest_execution_date
        # need to be 0 does not exist.
        with pytest.raises(ValueError):
            set_dag_run_state_to_success(self.dag2, timezone.make_naive(self.execution_dates[0]))
        # altered = set_dag_run_state_to_success(self.dag1, self.execution_dates[0])
        # DagRun does not exist
        # This will throw ValueError since dag.latest_execution_date does not exist
        with pytest.raises(ValueError):
            set_dag_run_state_to_success(self.dag2, self.execution_dates[0])

    def test_set_dag_run_state_to_failed_no_running_tasks(self):
        """
        set_dag_run_state_to_failed when there are no running tasks to update
        """
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        for task in self.dag1.tasks:
            dr.get_task_instance(task.task_id).set_state(State.SUCCESS)

        set_dag_run_state_to_failed(self.dag1, date)

    def tearDown(self):
        self.dag1.clear()
        self.dag2.clear()

        with create_session() as session:
            session.query(models.DagRun).delete()
            session.query(models.TaskInstance).delete()
