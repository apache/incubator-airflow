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

import datetime
import unittest

import freezegun

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)


class TestBranchDateTimeOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

        cls.targets = [
            (datetime.datetime(2020, 7, 7, 10, 0, 0), datetime.datetime(2020, 7, 7, 11, 0, 0)),
            (datetime.time(10, 0, 0), datetime.time(11, 0, 0)),
            (datetime.datetime(2020, 7, 7, 10, 0, 0), datetime.time(11, 0, 0)),
            (datetime.time(10, 0, 0), datetime.datetime(2020, 7, 7, 11, 0, 0)),
            (datetime.time(11, 0, 0), datetime.time(10, 0, 0)),
        ]

    def setUp(self):
        self.dag = DAG(
            'branch_datetime_operator_test',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)

        self.branch_op = BranchDateTimeOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=datetime.datetime(2020, 7, 7, 11, 0, 0),
            target_lower=datetime.datetime(2020, 7, 7, 10, 0, 0),
            dag=self.dag,
        )

        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        self.dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def _assert_task_ids_match_states(self, task_ids_to_states):
        """Helper that asserts task instances with a given id are in a given state"""
        tis = self.dr.get_task_instances()
        for ti in tis:
            try:
                expected_state = task_ids_to_states[ti.task_id]
            except KeyError:
                raise ValueError(f'Invalid task id {ti.task_id} found!')
            else:
                self.assertEqual(
                    ti.state,
                    expected_state,
                    f"Task {ti.task_id} has state {ti.state} instead of expected {expected_state}",
                )

    def test_no_target_time(self):
        """Check if BranchDateTimeOperator raises exception on missing target"""
        with self.assertRaises(AirflowException):
            BranchDateTimeOperator(
                task_id='datetime_branch',
                follow_task_ids_if_true='branch_1',
                follow_task_ids_if_false='branch_2',
                target_upper=None,
                target_lower=None,
                dag=self.dag,
            )

    @freezegun.freeze_time("2020-07-07 10:54:05")
    def test_branch_datetime_operator_falls_within_range(self):
        """Check BranchDateTimeOperator branch operation"""
        for target_lower, target_upper in self.targets:
            with self.subTest(target_lower=target_lower, target_upper=target_upper):
                self.branch_op.target_lower = target_lower
                self.branch_op.target_upper = target_upper
                self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

                self._assert_task_ids_match_states(
                    {
                        'datetime_branch': State.SUCCESS,
                        'branch_1': State.NONE,
                        'branch_2': State.SKIPPED,
                    }
                )

    def test_branch_datetime_operator_falls_outside_range(self):
        """Check BranchDateTimeOperator branch operation"""
        dates = [
            datetime.datetime(2020, 7, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 6, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
        ]

        for target_lower, target_upper in self.targets:
            with self.subTest(target_lower=target_lower, target_upper=target_upper):
                self.branch_op.target_lower = target_lower
                self.branch_op.target_upper = target_upper

                for date in dates:
                    with freezegun.freeze_time(date):
                        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

                        self._assert_task_ids_match_states(
                            {
                                'datetime_branch': State.SUCCESS,
                                'branch_1': State.SKIPPED,
                                'branch_2': State.NONE,
                            }
                        )

    @freezegun.freeze_time("2020-07-07 10:54:05")
    def test_branch_datetime_operator_upper_comparison_within_range(self):
        """Check BranchDateTimeOperator branch operation"""
        for _, target_upper in self.targets:
            with self.subTest(target_upper=target_upper):
                self.branch_op.target_upper = target_upper
                self.branch_op.target_lower = None

                self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

                self._assert_task_ids_match_states(
                    {
                        'datetime_branch': State.SUCCESS,
                        'branch_1': State.NONE,
                        'branch_2': State.SKIPPED,
                    }
                )

    @freezegun.freeze_time("2020-07-07 10:54:05")
    def test_branch_datetime_operator_lower_comparison_within_range(self):
        """Check BranchDateTimeOperator branch operation"""
        for target_lower, _ in self.targets:
            with self.subTest(target_lower=target_lower):
                self.branch_op.target_lower = target_lower
                self.branch_op.target_upper = None

                self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

                self._assert_task_ids_match_states(
                    {
                        'datetime_branch': State.SUCCESS,
                        'branch_1': State.NONE,
                        'branch_2': State.SKIPPED,
                    }
                )

    @freezegun.freeze_time("2020-07-07 12:00:00")
    def test_branch_datetime_operator_upper_comparison_outside_range(self):
        """Check BranchDateTimeOperator branch operation"""
        for _, target_upper in self.targets:
            with self.subTest(target_upper=target_upper):
                self.branch_op.target_upper = target_upper
                self.branch_op.target_lower = None

                self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

                self._assert_task_ids_match_states(
                    {
                        'datetime_branch': State.SUCCESS,
                        'branch_1': State.SKIPPED,
                        'branch_2': State.NONE,
                    }
                )

    @freezegun.freeze_time("2020-07-07 09:00:00")
    def test_branch_datetime_operator_lower_comparison_outside_range(self):
        """Check BranchDateTimeOperator branch operation"""
        for target_lower, _ in self.targets:
            with self.subTest(target_lower=target_lower):
                self.branch_op.target_lower = target_lower
                self.branch_op.target_upper = None

                self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

                self._assert_task_ids_match_states(
                    {
                        'datetime_branch': State.SUCCESS,
                        'branch_1': State.SKIPPED,
                        'branch_2': State.NONE,
                    }
                )

    @freezegun.freeze_time("2020-12-01 09:00:00")
    def test_branch_datetime_operator_use_task_execution_date(self):
        """Check if BranchDateTimeOperator uses task execution date"""
        in_between_date = timezone.datetime(2020, 7, 7, 10, 30, 0)
        self.branch_op.use_task_execution_date = True
        self.dr = self.dag.create_dagrun(
            run_id='manual_exec_date__',
            start_date=in_between_date,
            execution_date=in_between_date,
            state=State.RUNNING,
        )

        for target_lower, target_upper in self.targets:
            with self.subTest(target_lower=target_lower, target_upper=target_upper):
                self.branch_op.target_lower = target_lower
                self.branch_op.target_upper = target_upper
                self.branch_op.run(start_date=in_between_date, end_date=in_between_date)

                self._assert_task_ids_match_states(
                    {
                        'datetime_branch': State.SUCCESS,
                        'branch_1': State.NONE,
                        'branch_2': State.SKIPPED,
                    }
                )
