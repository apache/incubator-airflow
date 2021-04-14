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
# pylint: disable=no-value-for-parameter

import unittest
from unittest.mock import Mock, patch

import pytest
from freezegun import freeze_time

from airflow import settings
from airflow.models import DAG, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.utils.timezone import datetime


@freeze_time('2016-11-01')
@pytest.mark.parametrize(
    "allow_trigger_in_future,schedule_interval,execution_date,is_met",
    [
        (True, None, datetime(2016, 11, 3), True),
        (True, "@daily", datetime(2016, 11, 3), False),
        (False, None, datetime(2016, 11, 3), False),
        (False, "@daily", datetime(2016, 11, 3), False),
        (False, "@daily", datetime(2016, 11, 1), True),
        (False, None, datetime(2016, 11, 1), True),
    ],
)
def test_exec_date_dep(allow_trigger_in_future, schedule_interval, execution_date, is_met):
    """
    If the dag's execution date is in the future but (allow_trigger_in_future=False or not schedule_interval)
    this dep should fail
    """

    with patch.object(settings, 'ALLOW_FUTURE_EXEC_DATES', allow_trigger_in_future):
        dag = DAG(
            'test_localtaskjob_heartbeat',
            start_date=datetime(2015, 1, 1),
            end_date=datetime(2016, 11, 5),
            schedule_interval=schedule_interval,
        )

        with dag:
            op1 = DummyOperator(task_id='op1')

        ti = TaskInstance(task=op1, execution_date=execution_date)
        assert RunnableExecDateDep().is_met(ti=ti) == is_met


class TestRunnableExecDateDep(unittest.TestCase):
    def _get_task_instance(self, execution_date, dag_end_date=None, task_end_date=None):
        dag = Mock(end_date=dag_end_date)
        task = Mock(dag=dag, end_date=task_end_date)
        return TaskInstance(task=task, execution_date=execution_date)

    @freeze_time('2016-01-01')
    def test_exec_date_after_end_date(self):
        """
        If the dag's execution date is in the future this dep should fail
        """
        dag = DAG(
            'test_localtaskjob_heartbeat',
            start_date=datetime(2015, 1, 1),
            end_date=datetime(2016, 11, 5),
            schedule_interval=None,
        )

        with dag:
            op1 = DummyOperator(task_id='op1')

        ti = TaskInstance(task=op1, execution_date=datetime(2016, 11, 2))
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_exec_date_after_task_end_date(self):
        """
        If the task instance execution date is after the tasks end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 3),
            task_end_date=datetime(2016, 1, 1),
            execution_date=datetime(2016, 1, 2),
        )
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_exec_date_after_dag_end_date(self):
        """
        If the task instance execution date is after the dag's end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 1),
            task_end_date=datetime(2016, 1, 3),
            execution_date=datetime(2016, 1, 2),
        )
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_all_deps_met(self):
        """
        Test to make sure all the conditions for the dep are met
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 2),
            task_end_date=datetime(2016, 1, 2),
            execution_date=datetime(2016, 1, 1),
        )
        assert RunnableExecDateDep().is_met(ti=ti)
