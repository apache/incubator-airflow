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
from datetime import timedelta
from unittest.mock import Mock, patch

from airflow.models import DAG, TaskInstance, TaskReschedule
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils.state import State
from airflow.utils.timezone import utcnow


class TestNotInReschedulePeriodDep(unittest.TestCase):
    def _get_task_instance(self, state):
        dag = DAG('test_dag')
        task = Mock(dag=dag)
        ti = TaskInstance(task=task, state=state, execution_date=None)
        return ti

    def _get_task_reschedule(self, reschedule_date):
        task = Mock(dag_id='test_dag', task_id='test_task')
        reschedule = TaskReschedule(
            task=task,
            execution_date=None,
            try_number=None,
            start_date=reschedule_date,
            end_date=reschedule_date,
            reschedule_date=reschedule_date,
        )
        return reschedule

    def test_should_pass_if_ignore_in_reschedule_period_is_set(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        dep_context = DepContext(ignore_in_reschedule_period=True)
        assert ReadyToRescheduleDep().is_met(ti=ti, dep_context=dep_context)

    def test_should_pass_if_not_in_none_state(self):
        ti = self._get_task_instance(State.UP_FOR_RETRY)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    @patch('airflow.models.taskreschedule.TaskReschedule.query_for_task_instance')
    def test_should_pass_if_no_reschedule_record_exists(self, mock_query_for_task_instance):
        mock_query_for_task_instance.return_value.with_entities.return_value.first.return_value = []
        ti = self._get_task_instance(State.NONE)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    @patch('airflow.models.taskreschedule.TaskReschedule.query_for_task_instance')
    def test_should_pass_after_reschedule_date_one(self, mock_query_for_task_instance):
        mock_query_for_task_instance.return_value.with_entities.return_value.first.return_value = (
            self._get_task_reschedule(utcnow() - timedelta(minutes=1))
        )
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    @patch('airflow.models.taskreschedule.TaskReschedule.query_for_task_instance')
    def test_should_pass_after_reschedule_date_multiple(self, mock_query_for_task_instance):
        mock_query_for_task_instance.return_value.with_entities.return_value.first.return_value = [
            self._get_task_reschedule(utcnow() - timedelta(minutes=21)),
            self._get_task_reschedule(utcnow() - timedelta(minutes=11)),
            self._get_task_reschedule(utcnow() - timedelta(minutes=1)),
        ][-1]
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    @patch('airflow.models.taskreschedule.TaskReschedule.query_for_task_instance')
    def test_should_fail_before_reschedule_date_one(self, mock_query_for_task_instance):
        mock_query_for_task_instance.return_value.with_entities.return_value.first.return_value = (
            self._get_task_reschedule(utcnow() + timedelta(minutes=1))
        )

        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        assert not ReadyToRescheduleDep().is_met(ti=ti)

    @patch('airflow.models.taskreschedule.TaskReschedule.query_for_task_instance')
    def test_should_fail_before_reschedule_date_multiple(self, mock_query_for_task_instance):
        mock_query_for_task_instance.return_value.with_entities.return_value.first.return_value = [
            self._get_task_reschedule(utcnow() - timedelta(minutes=19)),
            self._get_task_reschedule(utcnow() - timedelta(minutes=9)),
            self._get_task_reschedule(utcnow() + timedelta(minutes=1)),
        ][-1]
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        assert not ReadyToRescheduleDep().is_met(ti=ti)
