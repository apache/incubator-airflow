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
from datetime import datetime
from unittest.mock import Mock

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.utils.state import State


class TestPrevDagrunDep(unittest.TestCase):
    def _get_task(self, **kwargs):
        return BaseOperator(task_id='test_task', dag=DAG('test_dag'), **kwargs)

    def test_not_depends_on_past(self):
        """
        If depends on past isn't set in the task then the previous dagrun should be
        ignored, even though there is no previous_ti which would normally fail the dep
        """
        task = self._get_task(
            depends_on_past=False, start_date=datetime(2016, 1, 1), wait_for_downstream=False
        )
        prev_ti = Mock(
            task=task,
            state=State.SUCCESS,
            are_dependents_done=Mock(return_value=True),
            execution_date=datetime(2016, 1, 2),
        )
        ti = Mock(task=task, previous_ti=prev_ti, execution_date=datetime(2016, 1, 3))
        dep_context = DepContext(ignore_depends_on_past=False)

        assert PrevDagrunDep().is_met(ti=ti, dep_context=dep_context)

    def test_context_ignore_depends_on_past(self):
        """
        If the context overrides depends_on_past then the dep should be met,
        even though there is no previous_ti which would normally fail the dep
        """
        task = self._get_task(
            depends_on_past=True, start_date=datetime(2016, 1, 1), wait_for_downstream=False
        )
        prev_ti = Mock(
            task=task,
            state=State.SUCCESS,
            are_dependents_done=Mock(return_value=True),
            execution_date=datetime(2016, 1, 2),
        )
        ti = Mock(task=task, previous_ti=prev_ti, execution_date=datetime(2016, 1, 3))
        dep_context = DepContext(ignore_depends_on_past=True)

        assert PrevDagrunDep().is_met(ti=ti, dep_context=dep_context)

    def test_first_task_run(self):
        """
        The first task run for a TI should pass since it has no previous dagrun.
        """
        task = self._get_task(
            depends_on_past=True, start_date=datetime(2016, 1, 1), wait_for_downstream=False
        )
        prev_ti = None
        ti = Mock(task=task, previous_ti=prev_ti, execution_date=datetime(2016, 1, 1))
        dep_context = DepContext(ignore_depends_on_past=False)

        assert PrevDagrunDep().is_met(ti=ti, dep_context=dep_context)

    def test_prev_ti_bad_state(self):
        """
        If the previous TI did not complete execution this dep should fail.
        """
        task = self._get_task(
            depends_on_past=True, start_date=datetime(2016, 1, 1), wait_for_downstream=False
        )
        prev_ti = Mock(state=State.NONE, are_dependents_done=Mock(return_value=True))
        ti = Mock(task=task, previous_ti=prev_ti, execution_date=datetime(2016, 1, 2))
        dep_context = DepContext(ignore_depends_on_past=False)

        assert not PrevDagrunDep().is_met(ti=ti, dep_context=dep_context)

    def test_failed_wait_for_downstream(self):
        """
        If the previous TI specified to wait for the downstream tasks of the
        previous dagrun then it should fail this dep if the downstream TIs of
        the previous TI are not done.
        """
        task = self._get_task(depends_on_past=True, start_date=datetime(2016, 1, 1), wait_for_downstream=True)
        prev_ti = Mock(state=State.SUCCESS, are_dependents_done=Mock(return_value=False))
        ti = Mock(task=task, previous_ti=prev_ti, execution_date=datetime(2016, 1, 2))
        dep_context = DepContext(ignore_depends_on_past=False)

        assert not PrevDagrunDep().is_met(ti=ti, dep_context=dep_context)

    def test_all_met(self):
        """
        Test to make sure all the conditions for the dep are met
        """
        task = self._get_task(depends_on_past=True, start_date=datetime(2016, 1, 1), wait_for_downstream=True)
        prev_ti = Mock(state=State.SUCCESS, are_dependents_done=Mock(return_value=True))
        ti = Mock(task=task, execution_date=datetime(2016, 1, 2), **{'get_previous_ti.return_value': prev_ti})
        dep_context = DepContext(ignore_depends_on_past=False)

        assert PrevDagrunDep().is_met(ti=ti, dep_context=dep_context)
