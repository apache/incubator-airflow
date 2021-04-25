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
from airflow.ti_deps.deps.task_concurrency_dep import TaskConcurrencyDep


class TestTaskConcurrencyDep(unittest.TestCase):
    def _get_task(self, **kwargs):
        return BaseOperator(task_id='test_task', dag=DAG('test_dag'), **kwargs)

    def test_not_task_concurrency(self):
        task = self._get_task(start_date=datetime(2016, 1, 1))
        dep_context = DepContext()
        ti = Mock(task=task, execution_date=datetime(2016, 1, 1))
        assert TaskConcurrencyDep().is_met(ti=ti, dep_context=dep_context)

    def test_not_reached_concurrency(self):
        task = self._get_task(start_date=datetime(2016, 1, 1), task_concurrency=1)
        dep_context = DepContext()
        ti = Mock(task=task, execution_date=datetime(2016, 1, 1))
        ti.get_num_running_task_instances = lambda x: 0
        assert TaskConcurrencyDep().is_met(ti=ti, dep_context=dep_context)

    def test_reached_concurrency(self):
        task = self._get_task(start_date=datetime(2016, 1, 1), task_concurrency=2)
        dep_context = DepContext()
        ti = Mock(task=task, execution_date=datetime(2016, 1, 1))
        ti.get_num_running_task_instances = lambda x: 1
        assert TaskConcurrencyDep().is_met(ti=ti, dep_context=dep_context)
        ti.get_num_running_task_instances = lambda x: 2
        assert not TaskConcurrencyDep().is_met(ti=ti, dep_context=dep_context)
