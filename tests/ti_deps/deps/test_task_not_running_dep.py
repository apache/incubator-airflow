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

from airflow.ti_deps.deps.task_not_running_dep import TaskNotRunningDep
from airflow.utils.state import State


class TestTaskNotRunningDep(unittest.TestCase):
    def test_not_running_state(self):
        ti = Mock(state=State.QUEUED, end_date=datetime(2016, 1, 1))
        assert TaskNotRunningDep().is_met(ti=ti)

    def test_running_state(self):
        ti = Mock(state=State.RUNNING, end_date=datetime(2016, 1, 1))
        assert not TaskNotRunningDep().is_met(ti=ti)
