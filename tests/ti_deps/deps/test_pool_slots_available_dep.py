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

from airflow.models import Pool
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.ti_deps.deps.pool_slots_available_dep import PoolSlotsAvailableDep
from airflow.utils.session import create_session
from tests.test_utils import db


class TestPoolSlotsAvailableDep(unittest.TestCase):
    def setUp(self):
        db.clear_db_pools()
        with create_session() as session:
            test_pool = Pool(pool='test_pool')
            session.add(test_pool)
            session.commit()

    def tearDown(self):
        db.clear_db_pools()

    @patch('airflow.models.Pool.open_slots', return_value=0)
    # pylint: disable=unused-argument
    def test_pooled_task_reached_concurrency(self, mock_open_slots):
        ti = Mock(pool='test_pool', pool_slots=1)
        assert not PoolSlotsAvailableDep().is_met(ti=ti)

    @patch('airflow.models.Pool.open_slots', return_value=1)
    # pylint: disable=unused-argument
    def test_pooled_task_pass(self, mock_open_slots):
        ti = Mock(pool='test_pool', pool_slots=1)
        assert PoolSlotsAvailableDep().is_met(ti=ti)

    @patch('airflow.models.Pool.open_slots', return_value=0)
    # pylint: disable=unused-argument
    def test_running_pooled_task_pass(self, mock_open_slots):
        for state in EXECUTION_STATES:
            ti = Mock(pool='test_pool', state=state, pool_slots=1)
            assert PoolSlotsAvailableDep().is_met(ti=ti)

    def test_task_with_nonexistent_pool(self):
        ti = Mock(pool='nonexistent_pool', pool_slots=1)
        assert not PoolSlotsAvailableDep().is_met(ti=ti)
