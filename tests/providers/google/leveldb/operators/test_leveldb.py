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
from unittest import mock

from airflow.models.dag import DAG
from airflow.providers.google.leveldb.hooks.leveldb import LevelDBHook
from airflow.providers.google.leveldb.operators.leveldb import LevelDBOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
TEST_TASK_ID = 'test_leveldb_dag_id'
TEST_CONTEXT_ID = "test_context_id"


class TestLevelDBOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_TASK_ID, default_args=args)

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    @mock.patch.object(LevelDBHook, 'run_cli')
    def test_execute(self, mock_run_cli):
        operator = LevelDBOperator(
            task_id='test_task',
            dag=self.dag,
            leveldb_conn_id='leveldb_default',
            command='put',
            key=b'key',
            value=b'value',
        )
        operator.execute(context=TEST_CONTEXT_ID)
        mock_run_cli.assert_called_once_with(command='put', value=b'value', key=b'key')
