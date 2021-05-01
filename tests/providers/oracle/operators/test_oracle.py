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

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.oracle.operators.oracle import OracleOperator


class TestOracleOperator(unittest.TestCase):
    @mock.patch.object(OracleHook, 'run')
    def test_execute(self, mock_run):
        sql = 'SELECT * FROM test_table'
        oracle_conn_id = 'oracle_default'
        parameters = {'parameter': 'value'}
        autocommit = False
        context = "test_context"
        task_id = "test_task_id"

        operator = OracleOperator(
            sql=sql,
            oracle_conn_id=oracle_conn_id,
            parameters=parameters,
            autocommit=autocommit,
            task_id=task_id,
        )
        result = operator.execute(context=context)
        assert len(mock_run.mock_calls) == 1
        assert mock_run.mock_calls[0].args == (sql, )
        assert mock_run.mock_calls[0].kwargs["autocommit"] is autocommit
        assert mock_run.mock_calls[0].kwargs["parameters"] is parameters
