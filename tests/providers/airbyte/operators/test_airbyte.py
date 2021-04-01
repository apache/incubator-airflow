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
import unittest
from unittest import mock

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


class TestAirbyteTriggerSyncOp(unittest.TestCase):
    """
    Test execute function from Airbyte Operator
    """

    airbyte_conn_id = 'test_airbyte_conn_id'
    connection_id = 'test_airbyte_connection'
    job_id = 1
    wait_seconds = 0
    timeout = 360

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.submit_sync_connection')
    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.wait_for_job', return_value=None)
    def test_execute(self, mock_wait_for_job, mock_submit_sync_connection):
        mock_submit_sync_connection.return_value = mock.Mock(
            **{'json.return_value': {'job': {'id': self.job_id}}}
        )

        op = AirbyteTriggerSyncOperator(
            task_id='test_Airbyte_op',
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )
        op.execute({})

        mock_submit_sync_connection.assert_called_once_with(connection_id=self.connection_id)
        mock_wait_for_job.assert_called_once_with(
            job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout
        )
