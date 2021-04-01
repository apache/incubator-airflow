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

import pytest
import requests_mock

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.utils import db


class TestAirbyteHook(unittest.TestCase):
    """
    Test all functions from Airbyte Hook
    """

    airbyte_conn_id = 'airbyte_conn_id_test'
    connection_id = 'conn_test_sync'
    job_id = 1
    sync_connection_endpoint = 'http://test-airbyte:8001/api/v1/connections/sync'
    get_job_endpoint = 'http://test-airbyte:8001/api/v1/jobs/get'
    _mock_sync_conn_success_response_body = {'job': {'id': 1}}
    _mock_job_status_success_response_body = {'job': {'status': 'succeeded'}}

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='airbyte_conn_id_test', conn_type='http', host='http://test-airbyte', port=8001
            )
        )
        self.hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id)

    def return_value_get_job(self, status):
        response = mock.Mock()
        response.json.return_value = {'job': {'status': status}}
        return response

    @requests_mock.mock()
    def test_submit_sync_connection(self, m):
        m.post(
            self.sync_connection_endpoint, status_code=200, json=self._mock_sync_conn_success_response_body
        )
        resp = self.hook.submit_sync_connection(connection_id=self.connection_id)
        assert resp.status_code == 200
        assert resp.json() == self._mock_sync_conn_success_response_body

    @requests_mock.mock()
    def test_get_job_status(self, m):
        m.post(self.get_job_endpoint, status_code=200, json=self._mock_job_status_success_response_body)
        resp = self.hook.get_job(job_id=self.job_id)
        assert resp.status_code == 200
        assert resp.json() == self._mock_job_status_success_response_body

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_wait_for_job_succeeded(self, mock_get_job):
        mock_get_job.side_effect = [self.return_value_get_job(self.hook.SUCCEEDED)]
        self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)
        mock_get_job.assert_called_once_with(job_id=self.job_id)

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_wait_for_job_error(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(self.hook.RUNNING),
            self.return_value_get_job(self.hook.ERROR),
        ]
        with pytest.raises(AirflowException, match="Job failed"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        assert mock_get_job.has_calls(calls)

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_wait_for_job_timeout(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(self.hook.PENDING),
            self.return_value_get_job(self.hook.RUNNING),
            self.return_value_get_job(self.hook.RUNNING),
        ]
        with pytest.raises(AirflowException, match="Timeout"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=2, timeout=1)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        assert mock_get_job.has_calls(calls)

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_wait_for_job_state_unrecognized(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(self.hook.RUNNING),
            self.return_value_get_job("UNRECOGNIZED"),
        ]
        with pytest.raises(Exception, match="unexpected state"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        assert mock_get_job.has_calls(calls)

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_wait_for_job_cancelled(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(self.hook.RUNNING),
            self.return_value_get_job(self.hook.CANCELLED),
        ]
        with pytest.raises(AirflowException, match="Job was cancelled"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        assert mock_get_job.has_calls(calls)
