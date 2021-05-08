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
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock

from airflow.exceptions import AirflowException
from airflow.providers.apache.druid.hooks.druid import DruidDbApiHook, DruidHook


class TestDruidHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)

        class TestDRuidhook(DruidHook):
            def get_conn_url(self):
                return 'http://druid-overlord:8081/druid/indexer/v1/task'

        self.db_hook = TestDRuidhook()

    @requests_mock.mock()
    def test_submit_gone_wrong(self, m):
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}',
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "FAILED"}}',
        )

        # The job failed for some reason
        with pytest.raises(AirflowException):
            self.db_hook.submit_indexing_job('Long json file')

        assert task_post.called_once
        assert status_check.called_once

    @requests_mock.mock()
    def test_submit_ok(self, m):
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}',
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "SUCCESS"}}',
        )

        # Exists just as it should
        self.db_hook.submit_indexing_job('Long json file')

        assert task_post.called_once
        assert status_check.called_once

    @requests_mock.mock()
    def test_submit_correct_json_body(self, m):
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}',
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "SUCCESS"}}',
        )

        json_ingestion_string = """
        {
            "task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"
        }
        """
        self.db_hook.submit_indexing_job(json_ingestion_string)

        assert task_post.called_once
        assert status_check.called_once
        if task_post.called_once:
            req_body = task_post.request_history[0].json()
            assert req_body['task'] == "9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"

    @requests_mock.mock()
    def test_submit_unknown_response(self, m):
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}',
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "UNKNOWN"}}',
        )

        # An unknown error code
        with pytest.raises(AirflowException):
            self.db_hook.submit_indexing_job('Long json file')

        assert task_post.called_once
        assert status_check.called_once

    @requests_mock.mock()
    def test_submit_timeout(self, m):
        self.db_hook.timeout = 1
        self.db_hook.max_ingestion_time = 5
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}',
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "RUNNING"}}',
        )
        shutdown_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task/'
            '9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/shutdown',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}',
        )

        # Because the jobs keeps running
        with pytest.raises(AirflowException):
            self.db_hook.submit_indexing_job('Long json file')

        assert task_post.called_once
        assert status_check.called
        assert shutdown_post.called_once

    @patch('airflow.providers.apache.druid.hooks.druid.DruidHook.get_connection')
    def test_get_conn_url(self, mock_get_connection):
        get_conn_value = MagicMock()
        get_conn_value.host = 'test_host'
        get_conn_value.conn_type = 'https'
        get_conn_value.port = '1'
        get_conn_value.extra_dejson = {'endpoint': 'ingest'}
        mock_get_connection.return_value = get_conn_value
        hook = DruidHook(timeout=1, max_ingestion_time=5)
        assert hook.get_conn_url() == 'https://test_host:1/ingest'

    @patch('airflow.providers.apache.druid.hooks.druid.DruidHook.get_connection')
    def test_get_auth(self, mock_get_connection):
        get_conn_value = MagicMock()
        get_conn_value.login = 'airflow'
        get_conn_value.password = 'password'
        mock_get_connection.return_value = get_conn_value
        expected = requests.auth.HTTPBasicAuth('airflow', 'password')
        assert self.db_hook.get_auth() == expected

    @patch('airflow.providers.apache.druid.hooks.druid.DruidHook.get_connection')
    def test_get_auth_with_no_user(self, mock_get_connection):
        get_conn_value = MagicMock()
        get_conn_value.login = None
        get_conn_value.password = 'password'
        mock_get_connection.return_value = get_conn_value
        assert self.db_hook.get_auth() is None

    @patch('airflow.providers.apache.druid.hooks.druid.DruidHook.get_connection')
    def test_get_auth_with_no_password(self, mock_get_connection):
        get_conn_value = MagicMock()
        get_conn_value.login = 'airflow'
        get_conn_value.password = None
        mock_get_connection.return_value = get_conn_value
        assert self.db_hook.get_auth() is None

    @patch('airflow.providers.apache.druid.hooks.druid.DruidHook.get_connection')
    def test_get_auth_with_no_user_and_password(self, mock_get_connection):
        get_conn_value = MagicMock()
        get_conn_value.login = None
        get_conn_value.password = None
        mock_get_connection.return_value = get_conn_value
        assert self.db_hook.get_auth() is None


class TestDruidDbApiHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cur = MagicMock(rowcount=0)
        self.conn = conn = MagicMock()
        self.conn.host = 'host'
        self.conn.port = '1000'
        self.conn.conn_type = 'druid'
        self.conn.extra_dejson = {'endpoint': 'druid/v2/sql'}
        self.conn.cursor.return_value = self.cur

        class TestDruidDBApiHook(DruidDbApiHook):
            def get_conn(self):
                return conn

            def get_connection(self, conn_id):
                return conn

        self.db_hook = TestDruidDBApiHook

    def test_get_uri(self):
        db_hook = self.db_hook()
        assert 'druid://host:1000/druid/v2/sql' == db_hook.get_uri()

    def test_get_first_record(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchone.return_value = result_sets[0]

        assert result_sets[0] == self.db_hook().get_first(statement)
        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook().get_records(statement)
        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)

    def test_get_pandas_df(self):
        statement = 'SQL'
        column = 'col'
        result_sets = [('row1',), ('row2',)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook().get_pandas_df(statement)

        assert column == df.columns[0]
        for i in range(len(result_sets)):  # pylint: disable=consider-using-enumerate
            assert result_sets[i][0] == df.values.tolist()[i][0]
        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        self.cur.execute.assert_called_once_with(statement)
