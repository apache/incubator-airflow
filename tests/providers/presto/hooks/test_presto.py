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
import json
import re
import unittest
from unittest import mock
from unittest.mock import patch

import pytest
from parameterized import parameterized
from prestodb.transaction import IsolationLevel

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.presto.hooks.presto import PrestoHook


class TestPrestoHookConn(unittest.TestCase):
    @patch('airflow.providers.presto.hooks.presto.prestodb.auth.BasicAuthentication')
    @patch('airflow.providers.presto.hooks.presto.prestodb.dbapi.connect')
    @patch('airflow.providers.presto.hooks.presto.PrestoHook.get_connection')
    def test_get_conn_basic_auth(self, mock_get_connection, mock_connect, mock_basic_auth):
        mock_get_connection.return_value = Connection(
            login='login', password='password', host='host', schema='hive'
        )

        conn = PrestoHook().get_conn()
        mock_connect.assert_called_once_with(
            catalog='hive',
            host='host',
            port=None,
            http_scheme='http',
            schema='hive',
            source='airflow',
            user='login',
            isolation_level=0,
            auth=mock_basic_auth.return_value,
        )
        mock_basic_auth.assert_called_once_with('login', 'password')
        assert mock_connect.return_value == conn

    @patch('airflow.providers.presto.hooks.presto.PrestoHook.get_connection')
    def test_get_conn_invalid_auth(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            login='login',
            password='password',
            host='host',
            schema='hive',
            extra=json.dumps({'auth': 'kerberos'}),
        )
        with pytest.raises(
            AirflowException, match=re.escape("Kerberos authorization doesn't support password.")
        ):
            PrestoHook().get_conn()

    @patch('airflow.providers.presto.hooks.presto.prestodb.auth.KerberosAuthentication')
    @patch('airflow.providers.presto.hooks.presto.prestodb.dbapi.connect')
    @patch('airflow.providers.presto.hooks.presto.PrestoHook.get_connection')
    def test_get_conn_kerberos_auth(self, mock_get_connection, mock_connect, mock_auth):
        mock_get_connection.return_value = Connection(
            login='login',
            host='host',
            schema='hive',
            extra=json.dumps(
                {
                    'auth': 'kerberos',
                    'kerberos__config': 'TEST_KERBEROS_CONFIG',
                    'kerberos__service_name': 'TEST_SERVICE_NAME',
                    'kerberos__mutual_authentication': 'TEST_MUTUAL_AUTHENTICATION',
                    'kerberos__force_preemptive': True,
                    'kerberos__hostname_override': 'TEST_HOSTNAME_OVERRIDE',
                    'kerberos__sanitize_mutual_error_response': True,
                    'kerberos__principal': 'TEST_PRINCIPAL',
                    'kerberos__delegate': 'TEST_DELEGATE',
                    'kerberos__ca_bundle': 'TEST_CA_BUNDLE',
                }
            ),
        )

        conn = PrestoHook().get_conn()
        mock_connect.assert_called_once_with(
            catalog='hive',
            host='host',
            port=None,
            http_scheme='http',
            schema='hive',
            source='airflow',
            user='login',
            isolation_level=0,
            auth=mock_auth.return_value,
        )
        mock_auth.assert_called_once_with(
            ca_bundle='TEST_CA_BUNDLE',
            config='TEST_KERBEROS_CONFIG',
            delegate='TEST_DELEGATE',
            force_preemptive=True,
            hostname_override='TEST_HOSTNAME_OVERRIDE',
            mutual_authentication='TEST_MUTUAL_AUTHENTICATION',
            principal='TEST_PRINCIPAL',
            sanitize_mutual_error_response=True,
            service_name='TEST_SERVICE_NAME',
        )
        assert mock_connect.return_value == conn

    @parameterized.expand(
        [
            ('False', False),
            ('false', False),
            ('true', True),
            ('true', True),
            ('/tmp/cert.crt', '/tmp/cert.crt'),
        ]
    )
    def test_get_conn_verify(self, current_verify, expected_verify):
        patcher_connect = patch('airflow.providers.presto.hooks.presto.prestodb.dbapi.connect')
        patcher_get_connections = patch('airflow.providers.presto.hooks.presto.PrestoHook.get_connection')

        with patcher_connect as mock_connect, patcher_get_connections as mock_get_connection:
            mock_get_connection.return_value = Connection(
                login='login', host='host', schema='hive', extra=json.dumps({'verify': current_verify})
            )
            mock_verify = mock.PropertyMock()
            type(mock_connect.return_value._http_session).verify = mock_verify

            conn = PrestoHook().get_conn()
            mock_verify.assert_called_once_with(expected_verify)
            assert mock_connect.return_value == conn


class TestPrestoHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestPrestoHook(PrestoHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

            def get_isolation_level(self):
                return IsolationLevel.READ_COMMITTED

        self.db_hook = UnitTestPrestoHook()

    @patch('airflow.hooks.dbapi.DbApiHook.insert_rows')
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",), ("world",)]
        target_fields = None
        commit_every = 10
        self.db_hook.insert_rows(table, rows, target_fields, commit_every)
        mock_insert_rows.assert_called_once_with(table, rows, None, 10)

    def test_get_first_record(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchone.return_value = result_sets[0]

        assert result_sets[0] == self.db_hook.get_first(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook.get_records(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_pandas_df(self):
        statement = 'SQL'
        column = 'col'
        result_sets = [('row1',), ('row2',)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_pandas_df(statement)

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]

        self.cur.execute.assert_called_once_with(statement, None)
