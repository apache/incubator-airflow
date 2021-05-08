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
from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


class TestSqliteHookConn(unittest.TestCase):
    def setUp(self):

        self.connection = Connection(host='host')

        class UnitTestSqliteHook(SqliteHook):
            conn_name_attr = 'test_conn_id'

        self.db_hook = UnitTestSqliteHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch('airflow.providers.sqlite.hooks.sqlite.sqlite3.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with('host')

    @patch('airflow.providers.sqlite.hooks.sqlite.sqlite3.connect')
    def test_get_conn_non_default_id(self, mock_connect):
        self.db_hook.test_conn_id = 'non_default'  # pylint: disable=attribute-defined-outside-init
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with('host')
        self.db_hook.get_connection.assert_called_once_with('non_default')


class TestSqliteHook(unittest.TestCase):
    def setUp(self):

        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestSqliteHook(SqliteHook):
            conn_name_attr = 'test_conn_id'
            log = mock.MagicMock()

            def get_conn(self):
                return conn

        self.db_hook = UnitTestSqliteHook()

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

        self.cur.execute.assert_called_once_with(statement)

    def test_run_log(self):
        statement = 'SQL'
        self.db_hook.run(statement)
        assert self.db_hook.log.info.call_count == 2
