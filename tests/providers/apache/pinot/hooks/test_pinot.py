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

import io
import os
import subprocess
import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.pinot.hooks.pinot import PinotAdminHook, PinotDbApiHook


class TestPinotAdminHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.conn = conn = mock.MagicMock()
        self.conn.host = 'host'
        self.conn.port = '1000'
        self.conn.extra_dejson = {'cmd_path': './pinot-admin.sh'}

        class PinotAdminHookTest(PinotAdminHook):
            def get_connection(self, conn_id):
                return conn

        self.db_hook = PinotAdminHookTest()

    @mock.patch('airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook.run_cli')
    def test_add_schema(self, mock_run_cli):
        params = ["schema_file", False]
        self.db_hook.add_schema(*params)
        mock_run_cli.assert_called_once_with(
            [
                'AddSchema',
                '-controllerHost',
                self.conn.host,
                '-controllerPort',
                self.conn.port,
                '-schemaFile',
                params[0],
            ]
        )

    @mock.patch('airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook.run_cli')
    def test_add_table(self, mock_run_cli):
        params = ["config_file", False]
        self.db_hook.add_table(*params)
        mock_run_cli.assert_called_once_with(
            [
                'AddTable',
                '-controllerHost',
                self.conn.host,
                '-controllerPort',
                self.conn.port,
                '-filePath',
                params[0],
            ]
        )

    @mock.patch('airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook.run_cli')
    def test_create_segment(self, mock_run_cli):
        params = {
            "generator_config_file": "a",
            "data_dir": "b",
            "segment_format": "c",
            "out_dir": "d",
            "overwrite": True,
            "table_name": "e",
            "segment_name": "f",
            "time_column_name": "g",
            "schema_file": "h",
            "reader_config_file": "i",
            "enable_star_tree_index": False,
            "star_tree_index_spec_file": "j",
            "hll_size": 9,
            "hll_columns": "k",
            "hll_suffix": "l",
            "num_threads": 8,
            "post_creation_verification": True,
            "retry": 7,
        }

        self.db_hook.create_segment(**params)

        mock_run_cli.assert_called_once_with(
            [
                'CreateSegment',
                '-generatorConfigFile',
                params["generator_config_file"],
                '-dataDir',
                params["data_dir"],
                '-format',
                params["segment_format"],
                '-outDir',
                params["out_dir"],
                '-overwrite',
                params["overwrite"],
                '-tableName',
                params["table_name"],
                '-segmentName',
                params["segment_name"],
                '-timeColumnName',
                params["time_column_name"],
                '-schemaFile',
                params["schema_file"],
                '-readerConfigFile',
                params["reader_config_file"],
                '-starTreeIndexSpecFile',
                params["star_tree_index_spec_file"],
                '-hllSize',
                params["hll_size"],
                '-hllColumns',
                params["hll_columns"],
                '-hllSuffix',
                params["hll_suffix"],
                '-numThreads',
                params["num_threads"],
                '-postCreationVerification',
                params["post_creation_verification"],
                '-retry',
                params["retry"],
            ]
        )

    @mock.patch('airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook.run_cli')
    def test_upload_segment(self, mock_run_cli):
        params = ["segment_dir", False]
        self.db_hook.upload_segment(*params)
        mock_run_cli.assert_called_once_with(
            [
                'UploadSegment',
                '-controllerHost',
                self.conn.host,
                '-controllerPort',
                self.conn.port,
                '-segmentDir',
                params[0],
            ]
        )

    @mock.patch('subprocess.Popen')
    def test_run_cli_success(self, mock_popen):
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = io.BytesIO(b'')
        mock_popen.return_value.__enter__.return_value = mock_proc

        params = ["foo", "bar", "baz"]
        self.db_hook.run_cli(params)
        params.insert(0, self.conn.extra_dejson.get('cmd_path'))
        mock_popen.assert_called_once_with(
            params, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True, env=None
        )

    @mock.patch('subprocess.Popen')
    def test_run_cli_failure_error_message(self, mock_popen):
        msg = b"Exception caught"
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = io.BytesIO(msg)
        mock_popen.return_value.__enter__.return_value = mock_proc
        params = ["foo", "bar", "baz"]
        with pytest.raises(AirflowException):
            self.db_hook.run_cli(params)
        params.insert(0, self.conn.extra_dejson.get('cmd_path'))
        mock_popen.assert_called_once_with(
            params, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True, env=None
        )

    @mock.patch('subprocess.Popen')
    def test_run_cli_failure_status_code(self, mock_popen):
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = io.BytesIO(b'')
        mock_popen.return_value.__enter__.return_value = mock_proc

        self.db_hook.pinot_admin_system_exit = True
        params = ["foo", "bar", "baz"]
        with pytest.raises(AirflowException):
            self.db_hook.run_cli(params)
        params.insert(0, self.conn.extra_dejson.get('cmd_path'))
        env = os.environ.copy()
        env.update({"JAVA_OPTS": "-Dpinot.admin.system.exit=true "})
        mock_popen.assert_called_once_with(
            params, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True, env=env
        )


class TestPinotDbApiHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.conn = conn = mock.MagicMock()
        self.conn.host = 'host'
        self.conn.port = '1000'
        self.conn.conn_type = 'http'
        self.conn.extra_dejson = {'endpoint': 'query/sql'}
        self.cur = mock.MagicMock(rowcount=0)
        self.conn.cursor.return_value = self.cur
        self.conn.__enter__.return_value = self.cur
        self.conn.__exit__.return_value = None

        class TestPinotDBApiHook(PinotDbApiHook):
            def get_conn(self):
                return conn

            def get_connection(self, conn_id):
                return conn

        self.db_hook = TestPinotDBApiHook

    def test_get_uri(self):
        """
        Test on getting a pinot connection uri
        """
        db_hook = self.db_hook()
        assert db_hook.get_uri() == 'http://host:1000/query/sql'

    def test_get_conn(self):
        """
        Test on getting a pinot connection
        """
        conn = self.db_hook().get_conn()
        assert conn.host == 'host'
        assert conn.port == '1000'
        assert conn.conn_type == 'http'
        assert conn.extra_dejson.get('endpoint') == 'query/sql'

    def test_get_records(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchall.return_value = result_sets
        assert result_sets == self.db_hook().get_records(statement)

    def test_get_first(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchone.return_value = result_sets[0]
        assert result_sets[0] == self.db_hook().get_first(statement)

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


class TestPinotDbApiHookIntegration(unittest.TestCase):
    @pytest.mark.integration("pinot")
    @mock.patch.dict('os.environ', AIRFLOW_CONN_PINOT_BROKER_DEFAULT="pinot://pinot:8000/")
    def test_should_return_records(self):
        hook = PinotDbApiHook()
        sql = "select playerName from baseballStats  ORDER BY playerName limit 5"
        records = hook.get_records(sql)
        assert [["A. Harry"], ["A. Harry"], ["Aaron"], ["Aaron Albert"], ["Aaron Albert"]] == records
