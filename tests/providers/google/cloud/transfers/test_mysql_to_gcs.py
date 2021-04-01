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

import datetime
import decimal
import unittest
from unittest import mock

import pytest
from MySQLdb import ProgrammingError  # pylint: disable=no-name-in-module
from parameterized import parameterized

from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator

TASK_ID = 'test-mysql-to-gcs'
MYSQL_CONN_ID = 'mysql_conn_test'
TZ_QUERY = "SET time_zone = '+00:00'"
SQL = 'select 1'
BUCKET = 'gs://test'
JSON_FILENAME = 'test_{}.ndjson'
CSV_FILENAME = 'test_{}.csv'
SCHEMA = [
    {'mode': 'REQUIRED', 'name': 'some_str', 'type': 'FLOAT'},
    {'mode': 'REQUIRED', 'name': 'some_num', 'type': 'TIMESTAMP'},
]

ROWS = [('mock_row_content_1', 42), ('mock_row_content_2', 43), ('mock_row_content_3', 44)]
CURSOR_DESCRIPTION = (('some_str', 0, 0, 0, 0, 0, False), ('some_num', 1005, 0, 0, 0, 0, False))
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n',
]
CSV_LINES = [
    b'some_str,some_num\r\n' b'mock_row_content_1,42\r\n',
    b'mock_row_content_2,43\r\n',
    b'mock_row_content_3,44\r\n',
]
CSV_LINES_PIPE_DELIMITED = [
    b'some_str|some_num\r\n' b'mock_row_content_1|42\r\n',
    b'mock_row_content_2|43\r\n',
    b'mock_row_content_3|44\r\n',
]
SCHEMA_FILENAME = 'schema_test.json'
SCHEMA_JSON = [
    b'[{"mode": "REQUIRED", "name": "some_str", "type": "FLOAT"}, ',
    b'{"mode": "REQUIRED", "name": "some_num", "type": "STRING"}]',
]
CUSTOM_SCHEMA_JSON = [
    b'[{"mode": "REQUIRED", "name": "some_str", "type": "FLOAT"}, ',
    b'{"mode": "REQUIRED", "name": "some_num", "type": "TIMESTAMP"}]',
]


class TestMySqlToGoogleCloudStorageOperator(unittest.TestCase):
    def test_init(self):
        """Test MySqlToGoogleCloudStorageOperator instance is properly initialized."""
        op = MySQLToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            export_format='CSV',
            field_delimiter='|',
        )
        assert op.task_id == TASK_ID
        assert op.sql == SQL
        assert op.bucket == BUCKET
        assert op.filename == JSON_FILENAME
        assert op.export_format == 'csv'
        assert op.field_delimiter == '|'

    @parameterized.expand(
        [
            ("string", None, "string"),
            (datetime.date(1970, 1, 2), None, 86400),
            (datetime.date(1970, 1, 2), "DATE", "1970-01-02"),
            (datetime.datetime(1970, 1, 1, 1, 0), None, 3600),
            (decimal.Decimal(5), None, 5),
            (b"bytes", "BYTES", "Ynl0ZXM="),
            (b"\x00\x01", "INTEGER", 1),
            (None, "BYTES", None),
        ]
    )
    def test_convert_type(self, value, schema_type, expected):
        op = MySQLToGCSOperator(
            task_id=TASK_ID, mysql_conn_id=MYSQL_CONN_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME
        )
        assert op.convert_type(value, schema_type) == expected

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_exec_success_json(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = MySQLToGCSOperator(
            task_id=TASK_ID, mysql_conn_id=MYSQL_CONN_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME
        )

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            assert BUCKET == bucket
            assert JSON_FILENAME.format(0) == obj
            assert 'application/json' == mime_type
            assert not gzip
            with open(tmp_filename, 'rb') as file:
                assert b''.join(NDJSON_LINES) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_exec_success_csv(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for CSV"""
        op = MySQLToGCSOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            export_format='CSV',
            bucket=BUCKET,
            filename=CSV_FILENAME,
        )

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            assert BUCKET == bucket
            assert CSV_FILENAME.format(0) == obj
            assert 'text/csv' == mime_type
            assert not gzip
            with open(tmp_filename, 'rb') as file:
                assert b''.join(CSV_LINES) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_exec_success_csv_ensure_utc(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for CSV"""
        op = MySQLToGCSOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            export_format='CSV',
            bucket=BUCKET,
            filename=CSV_FILENAME,
            ensure_utc=True,
        )

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            assert BUCKET == bucket
            assert CSV_FILENAME.format(0) == obj
            assert 'text/csv' == mime_type
            assert not gzip
            with open(tmp_filename, 'rb') as file:
                assert b''.join(CSV_LINES) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_has_calls([mock.call(TZ_QUERY), mock.call(SQL)])

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_exec_success_csv_with_delimiter(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for CSV with a field delimiter"""
        op = MySQLToGCSOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            export_format='csv',
            field_delimiter='|',
            bucket=BUCKET,
            filename=CSV_FILENAME,
        )

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            assert BUCKET == bucket
            assert CSV_FILENAME.format(0) == obj
            assert 'text/csv' == mime_type
            assert not gzip
            with open(tmp_filename, 'rb') as file:
                assert b''.join(CSV_LINES_PIPE_DELIMITED) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_file_splitting(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            JSON_FILENAME.format(0): b''.join(NDJSON_LINES[:2]),
            JSON_FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            assert BUCKET == bucket
            assert 'application/json' == mime_type
            assert not gzip
            with open(tmp_filename, 'rb') as file:
                assert expected_upload[obj] == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySQLToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            approx_max_file_size_bytes=len(expected_upload[JSON_FILENAME.format(0)]),
        )
        op.execute(None)

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_schema_file(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test writing schema files."""
        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):  # pylint: disable=unused-argument
            if obj == SCHEMA_FILENAME:
                assert not gzip
                with open(tmp_filename, 'rb') as file:
                    assert b''.join(SCHEMA_JSON) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySQLToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME, schema_filename=SCHEMA_FILENAME
        )
        op.execute(None)

        # once for the file and once for the schema
        assert 2 == gcs_hook_mock.upload.call_count

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_schema_file_with_custom_schema(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test writing schema files with customized schema"""
        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):  # pylint: disable=unused-argument
            if obj == SCHEMA_FILENAME:
                assert not gzip
                with open(tmp_filename, 'rb') as file:
                    assert b''.join(CUSTOM_SCHEMA_JSON) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySQLToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            schema_filename=SCHEMA_FILENAME,
            schema=SCHEMA,
        )
        op.execute(None)

        # once for the file and once for the schema
        assert 2 == gcs_hook_mock.upload.call_count

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_query_with_error(self, mock_gcs_hook, mock_mysql_hook):
        mock_mysql_hook.return_value.get_conn.return_value.cursor.return_value.execute.side_effect = (
            ProgrammingError
        )
        op = MySQLToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME, schema_filename=SCHEMA_FILENAME
        )
        with pytest.raises(ProgrammingError):
            op.query()

    @mock.patch('airflow.providers.google.cloud.transfers.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_execute_with_query_error(self, mock_gcs_hook, mock_mysql_hook):
        mock_mysql_hook.return_value.get_conn.return_value.cursor.return_value.execute.side_effect = (
            ProgrammingError
        )
        op = MySQLToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME, schema_filename=SCHEMA_FILENAME
        )
        with pytest.raises(ProgrammingError):
            op.execute(None)
