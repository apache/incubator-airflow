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

import json
import re
import unittest
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException, AirflowFileParseException, ConnectionNotUnique
from airflow.secrets import local_filesystem
from airflow.secrets.local_filesystem import LocalFilesystemBackend


@contextmanager
def mock_local_file(content):
    with mock.patch("airflow.utils.parse.open", mock.mock_open(read_data=content)) as file_mock, mock.patch(
        "airflow.utils.parse.os.path.exists", return_value=True
    ):
        yield file_mock


class FileParsers(unittest.TestCase):
    @parameterized.expand(
        (
            ("AA", 'Invalid line format. The line should contain at least one equal sign ("=")'),
            ("=", "Invalid line format. Key is empty."),
        )
    )
    def test_env_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(AirflowFileParseException, match=re.escape(expected_message)):
                local_filesystem.load_variables("a.env")

    @parameterized.expand(
        (
            ("[]", "The file should contain an object."),
            ("{AAAAA}", "Expecting property name enclosed in double quotes"),
            ("", "The file is empty."),
        )
    )
    def test_json_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(AirflowFileParseException, match=re.escape(expected_message)):
                local_filesystem.load_variables("a.json")


class TestLoadVariables(unittest.TestCase):
    @parameterized.expand(
        (
            ("", {}),
            ("KEY=AAA", {"KEY": "AAA"}),
            ("KEY_A=AAA\nKEY_B=BBB", {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ("KEY_A=AAA\n # AAAA\nKEY_B=BBB", {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ("\n\n\n\nKEY_A=AAA\n\n\n\n\nKEY_B=BBB\n\n\n", {"KEY_A": "AAA", "KEY_B": "BBB"}),
        )
    )
    def test_env_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(file_content):
            variables = local_filesystem.load_variables("a.env")
            assert expected_variables == variables

    @parameterized.expand((("AA=A\nAA=B", "The \"a.env\" file contains multiple values for keys: ['AA']"),))
    def test_env_file_invalid_logic(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(AirflowException, match=re.escape(expected_message)):
                local_filesystem.load_variables("a.env")

    @parameterized.expand(
        (
            ({}, {}),
            ({"KEY": "AAA"}, {"KEY": "AAA"}),
            ({"KEY_A": "AAA", "KEY_B": "BBB"}, {"KEY_A": "AAA", "KEY_B": "BBB"}),
            ({"KEY_A": "AAA", "KEY_B": "BBB"}, {"KEY_A": "AAA", "KEY_B": "BBB"}),
        )
    )
    def test_json_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(json.dumps(file_content)):
            variables = local_filesystem.load_variables("a.json")
            assert expected_variables == variables

    @mock.patch("airflow.utils.parse.os.path.exists", return_value=False)
    def test_missing_file(self, mock_exists):
        with pytest.raises(
            AirflowException,
            match=re.escape("File a.json was not found."),
        ):
            local_filesystem.load_variables("a.json")

    @parameterized.expand(
        (
            ("KEY: AAA", {"KEY": "AAA"}),
            (
                """
            KEY_A: AAA
            KEY_B: BBB
            """,
                {"KEY_A": "AAA", "KEY_B": "BBB"},
            ),
        )
    )
    def test_yaml_file_should_load_variables(self, file_content, expected_variables):
        with mock_local_file(file_content):
            variables = local_filesystem.load_variables('a.yaml')
            assert expected_variables == variables


class TestLoadConnection(unittest.TestCase):
    @parameterized.expand(
        (
            ("CONN_ID=mysql://host_1/", {"CONN_ID": "mysql://host_1"}),
            (
                "CONN_ID1=mysql://host_1/\nCONN_ID2=mysql://host_2/",
                {"CONN_ID1": "mysql://host_1", "CONN_ID2": "mysql://host_2"},
            ),
            (
                "CONN_ID1=mysql://host_1/\n # AAAA\nCONN_ID2=mysql://host_2/",
                {"CONN_ID1": "mysql://host_1", "CONN_ID2": "mysql://host_2"},
            ),
            (
                "\n\n\n\nCONN_ID1=mysql://host_1/\n\n\n\n\nCONN_ID2=mysql://host_2/\n\n\n",
                {"CONN_ID1": "mysql://host_1", "CONN_ID2": "mysql://host_2"},
            ),
        )
    )
    def test_env_file_should_load_connection(self, file_content, expected_connection_uris):
        with mock_local_file(file_content):
            connection_by_conn_id = local_filesystem.load_connections_dict("a.env")
            connection_uris_by_conn_id = {
                conn_id: connection.get_uri() for conn_id, connection in connection_by_conn_id.items()
            }

            assert expected_connection_uris == connection_uris_by_conn_id

    @parameterized.expand(
        (
            ("AA", 'Invalid line format. The line should contain at least one equal sign ("=")'),
            ("=", "Invalid line format. Key is empty."),
        )
    )
    def test_env_file_invalid_format(self, content, expected_message):
        with mock_local_file(content):
            with pytest.raises(AirflowFileParseException, match=re.escape(expected_message)):
                local_filesystem.load_connections_dict("a.env")

    @parameterized.expand(
        (
            ({"CONN_ID": "mysql://host_1"}, {"CONN_ID": "mysql://host_1"}),
            ({"CONN_ID": ["mysql://host_1"]}, {"CONN_ID": "mysql://host_1"}),
            ({"CONN_ID": {"uri": "mysql://host_1"}}, {"CONN_ID": "mysql://host_1"}),
            ({"CONN_ID": [{"uri": "mysql://host_1"}]}, {"CONN_ID": "mysql://host_1"}),
        )
    )
    def test_json_file_should_load_connection(self, file_content, expected_connection_uris):
        with mock_local_file(json.dumps(file_content)):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.json")
            connection_uris_by_conn_id = {
                conn_id: connection.get_uri() for conn_id, connection in connections_by_conn_id.items()
            }

            assert expected_connection_uris == connection_uris_by_conn_id

    @parameterized.expand(
        (
            ({"CONN_ID": None}, "Unexpected value type: NoneType."),
            ({"CONN_ID": 1}, "Unexpected value type: int."),
            ({"CONN_ID": [2]}, "Unexpected value type: int."),
            ({"CONN_ID": [None]}, "Unexpected value type: NoneType."),
            ({"CONN_ID": {"AAA": "mysql://host_1"}}, "The object have illegal keys: AAA."),
            ({"CONN_ID": {"conn_id": "BBBB"}}, "Mismatch conn_id."),
            ({"CONN_ID": ["mysql://", "mysql://"]}, "Found multiple values for CONN_ID in a.json."),
        )
    )
    def test_env_file_invalid_input(self, file_content, expected_connection_uris):
        with mock_local_file(json.dumps(file_content)):
            with pytest.raises(AirflowException, match=re.escape(expected_connection_uris)):
                local_filesystem.load_connections_dict("a.json")

    @mock.patch("airflow.utils.parse.os.path.exists", return_value=False)
    def test_missing_file(self, mock_exists):
        with pytest.raises(
            AirflowException,
            match=re.escape("File a.json was not found."),
        ):
            local_filesystem.load_connections_dict("a.json")

    @parameterized.expand(
        (
            (
                """CONN_A: 'mysql://host_a'""",
                {"CONN_A": {'conn_type': 'mysql', 'host': 'host_a'}},
            ),
            (
                """
            conn_a: mysql://hosta
            conn_b:
               conn_type: scheme
               host: host
               schema: lschema
               login: Login
               password: None
               port: 1234
               extra_dejson:
                 arbitrary_dict:
                    a: b
                 extra__google_cloud_platform__keyfile_dict: '{"a": "b"}'
                 extra__google_cloud_platform__keyfile_path: asaa""",
                {
                    "conn_a": {'conn_type': 'mysql', 'host': 'hosta'},
                    "conn_b": {
                        'conn_type': 'scheme',
                        'host': 'host',
                        'schema': 'lschema',
                        'login': 'Login',
                        'password': 'None',
                        'port': 1234,
                        'extra_dejson': {
                            'arbitrary_dict': {"a": "b"},
                            'extra__google_cloud_platform__keyfile_dict': '{"a": "b"}',
                            'extra__google_cloud_platform__keyfile_path': 'asaa',
                        },
                    },
                },
            ),
        )
    )
    def test_yaml_file_should_load_connection(self, file_content, expected_attrs_dict):
        with mock_local_file(file_content):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.yaml")
            for conn_id, connection in connections_by_conn_id.items():
                expected_attrs = expected_attrs_dict[conn_id]
                actual_attrs = {k: getattr(connection, k) for k in expected_attrs.keys()}
                assert actual_attrs == expected_attrs

    @parameterized.expand(
        (
            (
                """
                conn_c:
                   conn_type: scheme
                   host: host
                   schema: lschema
                   login: Login
                   password: None
                   port: 1234
                   extra_dejson:
                     aws_conn_id: bbb
                     region_name: ccc
                 """,
                {"conn_c": {"aws_conn_id": "bbb", "region_name": "ccc"}},
            ),
            (
                """
                conn_d:
                   conn_type: scheme
                   host: host
                   schema: lschema
                   login: Login
                   password: None
                   port: 1234
                   extra_dejson:
                     extra__google_cloud_platform__keyfile_dict:
                       a: b
                     extra__google_cloud_platform__key_path: xxx
                """,
                {
                    "conn_d": {
                        "extra__google_cloud_platform__keyfile_dict": {"a": "b"},
                        "extra__google_cloud_platform__key_path": "xxx",
                    }
                },
            ),
            (
                """
                conn_d:
                   conn_type: scheme
                   host: host
                   schema: lschema
                   login: Login
                   password: None
                   port: 1234
                   extra: '{\"extra__google_cloud_platform__keyfile_dict\": {\"a\": \"b\"}}'
                """,
                {"conn_d": {"extra__google_cloud_platform__keyfile_dict": {"a": "b"}}},
            ),
        )
    )
    def test_yaml_file_should_load_connection_extras(self, file_content, expected_extras):
        with mock_local_file(file_content):
            connections_by_conn_id = local_filesystem.load_connections_dict("a.yaml")
            connection_uris_by_conn_id = {
                conn_id: connection.extra_dejson for conn_id, connection in connections_by_conn_id.items()
            }
            assert expected_extras == connection_uris_by_conn_id

    @parameterized.expand(
        (
            (
                """conn_c:
               conn_type: scheme
               host: host
               schema: lschema
               login: Login
               password: None
               port: 1234
               extra:
                 abc: xyz
               extra_dejson:
                 aws_conn_id: bbb
                 region_name: ccc
                 """,
                "The extra and extra_dejson parameters are mutually exclusive.",
            ),
        )
    )
    def test_yaml_invalid_extra(self, file_content, expected_message):
        with mock_local_file(file_content):
            with pytest.raises(AirflowException, match=re.escape(expected_message)):
                local_filesystem.load_connections_dict("a.yaml")

    @parameterized.expand(
        ("CONN_ID=mysql://host_1/\nCONN_ID=mysql://host_2/",),
    )
    def test_ensure_unique_connection_env(self, file_content):
        with mock_local_file(file_content):
            with pytest.raises(ConnectionNotUnique):
                local_filesystem.load_connections_dict("a.env")

    @parameterized.expand(
        (
            ({"CONN_ID": ["mysql://host_1", "mysql://host_2"]},),
            ({"CONN_ID": [{"uri": "mysql://host_1"}, {"uri": "mysql://host_2"}]},),
        )
    )
    def test_ensure_unique_connection_json(self, file_content):
        with mock_local_file(json.dumps(file_content)):
            with pytest.raises(ConnectionNotUnique):
                local_filesystem.load_connections_dict("a.json")

    @parameterized.expand(
        (
            (
                """
            conn_a:
              - mysql://hosta
              - mysql://hostb"""
            ),
        ),
    )
    def test_ensure_unique_connection_yaml(self, file_content):
        with mock_local_file(file_content):
            with pytest.raises(ConnectionNotUnique):
                local_filesystem.load_connections_dict("a.yaml")


class TestLocalFileBackend(unittest.TestCase):
    def test_should_read_variable(self):
        with NamedTemporaryFile(suffix="var.env") as tmp_file:
            tmp_file.write(b"KEY_A=VAL_A")
            tmp_file.flush()
            backend = LocalFilesystemBackend(variables_file_path=tmp_file.name)
            assert "VAL_A" == backend.get_variable("KEY_A")
            assert backend.get_variable("KEY_B") is None

    def test_should_read_connection(self):
        with NamedTemporaryFile(suffix=".env") as tmp_file:
            tmp_file.write(b"CONN_A=mysql://host_a")
            tmp_file.flush()
            backend = LocalFilesystemBackend(connections_file_path=tmp_file.name)
            assert ["mysql://host_a"] == [conn.get_uri() for conn in backend.get_connections("CONN_A")]
            assert backend.get_variable("CONN_B") is None

    def test_files_are_optional(self):
        backend = LocalFilesystemBackend()
        assert [] == backend.get_connections("CONN_A")
        assert backend.get_variable("VAR_A") is None
