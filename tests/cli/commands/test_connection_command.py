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

import io
import json
import re
import unittest
from contextlib import redirect_stdout
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.cli import cli_parser
from airflow.cli.commands import connection_command
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.db import merge_conn
from airflow.utils.session import create_session, provide_session
from tests.test_utils.db import clear_db_connections


class TestCliGetConnection(unittest.TestCase):
    def setUp(self):
        self.parser = cli_parser.get_parser()
        clear_db_connections()

    def tearDown(self):
        clear_db_connections()

    def test_cli_connection_get(self):
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_get(
                self.parser.parse_args(["connections", "get", "google_cloud_default", "--output", "json"])
            )
            stdout = stdout.getvalue()
        assert "google-cloud-platform:///default" in stdout

    def test_cli_connection_get_invalid(self):
        with pytest.raises(SystemExit, match=re.escape("Connection not found.")):
            connection_command.connections_get(self.parser.parse_args(["connections", "get", "INVALID"]))


class TestCliListConnections(unittest.TestCase):
    EXPECTED_CONS = [
        (
            'airflow_db',
            'mysql',
        ),
        (
            'google_cloud_default',
            'google_cloud_platform',
        ),
        (
            'http_default',
            'http',
        ),
        (
            'local_mysql',
            'mysql',
        ),
        (
            'mongo_default',
            'mongo',
        ),
        (
            'mssql_default',
            'mssql',
        ),
        (
            'mysql_default',
            'mysql',
        ),
        (
            'pinot_broker_default',
            'pinot',
        ),
        (
            'postgres_default',
            'postgres',
        ),
        (
            'presto_default',
            'presto',
        ),
        (
            'sqlite_default',
            'sqlite',
        ),
        (
            'trino_default',
            'trino',
        ),
        (
            'vertica_default',
            'vertica',
        ),
    ]

    def setUp(self):
        self.parser = cli_parser.get_parser()
        clear_db_connections()

    def tearDown(self):
        clear_db_connections()

    def test_cli_connections_list_as_json(self):
        args = self.parser.parse_args(["connections", "list", "--output", "json"])
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_list(args)
            print(stdout.getvalue())
            stdout = stdout.getvalue()

        for conn_id, conn_type in self.EXPECTED_CONS:
            assert conn_type in stdout
            assert conn_id in stdout

    def test_cli_connections_filter_conn_id(self):
        args = self.parser.parse_args(
            ["connections", "list", "--output", "json", '--conn-id', 'http_default']
        )

        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_list(args)
            stdout = stdout.getvalue()

        assert "http_default" in stdout


class TestCliExportConnections(unittest.TestCase):
    @provide_session
    def setUp(self, session=None):
        clear_db_connections(add_default_connections_back=False)
        merge_conn(
            Connection(
                conn_id="airflow_db",
                conn_type="mysql",
                description="mysql conn description",
                host="mysql",
                login="root",
                password="plainpassword",
                schema="airflow",
            ),
            session,
        )
        merge_conn(
            Connection(
                conn_id="druid_broker_default",
                conn_type="druid",
                description="druid-broker conn description",
                host="druid-broker",
                port=8082,
                extra='{"endpoint": "druid/v2/sql"}',
            ),
            session,
        )

        self.parser = cli_parser.get_parser()

    def tearDown(self):
        clear_db_connections()

    def test_cli_connections_export_should_return_error_for_invalid_command(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(
                [
                    "connections",
                    "export",
                ]
            )

    def test_cli_connections_export_should_return_error_for_invalid_format(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["connections", "export", "--format", "invalid", "/path/to/file"])

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_cli_connections_export_should_return_error_for_invalid_export_format(
        self, mock_file_open, mock_splittext
    ):
        output_filepath = '/tmp/connections.invalid'
        mock_splittext.return_value = (None, '.invalid')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        with pytest.raises(
            SystemExit, match=r"Unsupported file format. The file must have the extension .yaml, .json, .env"
        ):
            connection_command.connections_export(args)

        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_not_called()

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    @mock.patch.object(connection_command, 'create_session')
    def test_cli_connections_export_should_return_error_if_create_session_fails(
        self, mock_session, mock_file_open, mock_splittext
    ):
        output_filepath = '/tmp/connections.json'

        def my_side_effect():
            raise Exception("dummy exception")

        mock_session.side_effect = my_side_effect
        mock_splittext.return_value = (None, '.json')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        with pytest.raises(Exception, match=r"dummy exception"):
            connection_command.connections_export(args)

        mock_splittext.assert_not_called()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_not_called()

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    @mock.patch.object(connection_command, 'create_session')
    def test_cli_connections_export_should_return_error_if_fetching_connections_fails(
        self, mock_session, mock_file_open, mock_splittext
    ):
        output_filepath = '/tmp/connections.json'

        def my_side_effect(_):
            raise Exception("dummy exception")

        mock_session.return_value.__enter__.return_value.query.return_value.order_by.side_effect = (
            my_side_effect
        )
        mock_splittext.return_value = (None, '.json')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        with pytest.raises(Exception, match=r"dummy exception"):
            connection_command.connections_export(args)

        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_not_called()

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    @mock.patch.object(connection_command, 'create_session')
    def test_cli_connections_export_should_not_return_error_if_connections_is_empty(
        self, mock_session, mock_file_open, mock_splittext
    ):
        output_filepath = '/tmp/connections.json'

        mock_session.return_value.__enter__.return_value.query.return_value.all.return_value = []
        mock_splittext.return_value = (None, '.json')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        connection_command.connections_export(args)

        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_called_once_with('{}')

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_cli_connections_export_should_export_as_json(self, mock_file_open, mock_splittext):
        output_filepath = '/tmp/connections.json'
        mock_splittext.return_value = (None, '.json')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        connection_command.connections_export(args)

        expected_connections = json.dumps(
            {
                "airflow_db": {
                    "conn_type": "mysql",
                    "description": "mysql conn description",
                    "host": "mysql",
                    "login": "root",
                    "password": "plainpassword",
                    "schema": "airflow",
                    "port": None,
                    "extra": None,
                },
                "druid_broker_default": {
                    "conn_type": "druid",
                    "description": "druid-broker conn description",
                    "host": "druid-broker",
                    "login": None,
                    "password": None,
                    "schema": None,
                    "port": 8082,
                    "extra": "{\"endpoint\": \"druid/v2/sql\"}",
                },
            },
            indent=2,
        )

        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_called_once_with(expected_connections)

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_cli_connections_export_should_export_as_yaml(self, mock_file_open, mock_splittext):
        output_filepath = '/tmp/connections.yaml'
        mock_splittext.return_value = (None, '.yaml')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        connection_command.connections_export(args)

        expected_connections = (
            "airflow_db:\n"
            "  conn_type: mysql\n"
            "  description: mysql conn description\n"
            "  extra: null\n"
            "  host: mysql\n"
            "  login: root\n"
            "  password: plainpassword\n"
            "  port: null\n"
            "  schema: airflow\n"
            "druid_broker_default:\n"
            "  conn_type: druid\n"
            "  description: druid-broker conn description\n"
            "  extra: \'{\"endpoint\": \"druid/v2/sql\"}\'\n"
            "  host: druid-broker\n"
            "  login: null\n"
            "  password: null\n"
            "  port: 8082\n"
            "  schema: null\n"
        )
        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_called_once_with(expected_connections)

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_cli_connections_export_should_export_as_env(self, mock_file_open, mock_splittext):
        output_filepath = '/tmp/connections.env'
        mock_splittext.return_value = (None, '.env')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        connection_command.connections_export(args)

        expected_connections = [
            "airflow_db=mysql://root:plainpassword@mysql/airflow\n"
            "druid_broker_default=druid://druid-broker:8082?endpoint=druid%2Fv2%2Fsql\n",
            "druid_broker_default=druid://druid-broker:8082?endpoint=druid%2Fv2%2Fsql\n"
            "airflow_db=mysql://root:plainpassword@mysql/airflow\n",
        ]

        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_called_once_with(mock.ANY)
        assert mock_file_open.return_value.write.call_args_list[0][0][0] in expected_connections

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_cli_connections_export_should_export_as_env_for_uppercase_file_extension(
        self, mock_file_open, mock_splittext
    ):
        output_filepath = '/tmp/connections.ENV'
        mock_splittext.return_value = (None, '.ENV')

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
            ]
        )
        connection_command.connections_export(args)

        expected_connections = [
            "airflow_db=mysql://root:plainpassword@mysql/airflow\n"
            "druid_broker_default=druid://druid-broker:8082?endpoint=druid%2Fv2%2Fsql\n",
            "druid_broker_default=druid://druid-broker:8082?endpoint=druid%2Fv2%2Fsql\n"
            "airflow_db=mysql://root:plainpassword@mysql/airflow\n",
        ]

        mock_splittext.assert_called_once()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_called_once_with(mock.ANY)
        assert mock_file_open.return_value.write.call_args_list[0][0][0] in expected_connections

    @mock.patch('os.path.splitext')
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_cli_connections_export_should_force_export_as_specified_format(
        self, mock_file_open, mock_splittext
    ):
        output_filepath = '/tmp/connections.yaml'

        args = self.parser.parse_args(
            [
                "connections",
                "export",
                output_filepath,
                "--format",
                "json",
            ]
        )
        connection_command.connections_export(args)

        expected_connections = json.dumps(
            {
                "airflow_db": {
                    "conn_type": "mysql",
                    "description": "mysql conn description",
                    "host": "mysql",
                    "login": "root",
                    "password": "plainpassword",
                    "schema": "airflow",
                    "port": None,
                    "extra": None,
                },
                "druid_broker_default": {
                    "conn_type": "druid",
                    "description": "druid-broker conn description",
                    "host": "druid-broker",
                    "login": None,
                    "password": None,
                    "schema": None,
                    "port": 8082,
                    "extra": "{\"endpoint\": \"druid/v2/sql\"}",
                },
            },
            indent=2,
        )
        mock_splittext.assert_not_called()
        mock_file_open.assert_called_once_with(output_filepath, 'w', -1, 'UTF-8', None)
        mock_file_open.return_value.write.assert_called_once_with(expected_connections)


TEST_URL = "postgresql://airflow:airflow@host:5432/airflow"


class TestCliAddConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()
        clear_db_connections()

    @classmethod
    def tearDownClass(cls):
        clear_db_connections()

    @parameterized.expand(
        [
            (
                [
                    "connections",
                    "add",
                    "new0",
                    f"--conn-uri={TEST_URL}",
                    "--conn-description=new0 description",
                ],
                "Successfully added `conn_id`=new0 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new0 description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new1",
                    f"--conn-uri={TEST_URL}",
                    "--conn-description=new1 description",
                ],
                "Successfully added `conn_id`=new1 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new1 description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new2",
                    f"--conn-uri={TEST_URL}",
                    "--conn-extra",
                    "{'extra': 'yes'}",
                ],
                "Successfully added `conn_id`=new2 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": None,
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": True,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new3",
                    f"--conn-uri={TEST_URL}",
                    "--conn-extra",
                    "{'extra': 'yes'}",
                    "--conn-description",
                    "new3 description",
                ],
                "Successfully added `conn_id`=new3 : postgresql://airflow:airflow@host:5432/airflow",
                {
                    "conn_type": "postgres",
                    "description": "new3 description",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": True,
                    "login": "airflow",
                    "port": 5432,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new4",
                    "--conn-type=hive_metastore",
                    "--conn-login=airflow",
                    "--conn-password=airflow",
                    "--conn-host=host",
                    "--conn-port=9083",
                    "--conn-schema=airflow",
                    "--conn-description=  new4 description  ",
                ],
                "Successfully added `conn_id`=new4 : hive_metastore://airflow:******@host:9083/airflow",
                {
                    "conn_type": "hive_metastore",
                    "description": "  new4 description  ",
                    "host": "host",
                    "is_encrypted": True,
                    "is_extra_encrypted": False,
                    "login": "airflow",
                    "port": 9083,
                    "schema": "airflow",
                },
            ),
            (
                [
                    "connections",
                    "add",
                    "new5",
                    "--conn-uri",
                    "",
                    "--conn-type=google_cloud_platform",
                    "--conn-extra",
                    "{'extra': 'yes'}",
                    "--conn-description=new5 description",
                ],
                "Successfully added `conn_id`=new5 : google_cloud_platform://:@:",
                {
                    "conn_type": "google_cloud_platform",
                    "description": "new5 description",
                    "host": None,
                    "is_encrypted": False,
                    "is_extra_encrypted": True,
                    "login": None,
                    "port": None,
                    "schema": None,
                },
            ),
        ]
    )
    def test_cli_connection_add(self, cmd, expected_output, expected_conn):
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_add(self.parser.parse_args(cmd))

        stdout = stdout.getvalue()

        assert expected_output in stdout
        conn_id = cmd[2]
        with create_session() as session:
            comparable_attrs = [
                "conn_type",
                "description",
                "host",
                "is_encrypted",
                "is_extra_encrypted",
                "login",
                "port",
                "schema",
            ]
            current_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            assert expected_conn == {attr: getattr(current_conn, attr) for attr in comparable_attrs}

    def test_cli_connections_add_duplicate(self):
        conn_id = "to_be_duplicated"
        connection_command.connections_add(
            self.parser.parse_args(["connections", "add", conn_id, f"--conn-uri={TEST_URL}"])
        )
        # Check for addition attempt
        with pytest.raises(SystemExit, match=rf"A connection with `conn_id`={conn_id} already exists"):
            connection_command.connections_add(
                self.parser.parse_args(["connections", "add", conn_id, f"--conn-uri={TEST_URL}"])
            )

    def test_cli_connections_add_delete_with_missing_parameters(self):
        # Attempt to add without providing conn_uri
        with pytest.raises(
            SystemExit,
            match=r"The following args are required to add a connection: \['conn-uri or conn-type'\]",
        ):
            connection_command.connections_add(self.parser.parse_args(["connections", "add", "new1"]))

    def test_cli_connections_add_invalid_uri(self):
        # Attempt to add with invalid uri
        with pytest.raises(SystemExit, match=r"The URI provided to --conn-uri is invalid: nonsense_uri"):
            connection_command.connections_add(
                self.parser.parse_args(["connections", "add", "new1", f"--conn-uri={'nonsense_uri'}"])
            )


class TestCliDeleteConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()
        clear_db_connections()

    @classmethod
    def tearDownClass(cls):
        clear_db_connections()

    @provide_session
    def test_cli_delete_connections(self, session=None):
        merge_conn(
            Connection(
                conn_id="new1",
                conn_type="mysql",
                description="mysql description",
                host="mysql",
                login="root",
                password="",
                schema="airflow",
            ),
            session=session,
        )
        # Delete connections
        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_delete(self.parser.parse_args(["connections", "delete", "new1"]))
            stdout = stdout.getvalue()

        # Check deletion stdout
        assert "Successfully deleted connection with `conn_id`=new1" in stdout

        # Check deletions
        result = session.query(Connection).filter(Connection.conn_id == "new1").first()

        assert result is None

    def test_cli_delete_invalid_connection(self):
        # Attempt to delete a non-existing connection
        with pytest.raises(SystemExit, match=r"Did not find a connection with `conn_id`=fake"):
            connection_command.connections_delete(self.parser.parse_args(["connections", "delete", "fake"]))


class TestCliImportConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()
        clear_db_connections(add_default_connections_back=False)

    @classmethod
    def tearDownClass(cls):
        clear_db_connections()

    @mock.patch('os.path.exists')
    def test_cli_connections_import_should_return_error_if_file_does_not_exist(self, mock_exists):
        mock_exists.return_value = False
        filepath = '/does/not/exist.json'
        with pytest.raises(SystemExit, match=r"Missing connections file."):
            connection_command.connections_import(self.parser.parse_args(["connections", "import", filepath]))

    @parameterized.expand(
        [
            ("sample.jso",),
            ("sample.yml",),
            ("sample.environ",),
        ]
    )
    @mock.patch('os.path.exists')
    def test_cli_connections_import_should_return_error_if_file_format_is_invalid(
        self, filepath, mock_exists
    ):
        mock_exists.return_value = True
        with pytest.raises(
            AirflowException,
            match=r"Unsupported file format. The file must have the extension .env or .json or .yaml",
        ):
            connection_command.connections_import(self.parser.parse_args(["connections", "import", filepath]))

    @mock.patch('airflow.secrets.local_filesystem._parse_secret_file')
    @mock.patch('os.path.exists')
    def test_cli_connections_import_should_load_connections(self, mock_exists, mock_parse_secret_file):
        mock_exists.return_value = True

        # Sample connections to import
        expected_connections = {
            "new0": {
                "conn_type": "postgres",
                "description": "new0 description",
                "host": "host",
                "login": "airflow",
                "password": "password",
                "port": 5432,
                "schema": "airflow",
                "extra": "test",
            },
            "new1": {
                "conn_type": "mysql",
                "description": "new1 description",
                "host": "host",
                "login": "airflow",
                "password": "password",
                "port": 3306,
                "schema": "airflow",
                "extra": "test",
            },
        }

        # The serialized connections are read in using _parse_secret_file, which returns a dictionary
        mock_parse_secret_file.return_value = expected_connections

        connection_command.connections_import(self.parser.parse_args(["connections", "import", "dummy.json"]))

        # Verify that the imported connections match the expected, sample connections
        with create_session() as session:
            current_conns = session.query(Connection).all()

            comparable_attrs = [
                "conn_id",
                "conn_type",
                "description",
                "host",
                "login",
                "password",
                "port",
                "schema",
                "extra",
            ]

            current_conns_as_dicts = {
                current_conn.conn_id: {attr: getattr(current_conn, attr) for attr in comparable_attrs}
                for current_conn in current_conns
            }
            assert expected_connections == current_conns_as_dicts

    @provide_session
    @mock.patch('airflow.secrets.local_filesystem._parse_secret_file')
    @mock.patch('os.path.exists')
    def test_cli_connections_import_should_not_overwrite_existing_connections(
        self, mock_exists, mock_parse_secret_file, session=None
    ):
        mock_exists.return_value = True

        # Add a pre-existing connection "new3"
        merge_conn(
            Connection(
                conn_id="new3",
                conn_type="mysql",
                description="mysql description",
                host="mysql",
                login="root",
                password="password",
                schema="airflow",
            ),
            session=session,
        )

        # Sample connections to import, including a collision with "new3"
        expected_connections = {
            "new2": {
                "conn_type": "postgres",
                "description": "new2 description",
                "host": "host",
                "login": "airflow",
                "password": "password",
                "port": 5432,
                "schema": "airflow",
                "extra": "test",
            },
            "new3": {
                "conn_type": "mysql",
                "description": "new3 description",
                "host": "host",
                "login": "airflow",
                "password": "new password",
                "port": 3306,
                "schema": "airflow",
                "extra": "test",
            },
        }

        # The serialized connections are read in using _parse_secret_file, which returns a dictionary
        mock_parse_secret_file.return_value = expected_connections

        with redirect_stdout(io.StringIO()) as stdout:
            connection_command.connections_import(
                self.parser.parse_args(["connections", "import", 'dummy.json'])
            )

            assert 'Could not import connection new3: connection already exists.' in stdout.getvalue()

        # Verify that the imported connections match the expected, sample connections
        current_conns = session.query(Connection).all()

        comparable_attrs = [
            "conn_id",
            "conn_type",
            "description",
            "host",
            "login",
            "password",
            "port",
            "schema",
            "extra",
        ]

        current_conns_as_dicts = {
            current_conn.conn_id: {attr: getattr(current_conn, attr) for attr in comparable_attrs}
            for current_conn in current_conns
        }
        assert current_conns_as_dicts['new2'] == expected_connections['new2']

        # The existing connection's description should not have changed
        assert current_conns_as_dicts['new3']['description'] == 'mysql description'
