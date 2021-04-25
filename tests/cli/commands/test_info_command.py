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

import contextlib
import importlib
import io
import logging
import os
import unittest
from unittest import mock

from parameterized import parameterized
from rich.console import Console

from airflow.cli import cli_parser
from airflow.cli.commands import info_command
from airflow.config_templates import airflow_local_settings
from airflow.logging_config import configure_logging
from airflow.version import version as airflow_version
from tests.test_utils.config import conf_vars


def capture_show_output(instance):
    console = Console()
    with console.capture() as capture:
        instance.info(console)
    return capture.get()


class TestPiiAnonymizer(unittest.TestCase):
    def setUp(self) -> None:
        self.instance = info_command.PiiAnonymizer()

    def test_should_remove_pii_from_path(self):
        home_path = os.path.expanduser("~/airflow/config")
        assert "${HOME}/airflow/config" == self.instance.process_path(home_path)

    @parameterized.expand(
        [
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres@postgres/airflow",
                "postgresql+psycopg2://p...s@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://:airflow@postgres/airflow",
                "postgresql+psycopg2://:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres/airflow",
                "postgresql+psycopg2://postgres/airflow",
            ),
        ]
    )
    def test_should_remove_pii_from_url(self, before, after):
        assert after == self.instance.process_url(before)


class TestAirflowInfo:
    @classmethod
    def setup_class(cls):
        # pylint: disable=attribute-defined-outside-init
        cls.parser = cli_parser.get_parser()

    @classmethod
    def teardown_class(cls) -> None:
        for handler_ref in logging._handlerList[:]:
            logging._removeHandlerRef(handler_ref)
        importlib.reload(airflow_local_settings)
        configure_logging()

    @staticmethod
    def unique_items(items):
        return {i[0] for i in items}

    @conf_vars(
        {
            ("core", "executor"): "TEST_EXECUTOR",
            ("core", "dags_folder"): "TEST_DAGS_FOLDER",
            ("core", "plugins_folder"): "TEST_PLUGINS_FOLDER",
            ("logging", "base_log_folder"): "TEST_LOG_FOLDER",
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
            ('logging', 'remote_logging'): 'True',
            ('logging', 'remote_base_log_folder'): 's3://logs-name',
        }
    )
    def test_airflow_info(self):
        importlib.reload(airflow_local_settings)
        configure_logging()

        instance = info_command.AirflowInfo(info_command.NullAnonymizer())
        expected = {
            'executor',
            'version',
            'task_logging_handler',
            'plugins_folder',
            'base_log_folder',
            'remote_base_log_folder',
            'dags_folder',
            'sql_alchemy_conn',
        }
        assert self.unique_items(instance._airflow_info) == expected

    def test_system_info(self):
        instance = info_command.AirflowInfo(info_command.NullAnonymizer())
        expected = {'uname', 'architecture', 'OS', 'python_location', 'locale', 'python_version'}
        assert self.unique_items(instance._system_info) == expected

    def test_paths_info(self):
        instance = info_command.AirflowInfo(info_command.NullAnonymizer())
        expected = {'airflow_on_path', 'airflow_home', 'system_path', 'python_path'}
        assert self.unique_items(instance._paths_info) == expected

    def test_tools_info(self):
        instance = info_command.AirflowInfo(info_command.NullAnonymizer())
        expected = {
            'cloud_sql_proxy',
            'gcloud',
            'git',
            'kubectl',
            'mysql',
            'psql',
            'sqlite3',
            'ssh',
        }
        assert self.unique_items(instance._tools_info) == expected

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info"]))

        output = stdout.getvalue()
        assert airflow_version in output
        assert "postgresql+psycopg2://postgres:airflow@postgres/airflow" in output

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info_anonymize(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info", "--anonymize"]))

        output = stdout.getvalue()
        assert airflow_version in output
        assert "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow" in output

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    @mock.patch(
        "airflow.cli.commands.info_command.requests",
        **{  # type: ignore
            "post.return_value.ok": True,
            "post.return_value.json.return_value": {
                "success": True,
                "key": "f9U3zs3I",
                "link": "https://file.io/TEST",
                "expiry": "14 days",
            },
        },
    )
    def test_show_info_anonymize_fileio(self, mock_requests):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            info_command.show_info(self.parser.parse_args(["info", "--file-io"]))

        assert "https://file.io/TEST" in stdout.getvalue()
        content = mock_requests.post.call_args[1]["data"]["text"]
        assert "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow" in content
