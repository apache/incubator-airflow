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

import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import ANY

from azure.common import AzureHttpError

from airflow.models import DAG, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.log.wasb_task_handler import WasbTaskHandler
from airflow.utils.state import State
from tests.test_utils.config import conf_vars


class TestWasbTaskHandler(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.wasb_log_folder = 'wasb://container/remote/log/location'
        self.remote_log_location = 'remote/log/location/1.log'
        self.local_log_location = 'local/log/location'
        self.container_name = "wasb-container"
        self.filename_template = '{try_number}.log'
        self.wasb_task_handler = WasbTaskHandler(
            base_log_folder=self.local_log_location,
            wasb_log_folder=self.wasb_log_folder,
            wasb_container=self.container_name,
            filename_template=self.filename_template,
            delete_local_copy=True,
        )

        date = datetime(2020, 8, 10)
        self.dag = DAG('dag_for_testing_file_task_handler', start_date=date)
        task = DummyOperator(task_id='task_for_testing_file_log_handler', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=date)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)

    @conf_vars({('logging', 'remote_log_conn_id'): 'wasb_default'})
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_hook(self, mock_service):
        assert isinstance(self.wasb_task_handler.hook, WasbHook)

    @conf_vars({('logging', 'remote_log_conn_id'): 'wasb_default'})
    def test_hook_raises(self):
        handler = WasbTaskHandler(
            self.local_log_location, self.wasb_log_folder, self.container_name, self.filename_template, True
        )
        with mock.patch.object(handler.log, 'error') as mock_error:
            with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook") as mock_hook:
                mock_hook.side_effect = AzureHttpError("failed to connect", 404)
                # Initialize the hook
                handler.hook

            mock_error.assert_called_once_with(
                'Could not create an WasbHook with connection id "%s". '
                'Please make sure that airflow[azure] is installed and '
                'the Wasb connection exists. Exception "%s"',
                "wasb_default",
                ANY,
            )

    def test_set_context_raw(self):
        self.ti.raw = True
        self.wasb_task_handler.set_context(self.ti)
        assert not self.wasb_task_handler.upload_on_close

    def test_set_context_not_raw(self):
        self.wasb_task_handler.set_context(self.ti)
        assert self.wasb_task_handler.upload_on_close

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_wasb_log_exists(self, mock_hook):
        instance = mock_hook.return_value
        instance.check_for_blob.return_value = True
        self.wasb_task_handler.wasb_log_exists(self.remote_log_location)
        mock_hook.return_value.check_for_blob.assert_called_once_with(
            self.container_name, self.remote_log_location
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_wasb_read(self, mock_hook):
        mock_hook.return_value.read_file.return_value = 'Log line'
        assert self.wasb_task_handler.wasb_read(self.remote_log_location) == "Log line"
        assert self.wasb_task_handler.read(self.ti) == (
            [
                [
                    (
                        '',
                        '*** Reading remote log from wasb://container/remote/log/location/1.log.\n'
                        'Log line\n',
                    )
                ]
            ],
            [{'end_of_log': True}],
        )

    def test_wasb_read_raises(self):
        handler = WasbTaskHandler(
            self.local_log_location, self.wasb_log_folder, self.container_name, self.filename_template, True
        )
        with mock.patch.object(handler.log, 'error') as mock_error:
            with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook") as mock_hook:
                mock_hook.return_value.read_file.side_effect = AzureHttpError("failed to connect", 404)

                handler.wasb_read(self.remote_log_location, return_error=True)
            mock_error.assert_called_once_with(
                "Message: '%s', exception '%s'",
                'Could not read logs from remote/log/location/1.log',
                ANY,
                exc_info=True,
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbTaskHandler, "wasb_read")
    @mock.patch.object(WasbTaskHandler, "wasb_log_exists")
    def test_write_log(self, mock_log_exists, mock_wasb_read, mock_hook):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = ""
        self.wasb_task_handler.wasb_write('text', self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "text", self.container_name, self.remote_log_location
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbTaskHandler, "wasb_read")
    @mock.patch.object(WasbTaskHandler, "wasb_log_exists")
    def test_write_on_existing_log(self, mock_log_exists, mock_wasb_read, mock_hook):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = "old log"
        self.wasb_task_handler.wasb_write('text', self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "old log\ntext", self.container_name, self.remote_log_location
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_write_when_append_is_false(self, mock_hook):
        self.wasb_task_handler.wasb_write('text', self.remote_log_location, False)
        mock_hook.return_value.load_string.assert_called_once_with(
            "text", self.container_name, self.remote_log_location
        )

    def test_write_raises(self):
        handler = WasbTaskHandler(
            self.local_log_location, self.wasb_log_folder, self.container_name, self.filename_template, True
        )
        with mock.patch.object(handler.log, 'error') as mock_error:
            with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook") as mock_hook:
                mock_hook.return_value.load_string.side_effect = AzureHttpError("failed to connect", 404)

                handler.wasb_write('text', self.remote_log_location, append=False)

            mock_error.assert_called_once_with(
                'Could not write logs to %s', 'remote/log/location/1.log', exc_info=True
            )
