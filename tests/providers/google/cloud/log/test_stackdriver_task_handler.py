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

import logging
import unittest
from datetime import datetime
from unittest import mock
from urllib.parse import parse_qs, urlparse

from google.cloud.logging import Resource
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse, LogEntry

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.log.stackdriver_task_handler import StackdriverTaskHandler
from airflow.utils.state import State


def _create_list_log_entries_response_mock(messages, token):
    return ListLogEntriesResponse(
        entries=[LogEntry(json_payload={"message": message}) for message in messages], next_page_token=token
    )


def _remove_stackdriver_handlers():
    for handler_ref in reversed(logging._handlerList[:]):
        handler = handler_ref()
        if not isinstance(handler, StackdriverTaskHandler):
            continue
        logging._removeHandlerRef(handler_ref)
        del handler


class TestStackdriverLoggingHandlerStandalone(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_pass_message_to_client(self, mock_client, mock_get_creds_and_project_id):
        self.addCleanup(_remove_stackdriver_handlers)

        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        transport_type = mock.MagicMock()
        stackdriver_task_handler = StackdriverTaskHandler(transport=transport_type, labels={"key": 'value'})
        logger = logging.getLogger("logger")
        logger.addHandler(stackdriver_task_handler)

        logger.info("test-message")
        stackdriver_task_handler.flush()

        transport_type.assert_called_once_with(mock_client.return_value, 'airflow')
        transport_type.return_value.send.assert_called_once_with(
            mock.ANY, 'test-message', labels={"key": 'value'}, resource=Resource(type='global', labels={})
        )
        mock_client.assert_called_once_with(credentials='creds', client_info=mock.ANY, project="project_id")


class TestStackdriverLoggingHandlerTask(unittest.TestCase):
    def setUp(self) -> None:
        self.transport_mock = mock.MagicMock()
        self.stackdriver_task_handler = StackdriverTaskHandler(transport=self.transport_mock)
        self.logger = logging.getLogger("logger")

        date = datetime(2016, 1, 1)
        self.dag = DAG('dag_for_testing_file_task_handler', start_date=date)
        task = DummyOperator(task_id='task_for_testing_file_log_handler', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=date)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)
        self.addCleanup(_remove_stackdriver_handlers)

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_set_labels(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        self.stackdriver_task_handler.set_context(self.ti)
        self.logger.addHandler(self.stackdriver_task_handler)

        self.logger.info("test-message")
        self.stackdriver_task_handler.flush()

        labels = {
            'task_id': 'task_for_testing_file_log_handler',
            'dag_id': 'dag_for_testing_file_task_handler',
            'execution_date': '2016-01-01T00:00:00+00:00',
            'try_number': '1',
        }
        resource = Resource(type='global', labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, 'test-message', labels=labels, resource=resource
        )

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_append_labels(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')
        self.stackdriver_task_handler = StackdriverTaskHandler(
            transport=self.transport_mock, labels={"product.googleapis.com/task_id": "test-value"}
        )
        self.stackdriver_task_handler.set_context(self.ti)
        self.logger.addHandler(self.stackdriver_task_handler)

        self.logger.info("test-message")
        self.stackdriver_task_handler.flush()

        labels = {
            'task_id': 'task_for_testing_file_log_handler',
            'dag_id': 'dag_for_testing_file_task_handler',
            'execution_date': '2016-01-01T00:00:00+00:00',
            'try_number': '1',
            'product.googleapis.com/task_id': 'test-value',
        }
        resource = Resource(type='global', labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, 'test-message', labels=labels, resource=resource
        )

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_for_all_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.stackdriver_task_handler.read(self.ti)
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="global"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )
        assert [(('default-hostname', 'MSG1\nMSG2'),)] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_for_task_with_quote(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')
        self.ti.task_id = "K\"OT"
        logs, metadata = self.stackdriver_task_handler.read(self.ti)
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="global"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'labels.task_id="K\\"OT"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )
        assert [(('default-hostname', 'MSG1\nMSG2'),)] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_for_single_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.stackdriver_task_handler.read(self.ti, 3)
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="global"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"\n'
                    'labels.try_number="3"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )
        assert [(('default-hostname', 'MSG1\nMSG2'),)] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_with_pagination(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.side_effect = [
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG1", "MSG2"], "TOKEN1")])),
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG3", "MSG4"], None)])),
        ]
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')
        logs, metadata1 = self.stackdriver_task_handler.read(self.ti, 3)
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    '''resource.type="global"
logName="projects/project_id/logs/airflow"
labels.task_id="task_for_testing_file_log_handler"
labels.dag_id="dag_for_testing_file_task_handler"
labels.execution_date="2016-01-01T00:00:00+00:00"
labels.try_number="3"'''
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )
        assert [(('default-hostname', 'MSG1\nMSG2'),)] == logs
        assert [{'end_of_log': False, 'next_page_token': 'TOKEN1'}] == metadata1

        mock_client.return_value.list_log_entries.return_value.next_page_token = None
        logs, metadata2 = self.stackdriver_task_handler.read(self.ti, 3, metadata1[0])

        mock_client.return_value.list_log_entries.assert_called_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="global"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"\n'
                    'labels.try_number="3"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token="TOKEN1",
            )
        )
        assert [(('default-hostname', 'MSG3\nMSG4'),)] == logs
        assert [{'end_of_log': True}] == metadata2

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_with_download(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.side_effect = [
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG1", "MSG2"], "TOKEN1")])),
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG3", "MSG4"], None)])),
        ]
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata1 = self.stackdriver_task_handler.read(self.ti, 3, {'download_logs': True})

        assert [(('default-hostname', 'MSG1\nMSG2\nMSG3\nMSG4'),)] == logs
        assert [{'end_of_log': True}] == metadata1

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_with_custom_resources(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')
        resource = Resource(
            type="cloud_composer_environment",
            labels={
                "environment.name": 'test-instance',
                "location": 'europe-west-3',
                "project_id": "project_id",
            },
        )
        self.stackdriver_task_handler = StackdriverTaskHandler(
            transport=self.transport_mock, resource=resource
        )

        entry = mock.MagicMock(json_payload={"message": "TEXT"})
        page = mock.MagicMock(entries=[entry, entry], next_page_token=None)
        mock_client.return_value.list_log_entries.return_value.pages = (n for n in [page])

        logs, metadata = self.stackdriver_task_handler.read(self.ti)
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="cloud_composer_environment"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'resource.labels."environment.name"="test-instance"\n'
                    'resource.labels.location="europe-west-3"\n'
                    'resource.labels.project_id="project_id"\n'
                    'labels.task_id="task_for_testing_file_log_handler"\n'
                    'labels.dag_id="dag_for_testing_file_task_handler"\n'
                    'labels.execution_date="2016-01-01T00:00:00+00:00"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )
        assert [(('default-hostname', 'TEXT\nTEXT'),)] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client')
    def test_should_use_credentials(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        stackdriver_task_handler = StackdriverTaskHandler(
            gcp_key_path="KEY_PATH",
        )

        client = stackdriver_task_handler._client

        mock_get_creds_and_project_id.assert_called_once_with(
            disable_logging=True,
            key_path='KEY_PATH',
            scopes=frozenset(
                {
                    'https://www.googleapis.com/auth/logging.write',
                    'https://www.googleapis.com/auth/logging.read',
                }
            ),
        )
        mock_client.assert_called_once_with(credentials='creds', client_info=mock.ANY, project="project_id")
        assert mock_client.return_value == client

    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client')
    def test_should_return_valid_external_url(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        stackdriver_task_handler = StackdriverTaskHandler(
            gcp_key_path="KEY_PATH",
        )

        url = stackdriver_task_handler.get_external_log_url(self.ti, self.ti.try_number)

        parsed_url = urlparse(url)
        parsed_qs = parse_qs(parsed_url.query)
        assert 'https' == parsed_url.scheme
        assert 'console.cloud.google.com' == parsed_url.netloc
        assert '/logs/viewer' == parsed_url.path
        assert {'project', 'interval', 'resource', 'advancedFilter'} == set(parsed_qs.keys())
        assert 'global' in parsed_qs['resource']

        filter_params = parsed_qs['advancedFilter'][0].split('\n')
        expected_filter = [
            'resource.type="global"',
            'logName="projects/project_id/logs/airflow"',
            f'labels.task_id="{self.ti.task_id}"',
            f'labels.dag_id="{self.dag.dag_id}"',
            f'labels.execution_date="{self.ti.execution_date.isoformat()}"',
            f'labels.try_number="{self.ti.try_number}"',
        ]
        assert set(expected_filter) == set(filter_params)
