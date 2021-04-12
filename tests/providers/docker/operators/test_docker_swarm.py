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

import unittest
from unittest import mock

import pytest
import requests
from docker import APIClient
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker_swarm import DockerSwarmOperator


class TestDockerSwarmOperator(unittest.TestCase):
    @mock.patch('airflow.providers.docker.operators.docker.APIClient')
    @mock.patch('airflow.providers.docker.operators.docker_swarm.types')
    def test_execute(self, types_mock, client_class_mock):

        mock_obj = mock.Mock()

        def _client_tasks_side_effect():
            for _ in range(2):
                yield [{'Status': {'State': 'pending'}}]
            while True:
                yield [{'Status': {'State': 'complete'}}]

        def _client_service_logs_effect():
            yield b'Testing is awesome.'

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {'ID': 'some_id'}
        client_mock.service_logs.return_value = _client_service_logs_effect()
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.side_effect = _client_tasks_side_effect()
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        client_class_mock.return_value = client_mock

        operator = DockerSwarmOperator(
            api_version='1.19',
            command='env',
            environment={'UNIT': 'TEST'},
            image='ubuntu:latest',
            mem_limit='128m',
            user='unittest',
            task_id='unittest',
            auto_remove=True,
            tty=True,
        )
        operator.execute(None)

        types_mock.TaskTemplate.assert_called_once_with(
            container_spec=mock_obj, restart_policy=mock_obj, resources=mock_obj
        )
        types_mock.ContainerSpec.assert_called_once_with(
            image='ubuntu:latest',
            command='env',
            user='unittest',
            tty=True,
            env={'UNIT': 'TEST', 'AIRFLOW_TMP_DIR': '/tmp/airflow'},
        )
        types_mock.RestartPolicy.assert_called_once_with(condition='none')
        types_mock.Resources.assert_called_once_with(mem_limit='128m')

        client_class_mock.assert_called_once_with(
            base_url='unix://var/run/docker.sock', tls=None, version='1.19'
        )

        client_mock.service_logs.assert_called_once_with(
            'some_id', follow=True, stdout=True, stderr=True, is_tty=True
        )

        csargs, cskwargs = client_mock.create_service.call_args_list[0]
        assert len(csargs) == 1, 'create_service called with different number of arguments than expected'
        assert csargs == (mock_obj,)
        assert cskwargs['labels'] == {'name': 'airflow__adhoc_airflow__unittest'}
        assert cskwargs['name'].startswith('airflow-')
        assert client_mock.tasks.call_count == 5
        client_mock.remove_service.assert_called_once_with('some_id')

    @mock.patch('airflow.providers.docker.operators.docker.APIClient')
    @mock.patch('airflow.providers.docker.operators.docker_swarm.types')
    def test_auto_remove(self, types_mock, client_class_mock):

        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {'ID': 'some_id'}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [{'Status': {'State': 'complete'}}]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        client_class_mock.return_value = client_mock

        operator = DockerSwarmOperator(image='', auto_remove=True, task_id='unittest', enable_logging=False)
        operator.execute(None)

        client_mock.remove_service.assert_called_once_with('some_id')

    @mock.patch('airflow.providers.docker.operators.docker.APIClient')
    @mock.patch('airflow.providers.docker.operators.docker_swarm.types')
    def test_no_auto_remove(self, types_mock, client_class_mock):

        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {'ID': 'some_id'}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [{'Status': {'State': 'complete'}}]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        client_class_mock.return_value = client_mock

        operator = DockerSwarmOperator(image='', auto_remove=False, task_id='unittest', enable_logging=False)
        operator.execute(None)

        assert (
            client_mock.remove_service.call_count == 0
        ), 'Docker service being removed even when `auto_remove` set to `False`'

    @parameterized.expand([('failed',), ('shutdown',), ('rejected',), ('orphaned',), ('remove',)])
    @mock.patch('airflow.providers.docker.operators.docker.APIClient')
    @mock.patch('airflow.providers.docker.operators.docker_swarm.types')
    def test_non_complete_service_raises_error(self, status, types_mock, client_class_mock):

        mock_obj = mock.Mock()

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {'ID': 'some_id'}
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.return_value = [{'Status': {'State': status}}]
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        client_class_mock.return_value = client_mock

        operator = DockerSwarmOperator(image='', auto_remove=False, task_id='unittest', enable_logging=False)
        msg = "Service did not complete: {'ID': 'some_id'}"
        with pytest.raises(AirflowException) as ctx:
            operator.execute(None)
        assert str(ctx.value) == msg

    @mock.patch('airflow.providers.docker.operators.docker.APIClient')
    @mock.patch('airflow.providers.docker.operators.docker_swarm.types')
    def test_logging_with_requests_timeout(self, types_mock, client_class_mock):

        mock_obj = mock.Mock()

        def _client_tasks_side_effect():
            for _ in range(2):
                yield [{'Status': {'State': 'pending'}}]
            while True:
                yield [{'Status': {'State': 'complete'}}]

        def _client_service_logs_effect():
            yield b'Testing is awesome.'
            raise requests.exceptions.ConnectionError('')

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_service.return_value = {'ID': 'some_id'}
        client_mock.service_logs.return_value = _client_service_logs_effect()
        client_mock.images.return_value = []
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.tasks.side_effect = _client_tasks_side_effect()
        types_mock.TaskTemplate.return_value = mock_obj
        types_mock.ContainerSpec.return_value = mock_obj
        types_mock.RestartPolicy.return_value = mock_obj
        types_mock.Resources.return_value = mock_obj

        client_class_mock.return_value = client_mock

        operator = DockerSwarmOperator(
            api_version='1.19',
            command='env',
            environment={'UNIT': 'TEST'},
            image='ubuntu:latest',
            mem_limit='128m',
            user='unittest',
            task_id='unittest',
            auto_remove=True,
            tty=True,
            enable_logging=True,
        )
        operator.execute(None)

        client_mock.service_logs.assert_called_once_with(
            'some_id', follow=True, stdout=True, stderr=True, is_tty=True
        )

    def test_on_kill(self):
        client_mock = mock.Mock(spec=APIClient)

        operator = DockerSwarmOperator(image='', auto_remove=False, task_id='unittest', enable_logging=False)
        operator.cli = client_mock
        operator.service = {'ID': 'some_id'}

        operator.on_kill()

        client_mock.remove_service.assert_called_once_with('some_id')
