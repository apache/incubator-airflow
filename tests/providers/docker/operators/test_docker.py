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
import logging
import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException

try:
    from docker import APIClient

    from airflow.providers.docker.hooks.docker import DockerHook
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    pass


class TestDockerOperator(unittest.TestCase):
    def setUp(self):
        self.tempdir_patcher = mock.patch('airflow.providers.docker.operators.docker.TemporaryDirectory')
        self.tempdir_mock = self.tempdir_patcher.start()
        self.tempdir_mock.return_value.__enter__.return_value = '/mkdtemp'

        self.client_mock = mock.Mock(spec=APIClient)
        self.client_mock.create_container.return_value = {'Id': 'some_id'}
        self.client_mock.images.return_value = []
        self.client_mock.attach.return_value = ['container log']
        self.client_mock.logs.return_value = ['container log']
        self.client_mock.pull.return_value = {"status": "pull log"}
        self.client_mock.wait.return_value = {"StatusCode": 0}
        self.client_mock.create_host_config.return_value = mock.Mock()
        self.client_class_patcher = mock.patch(
            'airflow.providers.docker.operators.docker.APIClient',
            return_value=self.client_mock,
        )
        self.client_class_mock = self.client_class_patcher.start()

    def tearDown(self) -> None:
        self.tempdir_patcher.stop()
        self.client_class_patcher.stop()

    def test_execute(self):
        operator = DockerOperator(
            api_version='1.19',
            command='env',
            environment={'UNIT': 'TEST'},
            private_environment={'PRIVATE': 'MESSAGE'},
            image='ubuntu:latest',
            network_mode='bridge',
            owner='unittest',
            task_id='unittest',
            volumes=['/host/path:/container/path'],
            working_dir='/container/path',
            shm_size=1000,
            host_tmp_dir='/host/airflow',
            container_name='test_container',
            tty=True,
        )
        operator.execute(None)

        self.client_class_mock.assert_called_once_with(
            base_url='unix://var/run/docker.sock', tls=None, version='1.19'
        )

        self.client_mock.create_container.assert_called_once_with(
            command='env',
            name='test_container',
            environment={'AIRFLOW_TMP_DIR': '/tmp/airflow', 'UNIT': 'TEST', 'PRIVATE': 'MESSAGE'},
            host_config=self.client_mock.create_host_config.return_value,
            image='ubuntu:latest',
            user=None,
            working_dir='/container/path',
            tty=True,
        )
        self.client_mock.create_host_config.assert_called_once_with(
            binds=['/host/path:/container/path', '/mkdtemp:/tmp/airflow'],
            network_mode='bridge',
            shm_size=1000,
            cpu_shares=1024,
            mem_limit=None,
            auto_remove=False,
            dns=None,
            dns_search=None,
            cap_add=None,
            extra_hosts=None,
            privileged=False,
        )
        self.tempdir_mock.assert_called_once_with(dir='/host/airflow', prefix='airflowtmp')
        self.client_mock.images.assert_called_once_with(name='ubuntu:latest')
        self.client_mock.attach.assert_called_once_with(
            container='some_id', stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with('ubuntu:latest', stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with('some_id')
        assert (
            operator.cli.pull('ubuntu:latest', stream=True, decode=True) == self.client_mock.pull.return_value
        )

    def test_private_environment_is_private(self):
        operator = DockerOperator(
            private_environment={'PRIVATE': 'MESSAGE'}, image='ubuntu:latest', task_id='unittest'
        )
        assert operator._private_environment == {
            'PRIVATE': 'MESSAGE'
        }, "To keep this private, it must be an underscored attribute."

    @mock.patch('airflow.providers.docker.operators.docker.tls.TLSConfig')
    def test_execute_tls(self, tls_class_mock):
        tls_mock = mock.Mock()
        tls_class_mock.return_value = tls_mock

        operator = DockerOperator(
            docker_url='tcp://127.0.0.1:2376',
            image='ubuntu',
            owner='unittest',
            task_id='unittest',
            tls_client_cert='cert.pem',
            tls_ca_cert='ca.pem',
            tls_client_key='key.pem',
        )
        operator.execute(None)

        tls_class_mock.assert_called_once_with(
            assert_hostname=None,
            ca_cert='ca.pem',
            client_cert=('cert.pem', 'key.pem'),
            ssl_version=None,
            verify=True,
        )

        self.client_class_mock.assert_called_once_with(
            base_url='https://127.0.0.1:2376', tls=tls_mock, version=None
        )

    def test_execute_unicode_logs(self):
        self.client_mock.attach.return_value = ['unicode container log 😁']

        originalRaiseExceptions = logging.raiseExceptions  # pylint: disable=invalid-name
        logging.raiseExceptions = True

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')

        with mock.patch('traceback.print_exception') as print_exception_mock:
            operator.execute(None)
            logging.raiseExceptions = originalRaiseExceptions
            print_exception_mock.assert_not_called()

    def test_execute_container_fails(self):
        self.client_mock.wait.return_value = {"StatusCode": 1}
        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')
        with pytest.raises(AirflowException):
            operator.execute(None)

    def test_auto_remove_container_fails(self):
        self.client_mock.wait.return_value = {"StatusCode": 1}
        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest', auto_remove=True)
        operator.container = {'Id': 'some_id'}
        with pytest.raises(AirflowException):
            operator.execute(None)

        self.client_mock.remove_container.assert_called_once_with('some_id')

    @staticmethod
    def test_on_kill():
        client_mock = mock.Mock(spec=APIClient)

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')
        operator.cli = client_mock
        operator.container = {'Id': 'some_id'}

        operator.on_kill()

        client_mock.stop.assert_called_once_with('some_id')

    def test_execute_no_docker_conn_id_no_hook(self):
        # Create the DockerOperator
        operator = DockerOperator(image='publicregistry/someimage', owner='unittest', task_id='unittest')

        # Mock out the DockerHook
        hook_mock = mock.Mock(name='DockerHook mock', spec=DockerHook)
        hook_mock.get_conn.return_value = self.client_mock
        operator.get_hook = mock.Mock(
            name='DockerOperator.get_hook mock', spec=DockerOperator.get_hook, return_value=hook_mock
        )

        operator.execute(None)
        assert operator.get_hook.call_count == 0, 'Hook called though no docker_conn_id configured'

    @mock.patch('airflow.providers.docker.operators.docker.DockerHook')
    def test_execute_with_docker_conn_id_use_hook(self, hook_class_mock):
        # Create the DockerOperator
        operator = DockerOperator(
            image='publicregistry/someimage',
            owner='unittest',
            task_id='unittest',
            docker_conn_id='some_conn_id',
        )

        # Mock out the DockerHook
        hook_mock = mock.Mock(name='DockerHook mock', spec=DockerHook)
        hook_mock.get_conn.return_value = self.client_mock
        hook_class_mock.return_value = hook_mock

        operator.execute(None)

        assert self.client_class_mock.call_count == 0, 'Client was called on the operator instead of the hook'
        assert hook_class_mock.call_count == 1, 'Hook was not called although docker_conn_id configured'
        assert self.client_mock.pull.call_count == 1, 'Image was not pulled using operator client'

    def test_execute_xcom_behavior(self):
        self.client_mock.pull.return_value = [b'{"status":"pull log"}']

        kwargs = {
            'api_version': '1.19',
            'command': 'env',
            'environment': {'UNIT': 'TEST'},
            'private_environment': {'PRIVATE': 'MESSAGE'},
            'image': 'ubuntu:latest',
            'network_mode': 'bridge',
            'owner': 'unittest',
            'task_id': 'unittest',
            'volumes': ['/host/path:/container/path'],
            'working_dir': '/container/path',
            'shm_size': 1000,
            'host_tmp_dir': '/host/airflow',
            'container_name': 'test_container',
            'tty': True,
        }

        xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=True)
        no_xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=False)

        xcom_push_result = xcom_push_operator.execute(None)
        no_xcom_push_result = no_xcom_push_operator.execute(None)

        assert xcom_push_result == b'container log'
        assert no_xcom_push_result is None

    def test_extra_hosts(self):
        hosts_obj = mock.Mock()
        operator = DockerOperator(task_id='test', image='test', extra_hosts=hosts_obj)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert 'host_config' in self.client_mock.create_container.call_args[1]
        assert 'extra_hosts' in self.client_mock.create_host_config.call_args[1]
        assert hosts_obj is self.client_mock.create_host_config.call_args[1]['extra_hosts']

    def test_privileged(self):
        privileged = mock.Mock()
        operator = DockerOperator(task_id='test', image='test', privileged=privileged)
        operator.execute(None)
        self.client_mock.create_container.assert_called_once()
        assert 'host_config' in self.client_mock.create_container.call_args[1]
        assert 'privileged' in self.client_mock.create_host_config.call_args[1]
        assert privileged is self.client_mock.create_host_config.call_args[1]['privileged']
