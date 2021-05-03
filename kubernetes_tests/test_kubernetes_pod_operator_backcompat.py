# pylint: disable=unused-argument
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
import sys
import unittest
from unittest import mock
from unittest.mock import patch

import kubernetes.client.models as k8s
import pendulum
import pytest
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.models import DAG, TaskInstance
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_launcher import PodLauncher
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.version import version as airflow_version


# noinspection DuplicatedCode
def create_context(task):
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("Europe/Amsterdam")
    execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=task, execution_date=execution_date)
    return {
        "dag": dag,
        "ts": execution_date.isoformat(),
        "task": task,
        "ti": task_instance,
    }


# noinspection DuplicatedCode,PyUnusedLocal
class TestKubernetesPodOperatorSystem(unittest.TestCase):
    def get_current_task_name(self):
        # reverse test name to make pod name unique (it has limited length)
        return "_" + unittest.TestCase.id(self).replace(".", "_")[::-1]

    def setUp(self):
        self.maxDiff = None  # pylint: disable=invalid-name
        self.api_client = ApiClient()
        self.expected_pod = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'namespace': 'default',
                'name': mock.ANY,
                'annotations': {},
                'labels': {
                    'foo': 'bar',
                    'kubernetes_pod_operator': 'True',
                    'airflow_version': airflow_version.replace('+', '-'),
                    'execution_date': '2016-01-01T0100000100-a2f50a31f',
                    'dag_id': 'dag',
                    'task_id': 'task',
                    'try_number': '1',
                },
            },
            'spec': {
                'affinity': {},
                'containers': [
                    {
                        'image': 'ubuntu:16.04',
                        'imagePullPolicy': 'IfNotPresent',
                        'args': ["echo 10"],
                        'command': ["bash", "-cx"],
                        'env': [],
                        'envFrom': [],
                        'resources': {},
                        'name': 'base',
                        'ports': [],
                        'volumeMounts': [],
                    }
                ],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'initContainers': [],
                'nodeSelector': {},
                'restartPolicy': 'Never',
                'securityContext': {},
                'serviceAccountName': 'default',
                'tolerations': [],
                'volumes': [],
            },
        }

    def tearDown(self):
        client = kube_client.get_kube_client(in_cluster=False)
        client.delete_collection_namespaced_pod(namespace="default")

    def create_context(self, task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
        }

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_image_pull_secrets_correctly_set(self, mock_client, monitor_mock, start_mock):
        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_secrets=fake_pull_secrets,
            cluster_context='default',
        )
        monitor_mock.return_value = (State.SUCCESS, None, None)
        context = self.create_context(k)
        k.execute(context=context)
        assert start_mock.call_args[0][0].spec.image_pull_secrets == [
            k8s.V1LocalObjectReference(name=fake_pull_secrets)
        ]

    def test_working_pod(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod['spec'] == actual_pod['spec']
        assert self.expected_pod['metadata']['labels'] == actual_pod['metadata']['labels']

    def test_pod_node_selectors(self):
        node_selectors = {'beta.kubernetes.io/os': 'linux'}
        k = KubernetesPodOperator(
            None,
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            node_selectors=node_selectors,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['nodeSelector'] = node_selectors
        assert self.expected_pod == actual_pod

    def test_pod_resources(self):
        resources = {
            'limit_cpu': 0.25,
            'limit_memory': '64Mi',
            'limit_ephemeral_storage': '2Gi',
            'request_cpu': '250m',
            'request_memory': '64Mi',
            'request_ephemeral_storage': '1Gi',
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            resources=resources,
        )
        context = self.create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['resources'] = {
            'requests': {'memory': '64Mi', 'cpu': '250m', 'ephemeral-storage': '1Gi'},
            'limits': {'memory': '64Mi', 'cpu': 0.25, 'ephemeral-storage': '2Gi'},
        }
        assert self.expected_pod == actual_pod

    def test_pod_affinity(self):
        affinity = {
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [
                        {
                            'matchExpressions': [
                                {'key': 'beta.kubernetes.io/os', 'operator': 'In', 'values': ['linux']}
                            ]
                        }
                    ]
                }
            }
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            affinity=affinity,
        )
        context = create_context(k)
        k.execute(context=context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['affinity'] = affinity
        assert self.expected_pod == actual_pod

    def test_port(self):
        port = Port('http', 80)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            ports=[port],
        )
        context = self.create_context(k)
        k.execute(context=context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['ports'] = [{'name': 'http', 'containerPort': 80}]
        assert self.expected_pod == actual_pod

    def test_volume_mount(self):
        with patch.object(PodLauncher, 'log') as mock_logger:
            volume_mount = VolumeMount(
                'test-volume', mount_path='/tmp/test_volume', sub_path=None, read_only=False
            )

            volume_config = {'persistentVolumeClaim': {'claimName': 'test-volume'}}
            volume = Volume(name='test-volume', configs=volume_config)
            args = [
                "echo \"retrieved from mount\" > /tmp/test_volume/test.txt "
                "&& cat /tmp/test_volume/test.txt"
            ]
            k = KubernetesPodOperator(
                namespace='default',
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=args,
                labels={"foo": "bar"},
                volume_mounts=[volume_mount],
                volumes=[volume],
                is_delete_operator_pod=False,
                name="test",
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
            )
            context = create_context(k)
            k.execute(context=context)
            mock_logger.info.assert_any_call('retrieved from mount')
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['args'] = args
            self.expected_pod['spec']['containers'][0]['volumeMounts'] = [
                {'name': 'test-volume', 'mountPath': '/tmp/test_volume', 'readOnly': False}
            ]
            self.expected_pod['spec']['volumes'] = [
                {'name': 'test-volume', 'persistentVolumeClaim': {'claimName': 'test-volume'}}
            ]
            assert self.expected_pod == actual_pod

    def test_run_as_user_root(self):
        security_context = {
            'securityContext': {
                'runAsUser': 0,
            }
        }
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        assert self.expected_pod == actual_pod

    def test_run_as_user_non_root(self):
        security_context = {
            'securityContext': {
                'runAsUser': 1000,
            }
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        assert self.expected_pod == actual_pod

    def test_fs_group(self):
        security_context = {
            'securityContext': {
                'fsGroup': 1000,
            }
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        assert self.expected_pod == actual_pod

    def test_faulty_service_account(self):
        bad_service_account_name = "foobar"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
            service_account_name=bad_service_account_name,
        )
        with pytest.raises(ApiException):
            context = create_context(k)
            k.execute(context)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['serviceAccountName'] = bad_service_account_name
            assert self.expected_pod == actual_pod

    def test_pod_failure(self):
        """
        Tests that the task fails when a pod reports a failure
        """
        bad_internal_command = ["foobar 10 "]
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=bad_internal_command,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        with pytest.raises(AirflowException):
            context = create_context(k)
            k.execute(context)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['args'] = bad_internal_command
            assert self.expected_pod == actual_pod

    def test_xcom_push(self):
        return_value = '{"foo": "bar"\n, "buzz": 2}'
        args = [f'echo \'{return_value}\' > /airflow/xcom/return.json']
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=args,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=True,
        )
        context = create_context(k)
        assert k.execute(context) == json.loads(return_value)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        volume = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME)
        volume_mount = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)
        container = self.api_client.sanitize_for_serialization(PodDefaults.SIDECAR_CONTAINER)
        self.expected_pod['spec']['containers'][0]['args'] = args
        self.expected_pod['spec']['containers'][0]['volumeMounts'].insert(0, volume_mount)  # noqa
        self.expected_pod['spec']['volumes'].insert(0, volume)
        self.expected_pod['spec']['containers'].append(container)
        assert self.expected_pod == actual_pod

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_configmaps(self, mock_client, mock_monitor, mock_start):
        # GIVEN
        configmap = 'test-configmap'
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            configmaps=[configmap],
        )
        # THEN
        mock_monitor.return_value = (State.SUCCESS, None, None)
        context = self.create_context(k)
        k.execute(context)
        assert mock_start.call_args[0][0].spec.containers[0].env_from == [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap))
        ]

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_secrets(self, mock_client, monitor_mock, start_mock):
        # GIVEN
        secret_ref = 'secret_name'
        secrets = [Secret('env', None, secret_ref)]
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            secrets=secrets,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        # THEN
        monitor_mock.return_value = (State.SUCCESS, None, None)
        context = self.create_context(k)
        k.execute(context)
        assert start_mock.call_args[0][0].spec.containers[0].env_from == [
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=secret_ref))
        ]

    def test_env_vars(self):
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars={
                "ENV1": "val1",
                "ENV2": "val2",
            },
            pod_runtime_info_envs=[PodRuntimeInfoEnv("ENV3", "status.podIP")],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )

        context = create_context(k)
        k.execute(context)

        # THEN
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['env'] = [
            {'name': 'ENV1', 'value': 'val1'},
            {'name': 'ENV2', 'value': 'val2'},
            {'name': 'ENV3', 'valueFrom': {'fieldRef': {'fieldPath': 'status.podIP'}}},
        ]
        assert self.expected_pod == actual_pod

    def test_pod_template_file_with_overrides_system(self):
        fixture = sys.path[0] + '/tests/kubernetes/basic_pod.yaml'
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            labels={"foo": "bar", "fizz": "buzz"},
            env_vars={"env_name": "value"},
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {
            'fizz': 'buzz',
            'foo': 'bar',
            'airflow_version': mock.ANY,
            'dag_id': 'dag',
            'execution_date': mock.ANY,
            'kubernetes_pod_operator': 'True',
            'task_id': mock.ANY,
            'try_number': '1',
        }
        assert k.pod.spec.containers[0].env == [k8s.V1EnvVar(name="env_name", value="value")]
        assert result == {"hello": "world"}

    def test_init_container(self):
        # GIVEN
        volume_mounts = [
            k8s.V1VolumeMount(mount_path='/etc/foo', name='test-volume', sub_path=None, read_only=True)
        ]

        init_environments = [
            k8s.V1EnvVar(name='key1', value='value1'),
            k8s.V1EnvVar(name='key2', value='value2'),
        ]

        init_container = k8s.V1Container(
            name="init-container",
            image="ubuntu:16.04",
            env=init_environments,
            volume_mounts=volume_mounts,
            command=["bash", "-cx"],
            args=["echo 10"],
        )

        volume_config = {'persistentVolumeClaim': {'claimName': 'test-volume'}}
        volume = Volume(name='test-volume', configs=volume_config)

        expected_init_container = {
            'name': 'init-container',
            'image': 'ubuntu:16.04',
            'command': ['bash', '-cx'],
            'args': ['echo 10'],
            'env': [{'name': 'key1', 'value': 'value1'}, {'name': 'key2', 'value': 'value2'}],
            'volumeMounts': [{'mountPath': '/etc/foo', 'name': 'test-volume', 'readOnly': True}],
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            volumes=[volume],
            init_containers=[init_container],
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['initContainers'] = [expected_init_container]
        self.expected_pod['spec']['volumes'] = [
            {'name': 'test-volume', 'persistentVolumeClaim': {'claimName': 'test-volume'}}
        ]
        assert self.expected_pod == actual_pod


# pylint: enable=unused-argument
