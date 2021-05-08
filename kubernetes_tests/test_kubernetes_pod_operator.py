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
import logging
import os
import random
import shutil
import sys
import textwrap
import unittest
from unittest import mock
from unittest.mock import ANY

import pendulum
import pytest
from kubernetes.client import models as k8s
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.secret import Secret
from airflow.models import DAG, TaskInstance
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils import timezone
from airflow.version import version as airflow_version


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


def get_kubeconfig_path():
    kubeconfig_path = os.environ.get('KUBECONFIG')
    return kubeconfig_path if kubeconfig_path else os.path.expanduser('~/.kube/config')


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
                'name': ANY,
                'annotations': {},
                'labels': {
                    'foo': 'bar',
                    'kubernetes_pod_operator': 'True',
                    'airflow_version': airflow_version.replace('+', '-'),
                    'execution_date': '2016-01-01T0100000100-a2f50a31f',
                    'dag_id': 'dag',
                    'task_id': ANY,
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
                'restartPolicy': 'Never',
                'securityContext': {},
                'serviceAccountName': 'default',
                'tolerations': [],
                'volumes': [],
            },
        }

    def tearDown(self) -> None:
        client = kube_client.get_kube_client(in_cluster=False)
        client.delete_collection_namespaced_pod(namespace="default")
        import time

        time.sleep(1)

    def test_do_xcom_push_defaults_false(self):
        new_config_path = '/tmp/kube_config'
        old_config_path = get_kubeconfig_path()
        shutil.copy(old_config_path, new_config_path)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            config_file=new_config_path,
        )
        assert not k.do_xcom_push

    def test_config_path_move(self):
        new_config_path = '/tmp/kube_config'
        old_config_path = get_kubeconfig_path()
        shutil.copy(old_config_path, new_config_path)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test1",
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            config_file=new_config_path,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod == actual_pod

    def test_working_pod(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod['spec'] == actual_pod['spec']
        assert self.expected_pod['metadata']['labels'] == actual_pod['metadata']['labels']

    def test_delete_operator_pod(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=True,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod['spec'] == actual_pod['spec']
        assert self.expected_pod['metadata']['labels'] == actual_pod['metadata']['labels']

    def test_pod_hostnetwork(self):
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            hostnetwork=True,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['hostNetwork'] = True
        assert self.expected_pod['spec'] == actual_pod['spec']
        assert self.expected_pod['metadata']['labels'] == actual_pod['metadata']['labels']

    def test_pod_dnspolicy(self):
        dns_policy = "ClusterFirstWithHostNet"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            hostnetwork=True,
            dnspolicy=dns_policy,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['hostNetwork'] = True
        self.expected_pod['spec']['dnsPolicy'] = dns_policy
        assert self.expected_pod['spec'] == actual_pod['spec']
        assert self.expected_pod['metadata']['labels'] == actual_pod['metadata']['labels']

    def test_pod_schedulername(self):
        scheduler_name = "default-scheduler"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            schedulername=scheduler_name,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['schedulerName'] = scheduler_name
        assert self.expected_pod == actual_pod

    def test_pod_node_selectors(self):
        node_selectors = {'beta.kubernetes.io/os': 'linux'}
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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
        resources = k8s.V1ResourceRequirements(
            requests={'memory': '64Mi', 'cpu': '250m', 'ephemeral-storage': '1Gi'},
            limits={'memory': '64Mi', 'cpu': 0.25, 'nvidia.com/gpu': None, 'ephemeral-storage': '2Gi'},
        )
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            resources=resources,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['resources'] = {
            'requests': {'memory': '64Mi', 'cpu': '250m', 'ephemeral-storage': '1Gi'},
            'limits': {'memory': '64Mi', 'cpu': 0.25, 'nvidia.com/gpu': None, 'ephemeral-storage': '2Gi'},
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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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
        port = k8s.V1ContainerPort(
            name='http',
            container_port=80,
        )

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            ports=[port],
        )
        context = create_context(k)
        k.execute(context=context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['containers'][0]['ports'] = [{'name': 'http', 'containerPort': 80}]
        assert self.expected_pod == actual_pod

    def test_volume_mount(self):
        with mock.patch.object(PodLauncher, 'log') as mock_logger:
            volume_mount = k8s.V1VolumeMount(
                name='test-volume', mount_path='/tmp/test_volume', sub_path=None, read_only=False
            )

            volume = k8s.V1Volume(
                name='test-volume',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume'),
            )

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
                name="test-" + str(random.randint(0, 1000000)),
                task_id="task" + self.get_current_task_name(),
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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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
            name="test-fs-group",
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['securityContext'] = security_context
        assert self.expected_pod == actual_pod

    def test_faulty_image(self):
        bad_image_name = "foobar"
        k = KubernetesPodOperator(
            namespace='default',
            image=bad_image_name,
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
        )
        with pytest.raises(AirflowException):
            context = create_context(k)
            k.execute(context)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod['spec']['containers'][0]['image'] = bad_image_name
            assert self.expected_pod == actual_pod

    def test_faulty_service_account(self):
        bad_service_account_name = "foobar"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_configmaps(self, mock_client, mock_monitor, mock_start):
        # GIVEN
        from airflow.utils.state import State

        configmap_name = "test-config-map"
        env_from = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name))]
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            env_from=env_from,
        )
        # THEN
        mock_monitor.return_value = (State.SUCCESS, None)
        context = create_context(k)
        k.execute(context)
        assert mock_start.call_args[0][0].spec.containers[0].env_from == env_from

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_envs_from_secrets(self, mock_client, monitor_mock, start_mock):
        # GIVEN
        from airflow.utils.state import State

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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
        )
        # THEN
        monitor_mock.return_value = (State.SUCCESS, None)
        context = create_context(k)
        k.execute(context)
        assert start_mock.call_args[0][0].spec.containers[0].env_from == [
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=secret_ref))
        ]

    def test_env_vars(self):
        # WHEN
        env_vars = [
            k8s.V1EnvVar(name="ENV1", value="val1"),
            k8s.V1EnvVar(name="ENV2", value="val2"),
            k8s.V1EnvVar(
                name="ENV3",
                value_from=k8s.V1EnvVarSource(field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")),
            ),
        ]

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars=env_vars,
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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

    def test_pod_template_file_system(self):
        fixture = sys.path[0] + '/tests/kubernetes/basic_pod.yaml'
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert result == {"hello": "world"}

    def test_pod_template_file_with_overrides_system(self):
        fixture = sys.path[0] + '/tests/kubernetes/basic_pod.yaml'
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            labels={"foo": "bar", "fizz": "buzz"},
            env_vars=[k8s.V1EnvVar(name="env_name", value="value")],
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {'fizz': 'buzz', 'foo': 'bar'}
        assert k.pod.spec.containers[0].env == [k8s.V1EnvVar(name="env_name", value="value")]
        assert result == {"hello": "world"}

    def test_pod_template_file_with_full_pod_spec(self):
        fixture = sys.path[0] + '/tests/kubernetes/basic_pod.yaml'
        pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={"foo": "bar", "fizz": "buzz"},
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=[k8s.V1EnvVar(name="env_name", value="value")],
                    )
                ]
            ),
        )
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            pod_template_file=fixture,
            full_pod_spec=pod_spec,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {'fizz': 'buzz', 'foo': 'bar'}
        assert k.pod.spec.containers[0].env == [k8s.V1EnvVar(name="env_name", value="value")]
        assert result == {"hello": "world"}

    def test_full_pod_spec(self):
        pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={"foo": "bar", "fizz": "buzz"}, namespace="default", name="test-pod"
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="perl",
                        command=["/bin/bash"],
                        args=["-c", 'echo {\\"hello\\" : \\"world\\"} | cat > /airflow/xcom/return.json'],
                        env=[k8s.V1EnvVar(name="env_name", value="value")],
                    )
                ],
                restart_policy="Never",
            ),
        )
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            full_pod_spec=pod_spec,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {'fizz': 'buzz', 'foo': 'bar'}
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

        volume = k8s.V1Volume(
            name='test-volume',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume'),
        )
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
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
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

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_template_file(
        self, mock_client, monitor_mock, start_mock  # pylint: disable=unused-argument
    ):
        from airflow.utils.state import State

        path = sys.path[0] + '/tests/kubernetes/pod.yaml'
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(), pod_template_file=path, do_xcom_push=True
        )

        monitor_mock.return_value = (State.SUCCESS, None)
        context = create_context(k)
        with self.assertLogs(k.log, level=logging.DEBUG) as cm:
            k.execute(context)
            expected_line = textwrap.dedent(
                """\
            DEBUG:airflow.task.operators:Starting pod:
            api_version: v1
            kind: Pod
            metadata:
              annotations: {}
              cluster_name: null
              creation_timestamp: null
              deletion_grace_period_seconds: null\
            """
            ).strip()
            assert any(line.startswith(expected_line) for line in cm.output)

        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        expected_dict = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'annotations': {}, 'labels': {}, 'name': 'memory-demo', 'namespace': 'mem-example'},
            'spec': {
                'affinity': {},
                'containers': [
                    {
                        'args': ['--vm', '1', '--vm-bytes', '150M', '--vm-hang', '1'],
                        'command': ['stress'],
                        'env': [],
                        'envFrom': [],
                        'image': 'apache/airflow:stress-2021.04.28-1.0.4',
                        'imagePullPolicy': 'IfNotPresent',
                        'name': 'base',
                        'ports': [],
                        'resources': {'limits': {'memory': '200Mi'}, 'requests': {'memory': '100Mi'}},
                        'volumeMounts': [{'mountPath': '/airflow/xcom', 'name': 'xcom'}],
                    },
                    {
                        'command': ['sh', '-c', 'trap "exit 0" INT; while true; do sleep 30; done;'],
                        'image': 'alpine',
                        'name': 'airflow-xcom-sidecar',
                        'resources': {'requests': {'cpu': '1m'}},
                        'volumeMounts': [{'mountPath': '/airflow/xcom', 'name': 'xcom'}],
                    },
                ],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'initContainers': [],
                'restartPolicy': 'Never',
                'securityContext': {},
                'serviceAccountName': 'default',
                'tolerations': [],
                'volumes': [{'emptyDir': {}, 'name': 'xcom'}],
            },
        }
        assert expected_dict == actual_pod

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_priority_class_name(
        self, mock_client, monitor_mock, start_mock  # pylint: disable=unused-argument
    ):
        """Test ability to assign priorityClassName to pod"""
        from airflow.utils.state import State

        priority_class_name = "medium-test"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test-" + str(random.randint(0, 1000000)),
            task_id="task" + self.get_current_task_name(),
            in_cluster=False,
            do_xcom_push=False,
            priority_class_name=priority_class_name,
        )

        monitor_mock.return_value = (State.SUCCESS, None)
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod['spec']['priorityClassName'] = priority_class_name
        assert self.expected_pod == actual_pod

    def test_pod_name(self):
        pod_name_too_long = "a" * 221
        with pytest.raises(AirflowException):
            KubernetesPodOperator(
                namespace='default',
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["echo 10"],
                labels={"foo": "bar"},
                name=pod_name_too_long,
                task_id="task" + self.get_current_task_name(),
                in_cluster=False,
                do_xcom_push=False,
            )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    def test_on_kill(self, monitor_mock):  # pylint: disable=unused-argument
        from airflow.utils.state import State

        client = kube_client.get_kube_client(in_cluster=False)
        name = "test"
        namespace = "default"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["sleep 1000"],
            labels={"foo": "bar"},
            name="test",
            task_id=name,
            in_cluster=False,
            do_xcom_push=False,
            termination_grace_period=0,
        )
        context = create_context(k)
        monitor_mock.return_value = (State.SUCCESS, None)
        k.execute(context)
        name = k.pod.metadata.name
        pod = client.read_namespaced_pod(name=name, namespace=namespace)
        assert pod.status.phase == "Running"
        k.on_kill()
        with pytest.raises(ApiException):
            pod = client.read_namespaced_pod(name=name, namespace=namespace)

    def test_reattach_failing_pod_once(self):
        from airflow.utils.state import State

        client = kube_client.get_kube_client(in_cluster=False)
        name = "test"
        namespace = "default"
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["exit 1"],
            labels={"foo": "bar"},
            name="test",
            task_id=name,
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=False,
            termination_grace_period=0,
        )

        context = create_context(k)

        with mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod") as monitor_mock:
            monitor_mock.return_value = (State.SUCCESS, None)
            k.execute(context)
            name = k.pod.metadata.name
            pod = client.read_namespaced_pod(name=name, namespace=namespace)
            while pod.status.phase != "Failed":
                pod = client.read_namespaced_pod(name=name, namespace=namespace)
        with pytest.raises(AirflowException):
            k.execute(context)
        pod = client.read_namespaced_pod(name=name, namespace=namespace)
        assert pod.metadata.labels["already_checked"] == "True"
        with mock.patch(
            "airflow.providers.cncf.kubernetes"
            ".operators.kubernetes_pod.KubernetesPodOperator"
            ".create_new_pod_for_operator"
        ) as create_mock:
            create_mock.return_value = ("success", {}, {})
            k.execute(context)
            create_mock.assert_called_once()


# pylint: enable=unused-argument
