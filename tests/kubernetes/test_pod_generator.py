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
import os
import sys
import unittest
import uuid
from unittest import mock

import pytest
from dateutil import parser
from kubernetes.client import ApiClient, models as k8s
from parameterized import parameterized

from airflow import __version__
from airflow.exceptions import AirflowConfigException
from airflow.kubernetes.pod_generator import (
    PodDefaults,
    PodGenerator,
    datetime_to_label_safe_datestring,
    extend_object_field,
    merge_objects,
)
from airflow.kubernetes.secret import Secret


class TestPodGenerator(unittest.TestCase):
    def setUp(self):
        self.static_uuid = uuid.UUID('cf4a56d2-8101-4217-b027-2af6216feb48')
        self.deserialize_result = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': 'memory-demo', 'namespace': 'mem-example'},
            'spec': {
                'containers': [
                    {
                        'args': ['--vm', '1', '--vm-bytes', '150M', '--vm-hang', '1'],
                        'command': ['stress'],
                        'image': 'apache/airflow:stress-2020.07.10-1.0.4',
                        'name': 'memory-demo-ctr',
                        'resources': {'limits': {'memory': '200Mi'}, 'requests': {'memory': '100Mi'}},
                    }
                ]
            },
        }

        self.envs = {'ENVIRONMENT': 'prod', 'LOG_LEVEL': 'warning'}
        self.secrets = [
            # This should be a secretRef
            Secret('env', None, 'secret_a'),
            # This should be a single secret mounted in volumeMounts
            Secret('volume', '/etc/foo', 'secret_b'),
            # This should produce a single secret mounted in env
            Secret('env', 'TARGET', 'secret_b', 'source_b'),
        ]

        self.execution_date = parser.parse('2020-08-24 00:00:00.000000')
        self.execution_date_label = datetime_to_label_safe_datestring(self.execution_date)
        self.dag_id = 'dag_id'
        self.task_id = 'task_id'
        self.try_number = 3
        self.labels = {
            'airflow-worker': 'uuid',
            'dag_id': self.dag_id,
            'execution_date': self.execution_date_label,
            'task_id': self.task_id,
            'try_number': str(self.try_number),
            'airflow_version': __version__.replace('+', '-'),
            'kubernetes_executor': 'True',
        }
        self.annotations = {
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            'execution_date': self.execution_date.isoformat(),
            'try_number': str(self.try_number),
        }
        self.metadata = {
            'labels': self.labels,
            'name': 'pod_id-' + self.static_uuid.hex,
            'namespace': 'namespace',
            'annotations': self.annotations,
        }

        self.resources = k8s.V1ResourceRequirements(
            requests={
                "cpu": 1,
                "memory": "1Gi",
                "ephemeral-storage": "2Gi",
            },
            limits={"cpu": 2, "memory": "2Gi", "ephemeral-storage": "4Gi", 'nvidia.com/gpu': 1},
        )

        self.k8s_client = ApiClient()
        self.expected = k8s.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s.V1ObjectMeta(
                namespace="default",
                name='myapp-pod.' + self.static_uuid.hex,
                labels={'app': 'myapp'},
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='base',
                        image='busybox',
                        command=['sh', '-c', 'echo Hello Kubernetes!'],
                        env=[
                            k8s.V1EnvVar(name='ENVIRONMENT', value='prod'),
                            k8s.V1EnvVar(
                                name="LOG_LEVEL",
                                value='warning',
                            ),
                            k8s.V1EnvVar(
                                name='TARGET',
                                value_from=k8s.V1EnvVarSource(
                                    secret_key_ref=k8s.V1SecretKeySelector(name='secret_b', key='source_b')
                                ),
                            ),
                        ],
                        env_from=[
                            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='configmap_a')),
                            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='configmap_b')),
                            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='secret_a')),
                        ],
                        ports=[k8s.V1ContainerPort(name="foo", container_port=1234)],
                        resources=k8s.V1ResourceRequirements(
                            requests={'memory': '100Mi'},
                            limits={
                                'memory': '200Mi',
                            },
                        ),
                    )
                ],
                security_context=k8s.V1PodSecurityContext(
                    fs_group=2000,
                    run_as_user=1000,
                ),
                host_network=True,
                image_pull_secrets=[
                    k8s.V1LocalObjectReference(name="pull_secret_a"),
                    k8s.V1LocalObjectReference(name="pull_secret_b"),
                ],
            ),
        )

    @mock.patch('uuid.uuid4')
    def test_gen_pod_extract_xcom(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'

        pod_generator = PodGenerator(pod_template_file=path, extract_xcom=True)
        result = pod_generator.gen_pod()
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        container_two = {
            'name': 'airflow-xcom-sidecar',
            'image': "alpine",
            'command': ['sh', '-c', PodDefaults.XCOM_CMD],
            'volumeMounts': [{'name': 'xcom', 'mountPath': '/airflow/xcom'}],
            'resources': {'requests': {'cpu': '1m'}},
        }
        self.expected.spec.containers.append(container_two)
        base_container: k8s.V1Container = self.expected.spec.containers[0]
        base_container.volume_mounts = base_container.volume_mounts or []
        base_container.volume_mounts.append(k8s.V1VolumeMount(name="xcom", mount_path="/airflow/xcom"))
        self.expected.spec.containers[0] = base_container
        self.expected.spec.volumes = self.expected.spec.volumes or []
        self.expected.spec.volumes.append(
            k8s.V1Volume(
                name='xcom',
                empty_dir={},
            )
        )
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        expected_dict = self.k8s_client.sanitize_for_serialization(self.expected)

        assert result_dict == expected_dict

    def test_from_obj(self):
        result = PodGenerator.from_obj(
            {
                "pod_override": k8s.V1Pod(
                    api_version="v1",
                    kind="Pod",
                    metadata=k8s.V1ObjectMeta(name="foo", annotations={"test": "annotation"}),
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        mount_path="/foo/", name="example-kubernetes-test-volume"
                                    )
                                ],
                            )
                        ],
                        volumes=[
                            k8s.V1Volume(
                                name="example-kubernetes-test-volume",
                                host_path=k8s.V1HostPathVolumeSource(path="/tmp/"),
                            )
                        ],
                    ),
                )
            }
        )
        result = self.k8s_client.sanitize_for_serialization(result)

        assert {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'foo',
                'annotations': {'test': 'annotation'},
            },
            'spec': {
                'containers': [
                    {
                        'name': 'base',
                        'volumeMounts': [{'mountPath': '/foo/', 'name': 'example-kubernetes-test-volume'}],
                    }
                ],
                'volumes': [{'hostPath': {'path': '/tmp/'}, 'name': 'example-kubernetes-test-volume'}],
            },
        } == result
        result = PodGenerator.from_obj(
            {
                "KubernetesExecutor": {
                    "annotations": {"test": "annotation"},
                    "volumes": [
                        {
                            "name": "example-kubernetes-test-volume",
                            "hostPath": {"path": "/tmp/"},
                        },
                    ],
                    "volume_mounts": [
                        {
                            "mountPath": "/foo/",
                            "name": "example-kubernetes-test-volume",
                        },
                    ],
                }
            }
        )

        result_from_pod = PodGenerator.from_obj(
            {
                "pod_override": k8s.V1Pod(
                    metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}),
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        name="example-kubernetes-test-volume", mount_path="/foo/"
                                    )
                                ],
                            )
                        ],
                        volumes=[k8s.V1Volume(name="example-kubernetes-test-volume", host_path="/tmp/")],
                    ),
                )
            }
        )

        result = self.k8s_client.sanitize_for_serialization(result)
        result_from_pod = self.k8s_client.sanitize_for_serialization(result_from_pod)
        expected_from_pod = {
            'metadata': {'annotations': {'test': 'annotation'}},
            'spec': {
                'containers': [
                    {
                        'name': 'base',
                        'volumeMounts': [{'mountPath': '/foo/', 'name': 'example-kubernetes-test-volume'}],
                    }
                ],
                'volumes': [{'hostPath': '/tmp/', 'name': 'example-kubernetes-test-volume'}],
            },
        }
        assert (
            result_from_pod == expected_from_pod
        ), "There was a discrepancy between KubernetesExecutor and pod_override"

        assert {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'annotations': {'test': 'annotation'},
            },
            'spec': {
                'containers': [
                    {
                        'args': [],
                        'command': [],
                        'env': [],
                        'envFrom': [],
                        'name': 'base',
                        'ports': [],
                        'volumeMounts': [{'mountPath': '/foo/', 'name': 'example-kubernetes-test-volume'}],
                    }
                ],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': [{'hostPath': {'path': '/tmp/'}, 'name': 'example-kubernetes-test-volume'}],
            },
        } == result

    @mock.patch('uuid.uuid4')
    def test_reconcile_pods_empty_mutator_pod(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'

        pod_generator = PodGenerator(pod_template_file=path, extract_xcom=True)
        base_pod = pod_generator.gen_pod()
        mutator_pod = None
        name = 'name1-' + self.static_uuid.hex

        base_pod.metadata.name = name

        result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
        assert base_pod == result

        mutator_pod = k8s.V1Pod()
        result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
        assert base_pod == result

    @mock.patch('uuid.uuid4')
    def test_reconcile_pods(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'

        base_pod = PodGenerator(pod_template_file=path, extract_xcom=False).gen_pod()

        mutator_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="name2",
                labels={"bar": "baz"},
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        image='',
                        name='name',
                        command=['/bin/command2.sh', 'arg2'],
                        volume_mounts=[
                            k8s.V1VolumeMount(mount_path="/foo/", name="example-kubernetes-test-volume2")
                        ],
                    )
                ],
                volumes=[
                    k8s.V1Volume(
                        host_path=k8s.V1HostPathVolumeSource(path="/tmp/"),
                        name="example-kubernetes-test-volume2",
                    )
                ],
            ),
        )

        result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
        expected: k8s.V1Pod = self.expected
        expected.metadata.name = "name2"
        expected.metadata.labels['bar'] = 'baz'
        expected.spec.volumes = expected.spec.volumes or []
        expected.spec.volumes.append(
            k8s.V1Volume(
                host_path=k8s.V1HostPathVolumeSource(path="/tmp/"), name="example-kubernetes-test-volume2"
            )
        )

        base_container: k8s.V1Container = expected.spec.containers[0]
        base_container.command = ['/bin/command2.sh', 'arg2']
        base_container.volume_mounts = [
            k8s.V1VolumeMount(mount_path="/foo/", name="example-kubernetes-test-volume2")
        ]
        base_container.name = "name"
        expected.spec.containers[0] = base_container

        result_dict = self.k8s_client.sanitize_for_serialization(result)
        expected_dict = self.k8s_client.sanitize_for_serialization(expected)

        assert result_dict == expected_dict

    @mock.patch('uuid.uuid4')
    def test_construct_pod(self, mock_uuid):
        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'
        worker_config = PodGenerator.deserialize_model_file(path)
        mock_uuid.return_value = self.static_uuid
        executor_config = k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='', resources=k8s.V1ResourceRequirements(limits={'cpu': '1m', 'memory': '1G'})
                    )
                ]
            )
        )

        result = PodGenerator.construct_pod(
            dag_id=self.dag_id,
            task_id=self.task_id,
            pod_id='pod_id',
            kube_image='airflow_image',
            try_number=self.try_number,
            date=self.execution_date,
            args=['command'],
            pod_override_object=executor_config,
            base_worker_pod=worker_config,
            namespace='test_namespace',
            scheduler_job_id='uuid',
        )
        expected = self.expected
        expected.metadata.labels = self.labels
        expected.metadata.labels['app'] = 'myapp'
        expected.metadata.annotations = self.annotations
        expected.metadata.name = 'pod_id.' + self.static_uuid.hex
        expected.metadata.namespace = 'test_namespace'
        expected.spec.containers[0].args = ['command']
        expected.spec.containers[0].image = 'airflow_image'
        expected.spec.containers[0].resources = {'limits': {'cpu': '1m', 'memory': '1G'}}
        expected.spec.containers[0].env.append(
            k8s.V1EnvVar(
                name="AIRFLOW_IS_K8S_EXECUTOR_POD",
                value='True',
            )
        )
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        expected_dict = self.k8s_client.sanitize_for_serialization(self.expected)

        assert expected_dict == result_dict

    @mock.patch('uuid.uuid4')
    def test_construct_pod_empty_executor_config(self, mock_uuid):
        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'
        worker_config = PodGenerator.deserialize_model_file(path)
        mock_uuid.return_value = self.static_uuid
        executor_config = None

        result = PodGenerator.construct_pod(
            dag_id='dag_id',
            task_id='task_id',
            pod_id='pod_id',
            kube_image='test-image',
            try_number=3,
            date=self.execution_date,
            args=['command'],
            pod_override_object=executor_config,
            base_worker_pod=worker_config,
            namespace='namespace',
            scheduler_job_id='uuid',
        )
        sanitized_result = self.k8s_client.sanitize_for_serialization(result)
        worker_config.spec.containers[0].image = "test-image"
        worker_config.spec.containers[0].args = ["command"]
        worker_config.metadata.annotations = self.annotations
        worker_config.metadata.labels = self.labels
        worker_config.metadata.labels['app'] = 'myapp'
        worker_config.metadata.name = 'pod_id.' + self.static_uuid.hex
        worker_config.metadata.namespace = 'namespace'
        worker_config.spec.containers[0].env.append(
            k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value='True')
        )
        worker_config_result = self.k8s_client.sanitize_for_serialization(worker_config)
        assert worker_config_result == sanitized_result

    @mock.patch('uuid.uuid4')
    def test_ensure_max_label_length(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        path = os.path.join(os.path.dirname(__file__), 'pod_generator_base_with_secrets.yaml')
        worker_config = PodGenerator.deserialize_model_file(path)

        result = PodGenerator.construct_pod(
            dag_id='a' * 512,
            task_id='a' * 512,
            pod_id='a' * 512,
            kube_image='a' * 512,
            try_number=3,
            date=self.execution_date,
            args=['command'],
            namespace='namespace',
            scheduler_job_id='a' * 512,
            pod_override_object=None,
            base_worker_pod=worker_config,
        )

        assert result.metadata.name == 'a' * 63 + '.' + self.static_uuid.hex
        for _, v in result.metadata.labels.items():
            assert len(v) <= 63

    def test_merge_objects_empty(self):
        annotations = {'foo1': 'bar1'}
        base_obj = k8s.V1ObjectMeta(annotations=annotations)
        client_obj = None
        res = merge_objects(base_obj, client_obj)
        assert base_obj == res

        client_obj = k8s.V1ObjectMeta()
        res = merge_objects(base_obj, client_obj)
        assert base_obj == res

        client_obj = k8s.V1ObjectMeta(annotations=annotations)
        base_obj = None
        res = merge_objects(base_obj, client_obj)
        assert client_obj == res

        base_obj = k8s.V1ObjectMeta()
        res = merge_objects(base_obj, client_obj)
        assert client_obj == res

    def test_merge_objects(self):
        base_annotations = {'foo1': 'bar1'}
        base_labels = {'foo1': 'bar1'}
        client_annotations = {'foo2': 'bar2'}
        base_obj = k8s.V1ObjectMeta(annotations=base_annotations, labels=base_labels)
        client_obj = k8s.V1ObjectMeta(annotations=client_annotations)
        res = merge_objects(base_obj, client_obj)
        client_obj.labels = base_labels
        assert client_obj == res

    def test_extend_object_field_empty(self):
        ports = [k8s.V1ContainerPort(container_port=1, name='port')]
        base_obj = k8s.V1Container(name='base_container', ports=ports)
        client_obj = k8s.V1Container(name='client_container')
        res = extend_object_field(base_obj, client_obj, 'ports')
        client_obj.ports = ports
        assert client_obj == res

        base_obj = k8s.V1Container(name='base_container')
        client_obj = k8s.V1Container(name='base_container', ports=ports)
        res = extend_object_field(base_obj, client_obj, 'ports')
        assert client_obj == res

    def test_extend_object_field_not_list(self):
        base_obj = k8s.V1Container(name='base_container', image='image')
        client_obj = k8s.V1Container(name='client_container')
        with pytest.raises(ValueError):
            extend_object_field(base_obj, client_obj, 'image')
        base_obj = k8s.V1Container(name='base_container')
        client_obj = k8s.V1Container(name='client_container', image='image')
        with pytest.raises(ValueError):
            extend_object_field(base_obj, client_obj, 'image')

    def test_extend_object_field(self):
        base_ports = [k8s.V1ContainerPort(container_port=1, name='base_port')]
        base_obj = k8s.V1Container(name='base_container', ports=base_ports)
        client_ports = [k8s.V1ContainerPort(container_port=1, name='client_port')]
        client_obj = k8s.V1Container(name='client_container', ports=client_ports)
        res = extend_object_field(base_obj, client_obj, 'ports')
        client_obj.ports = base_ports + client_ports
        assert client_obj == res

    def test_reconcile_containers_empty(self):
        base_objs = [k8s.V1Container(name='base_container')]
        client_objs = []
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        assert base_objs == res

        client_objs = [k8s.V1Container(name='client_container')]
        base_objs = []
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        assert client_objs == res

        res = PodGenerator.reconcile_containers([], [])
        assert res == []

    def test_reconcile_containers(self):
        base_ports = [k8s.V1ContainerPort(container_port=1, name='base_port')]
        base_objs = [
            k8s.V1Container(name='base_container1', ports=base_ports),
            k8s.V1Container(name='base_container2', image='base_image'),
        ]
        client_ports = [k8s.V1ContainerPort(container_port=2, name='client_port')]
        client_objs = [
            k8s.V1Container(name='client_container1', ports=client_ports),
            k8s.V1Container(name='client_container2', image='client_image'),
        ]
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        client_objs[0].ports = base_ports + client_ports
        assert client_objs == res

        base_ports = [k8s.V1ContainerPort(container_port=1, name='base_port')]
        base_objs = [
            k8s.V1Container(name='base_container1', ports=base_ports),
            k8s.V1Container(name='base_container2', image='base_image'),
        ]
        client_ports = [k8s.V1ContainerPort(container_port=2, name='client_port')]
        client_objs = [
            k8s.V1Container(name='client_container1', ports=client_ports),
            k8s.V1Container(name='client_container2', stdin=True),
        ]
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        client_objs[0].ports = base_ports + client_ports
        client_objs[1].image = 'base_image'
        assert client_objs == res

    def test_reconcile_specs_empty(self):
        base_spec = k8s.V1PodSpec(containers=[])
        client_spec = None
        res = PodGenerator.reconcile_specs(base_spec, client_spec)
        assert base_spec == res

        base_spec = None
        client_spec = k8s.V1PodSpec(containers=[])
        res = PodGenerator.reconcile_specs(base_spec, client_spec)
        assert client_spec == res

    def test_reconcile_specs(self):
        base_objs = [k8s.V1Container(name='base_container1', image='base_image')]
        client_objs = [k8s.V1Container(name='client_container1')]
        base_spec = k8s.V1PodSpec(priority=1, active_deadline_seconds=100, containers=base_objs)
        client_spec = k8s.V1PodSpec(priority=2, hostname='local', containers=client_objs)
        res = PodGenerator.reconcile_specs(base_spec, client_spec)
        client_spec.containers = [k8s.V1Container(name='client_container1', image='base_image')]
        client_spec.active_deadline_seconds = 100
        assert client_spec == res

    def test_deserialize_model_file(self):
        path = sys.path[0] + '/tests/kubernetes/pod.yaml'
        result = PodGenerator.deserialize_model_file(path)
        sanitized_res = self.k8s_client.sanitize_for_serialization(result)
        assert sanitized_res == self.deserialize_result

    @parameterized.expand(
        (
            ("max_label_length", "a" * 63),
            ("max_subdomain_length", "a" * 253),
            (
                "tiny",
                "aaa",
            ),
        )
    )
    def test_pod_name_confirm_to_max_length(self, _, pod_id):
        name = PodGenerator.make_unique_pod_id(pod_id)
        assert len(name) <= 253
        parts = name.split(".")
        if len(pod_id) <= 63:
            assert len(parts[0]) == len(pod_id)
        else:
            assert len(parts[0]) <= 63
        assert len(parts[1]) <= 63

    def test_deserialize_model_string(self):
        fixture = """
apiVersion: v1
kind: Pod
metadata:
  name: memory-demo
  namespace: mem-example
spec:
  containers:
    - name: memory-demo-ctr
      image: apache/airflow:stress-2020.07.10-1.0.4
      resources:
        limits:
          memory: "200Mi"
        requests:
          memory: "100Mi"
      command: ["stress"]
      args: ["--vm", "1", "--vm-bytes", "150M", "--vm-hang", "1"]
        """
        result = PodGenerator.deserialize_model_file(fixture)
        sanitized_res = self.k8s_client.sanitize_for_serialization(result)
        assert sanitized_res == self.deserialize_result

    def test_validate_pod_generator(self):
        with pytest.raises(AirflowConfigException):
            PodGenerator(pod=k8s.V1Pod(), pod_template_file='k')
        with pytest.raises(AirflowConfigException):
            PodGenerator()
        PodGenerator(pod_template_file='tests/kubernetes/pod.yaml')
        PodGenerator(pod=k8s.V1Pod())
