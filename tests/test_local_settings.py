# -*- coding: utf-8 -*-
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
#
import os
import sys
import tempfile
import unittest
from airflow.kubernetes import pod_generator
from tests.compat import MagicMock, Mock, mock, call, patch

from kubernetes.client.api_client import ApiClient


SETTINGS_FILE_POLICY = """
def test_policy(task_instance):
    task_instance.run_as_user = "myself"
"""

SETTINGS_FILE_POLICY_WITH_DUNDER_ALL = """
__all__ = ["test_policy"]

def test_policy(task_instance):
    task_instance.run_as_user = "myself"

def not_policy():
    print("This shouldn't be imported")
"""

SETTINGS_FILE_POD_MUTATION_HOOK = """
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.pod import Port, Resources

def pod_mutation_hook(pod):
    pod.namespace = 'airflow-tests'
    pod.image = 'my_image'
    pod.volumes.append(Volume(name="bar", configs={}))
    pod.ports = [Port(container_port=8080), {"containerPort": 8081}]
    pod.resources = Resources(
                    request_memory="2G",
                    request_cpu="200Mi",
                    limit_gpu="200G"
                )

    secret_volume = {
        "name":  "airflow-secrets-mount",
        "secret": {
          "secretName": "airflow-test-secrets"
        }
    }
    secret_volume_mount = {
      "name": "airflow-secrets-mount",
      "readOnly": True,
      "mountPath": "/opt/airflow/secrets/"
    }

    pod.volumes.append(secret_volume)
    pod.volume_mounts.append(secret_volume_mount)

    pod.labels.update({"test_label": "test_value"})
    pod.envs.update({"TEST_USER": "ADMIN"})

    pod.tolerations += [
        {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
    ]
    pod.affinity.update(
        {"nodeAffinity":
            {"requiredDuringSchedulingIgnoredDuringExecution":
                {"nodeSelectorTerms":
                    [{
                        "matchExpressions": [
                            {"key": "test/dynamic-pods", "operator": "In", "values": ["true"]}
                        ]
                    }]
                }
            }
        }
    )
"""

SETTINGS_FILE_POD_MUTATION_HOOK_V1_POD = """
def pod_mutation_hook(pod):
    from kubernetes.client import models as k8s
    secret_volume = {
        "name":  "airflow-secrets-mount",
        "secret": {
          "secretName": "airflow-test-secrets"
        }
    }
    secret_volume_mount = {
      "name": "airflow-secrets-mount",
      "readOnly": True,
      "mountPath": "/opt/airflow/secrets/"
    }
    base_container = pod.spec.containers[0]
    base_container.image = "test-image"
    base_container.volume_mounts.append(secret_volume_mount)
    base_container.env.extend([{'name': 'TEST_USER', 'value': 'ADMIN'}])
    base_container.ports.extend([{'containerPort': 8080}, k8s.V1ContainerPort(container_port=8081)])

    pod.spec.volumes.append(secret_volume)
    pod.metadata.namespace = 'airflow-tests'

"""


class SettingsContext:
    def __init__(self, content, module_name):
        self.content = content
        self.settings_root = tempfile.mkdtemp()
        filename = "{}.py".format(module_name)
        self.settings_file = os.path.join(self.settings_root, filename)

    def __enter__(self):
        with open(self.settings_file, 'w') as handle:
            handle.writelines(self.content)
        sys.path.append(self.settings_root)
        return self.settings_file

    def __exit__(self, *exc_info):
        sys.path.remove(self.settings_root)


class LocalSettingsTest(unittest.TestCase):
    # Make sure that the configure_logging is not cached
    def setUp(self):
        self.old_modules = dict(sys.modules)
        self.maxDiff = None

    def tearDown(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

    @patch("airflow.settings.import_local_settings")
    @patch("airflow.settings.prepare_syspath")
    def test_initialize_order(self, prepare_syspath, import_local_settings):
        """
        Tests that import_local_settings is called after prepare_classpath
        """
        mock = Mock()
        mock.attach_mock(prepare_syspath, "prepare_syspath")
        mock.attach_mock(import_local_settings, "import_local_settings")

        import airflow.settings
        airflow.settings.initialize()

        mock.assert_has_calls([call.prepare_syspath(), call.import_local_settings()])

    def test_import_with_dunder_all_not_specified(self):
        """
        Tests that if __all__ is specified in airflow_local_settings,
        only module attributes specified within are imported.
        """
        with SettingsContext(SETTINGS_FILE_POLICY_WITH_DUNDER_ALL, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            with self.assertRaises(AttributeError):
                settings.not_policy()

    def test_import_with_dunder_all(self):
        """
        Tests that if __all__ is specified in airflow_local_settings,
        only module attributes specified within are imported.
        """
        with SettingsContext(SETTINGS_FILE_POLICY_WITH_DUNDER_ALL, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            task_instance = MagicMock()
            settings.test_policy(task_instance)

            assert task_instance.run_as_user == "myself"

    @patch("airflow.settings.log.debug")
    def test_import_local_settings_without_syspath(self, log_mock):
        """
        Tests that an ImportError is raised in import_local_settings
        if there is no airflow_local_settings module on the syspath.
        """
        from airflow import settings
        settings.import_local_settings()
        log_mock.assert_called_with("Failed to import airflow_local_settings.", exc_info=True)

    def test_policy_function(self):
        """
        Tests that task instances are mutated by the policy
        function in airflow_local_settings.
        """
        with SettingsContext(SETTINGS_FILE_POLICY, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            task_instance = MagicMock()
            settings.test_policy(task_instance)

            assert task_instance.run_as_user == "myself"

    def test_pod_mutation_hook(self):
        """
        Tests that pods are mutated by the pod_mutation_hook
        function in airflow_local_settings.
        """
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            pod = MagicMock()
            pod.volumes = []
            settings.pod_mutation_hook(pod)

            assert pod.namespace == 'airflow-tests'
            self.assertEqual(pod.volumes[0].name, "bar")

    def test_pod_mutation_to_k8s_pod(self):
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore
            from airflow.kubernetes.pod_launcher import PodLauncher

            self.mock_kube_client = Mock()
            self.pod_launcher = PodLauncher(kube_client=self.mock_kube_client)
            self.api_client = ApiClient()
            pod = pod_generator.PodGenerator(
                image="foo",
                name="bar",
                namespace="baz",
                image_pull_policy="Never",
                cmds=["foo"],
                tolerations=[
                    {'effect': 'NoSchedule',
                     'key': 'static-pods',
                     'operator': 'Equal',
                     'value': 'true'}
                ],
                volume_mounts=[
                    {"name": "foo", "mountPath": "/mnt", "subPath": "/", "readOnly": True}
                ],
                volumes=[{"name": "foo"}]
            ).gen_pod()

            sanitized_pod_pre_mutation = self.api_client.sanitize_for_serialization(pod)
            self.assertEqual(
                sanitized_pod_pre_mutation,
                {'apiVersion': 'v1',
                 'kind': 'Pod',
                 'metadata': {'name': mock.ANY,
                              'namespace': 'baz'},
                 'spec': {'containers': [{'args': [],
                                          'command': ['foo'],
                                          'env': [],
                                          'envFrom': [],
                                          'image': 'foo',
                                          'imagePullPolicy': 'Never',
                                          'name': 'base',
                                          'ports': [],
                                          'volumeMounts': [{'mountPath': '/mnt',
                                                            'name': 'foo',
                                                            'readOnly': True,
                                                            'subPath': '/'}]}],
                          'hostNetwork': False,
                          'imagePullSecrets': [],
                          'tolerations': [{'effect': 'NoSchedule',
                                           'key': 'static-pods',
                                           'operator': 'Equal',
                                           'value': 'true'}],
                          'volumes': [{'name': 'foo'}]}}
            )

            # Apply Pod Mutation Hook
            pod = self.pod_launcher._mutate_pod_backcompat(pod)

            sanitized_pod_post_mutation = self.api_client.sanitize_for_serialization(pod)
            self.assertEqual(
                sanitized_pod_post_mutation,
                {'metadata': {'labels': {'test_label': 'test_value'},
                              'name': mock.ANY,
                              'namespace': 'airflow-tests'},
                 'spec': {'affinity': {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {
                     'nodeSelectorTerms': [{'matchExpressions': [{'key': 'test/dynamic-pods',
                                                                  'operator': 'In',
                                                                  'values': ['true']}]}]}}},
                          'containers': [{'args': [],
                                          'command': [],
                                          'env': [{'name': 'TEST_USER', 'value': 'ADMIN'}],
                                          'image': 'my_image',
                                          'imagePullPolicy': 'Never',
                                          'name': 'base',
                                          'ports': [{'containerPort': 8080},
                                                    {'containerPort': 8081}],
                                          'resources': {'limits': {'cpu': None,
                                                                   'ephemeral-storage': None,
                                                                   'memory': None,
                                                                   'nvidia.com/gpu': '200G'},
                                                        'requests': {'cpu': '200Mi',
                                                                     'ephemeral-storage': None,
                                                                     'memory': '2G'}},
                                          'volumeMounts': [{'mountPath': '/mnt',
                                                            'name': 'foo',
                                                            'readOnly': True,
                                                            'subPath': '/'},
                                                           {'mountPath': '/opt/airflow/secrets/',
                                                            'name': 'airflow-secrets-mount',
                                                            'readOnly': True}]}],
                          'hostNetwork': False,
                          'tolerations': [{'effect': 'NoSchedule',
                                           'key': 'static-pods',
                                           'operator': 'Equal',
                                           'value': 'true'},
                                          {'effect': 'NoSchedule',
                                           'key': 'dynamic-pods',
                                           'operator': 'Equal',
                                           'value': 'true'}],
                          'volumes': [{'name': 'foo'},
                                      {'name': 'bar'},
                                      {'name': 'airflow-secrets-mount',
                                       'secret': {'secretName': 'airflow-test-secrets'}}]}}
            )

    def test_pod_mutation_v1_pod(self):
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK_V1_POD, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore
            from airflow.kubernetes.pod_launcher import PodLauncher

            self.mock_kube_client = Mock()
            self.api_client = ApiClient()
            self.pod_launcher = PodLauncher(kube_client=self.mock_kube_client)
            pod = pod_generator.PodGenerator(
                image="myimage",
                cmds=["foo"],
                namespace="baz",
                volume_mounts=[
                    {"name": "foo", "mountPath": "/mnt", "subPath": "/", "readOnly": True}
                ],
                volumes=[{"name": "foo"}]
            ).gen_pod()

            sanitized_pod_pre_mutation = self.api_client.sanitize_for_serialization(pod)

            self.assertEqual(
                sanitized_pod_pre_mutation,
                {'apiVersion': 'v1',
                 'kind': 'Pod',
                 'metadata': {'namespace': 'baz'},
                 'spec': {'containers': [{'args': [],
                                          'command': ['foo'],
                                          'env': [],
                                          'envFrom': [],
                                          'image': 'myimage',
                                          'name': 'base',
                                          'ports': [],
                                          'volumeMounts': [{'mountPath': '/mnt',
                                                            'name': 'foo',
                                                            'readOnly': True,
                                                            'subPath': '/'}]}],
                          'hostNetwork': False,
                          'imagePullSecrets': [],
                          'volumes': [{'name': 'foo'}]}}
            )

            # Apply Pod Mutation Hook
            pod = self.pod_launcher._mutate_pod_backcompat(pod)

            sanitized_pod_post_mutation = self.api_client.sanitize_for_serialization(pod)
            self.assertEqual(
                sanitized_pod_post_mutation,
                {'apiVersion': 'v1',
                 'kind': 'Pod',
                 'metadata': {'namespace': 'airflow-tests'},
                 'spec': {'containers': [{'args': [],
                                          'command': ['foo'],
                                          'env': [{'name': 'TEST_USER', 'value': 'ADMIN'}],
                                          'envFrom': [],
                                          'image': 'test-image',
                                          'name': 'base',
                                          'ports': [{'containerPort': 8080}, {'containerPort': 8081}],
                                          'volumeMounts': [{'mountPath': '/mnt',
                                                            'name': 'foo',
                                                            'readOnly': True,
                                                            'subPath': '/'},
                                                           {'mountPath': '/opt/airflow/secrets/',
                                                            'name': 'airflow-secrets-mount',
                                                            'readOnly': True}]}],
                          'hostNetwork': False,
                          'imagePullSecrets': [],
                          'volumes': [{'name': 'foo'},
                                      {'name': 'airflow-secrets-mount',
                                       'secret': {'secretName': 'airflow-test-secrets'}}]}}
            )


class TestStatsWithAllowList(unittest.TestCase):

    def setUp(self):
        from airflow.settings import SafeStatsdLogger, AllowListValidator
        self.statsd_client = Mock()
        self.stats = SafeStatsdLogger(self.statsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.stats.incr('stats_one')
        self.statsd_client.incr.assert_called_once_with('stats_one', 1, 1)

    def test_increment_counter_with_allowed_prefix(self):
        self.stats.incr('stats_two.bla')
        self.statsd_client.incr.assert_called_once_with('stats_two.bla', 1, 1)

    def test_not_increment_counter_if_not_allowed(self):
        self.stats.incr('stats_three')
        self.statsd_client.assert_not_called()
