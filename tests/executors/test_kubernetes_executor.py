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
import random
import re
import string
import unittest
from datetime import datetime
from unittest import mock

from kubernetes.client import models as k8s
from urllib3 import HTTPResponse

from airflow.utils import timezone
from tests.test_utils.config import conf_vars

try:
    from kubernetes.client.rest import ApiException

    from airflow.executors.kubernetes_executor import (
        AirflowKubernetesScheduler,
        KubernetesExecutor,
        KubernetesJobWatcher,
        create_pod_id,
        get_base_pod_from_template,
    )
    from airflow.kubernetes import pod_generator
    from airflow.kubernetes.pod_generator import PodGenerator
    from airflow.utils.state import State
except ImportError:
    AirflowKubernetesScheduler = None  # type: ignore


# pylint: disable=unused-argument
class TestAirflowKubernetesScheduler(unittest.TestCase):
    @staticmethod
    def _gen_random_string(seed, str_len):
        char_list = []
        for char_seed in range(str_len):
            random.seed(str(seed) * char_seed)
            char_list.append(random.choice(string.printable))
        return ''.join(char_list)

    def _cases(self):
        cases = [
            ("my_dag_id", "my-task-id"),
            ("my.dag.id", "my.task.id"),
            ("MYDAGID", "MYTASKID"),
            ("my_dag_id", "my_task_id"),
            ("mydagid" * 200, "my_task_id" * 200),
        ]

        cases.extend(
            [(self._gen_random_string(seed, 200), self._gen_random_string(seed, 200)) for seed in range(100)]
        )

        return cases

    @staticmethod
    def _is_valid_pod_id(name):
        regex = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        return len(name) <= 253 and all(ch.lower() == ch for ch in name) and re.match(regex, name)

    @staticmethod
    def _is_safe_label_value(value):
        regex = r'^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$'
        return len(value) <= 63 and re.match(regex, value)

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    def test_create_pod_id(self):
        for dag_id, task_id in self._cases():
            pod_name = PodGenerator.make_unique_pod_id(create_pod_id(dag_id, task_id))
            assert self._is_valid_pod_id(pod_name)

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    @mock.patch("airflow.kubernetes.pod_generator.PodGenerator")
    @mock.patch("airflow.executors.kubernetes_executor.KubeConfig")
    def test_get_base_pod_from_template(self, mock_kubeconfig, mock_generator):
        pod_template_file_path = "/bar/biz"
        get_base_pod_from_template(pod_template_file_path, None)
        assert "deserialize_model_dict" == mock_generator.mock_calls[0][0]
        assert pod_template_file_path == mock_generator.mock_calls[0][1][0]
        mock_kubeconfig.pod_template_file = "/foo/bar"
        get_base_pod_from_template(None, mock_kubeconfig)
        assert "deserialize_model_dict" == mock_generator.mock_calls[1][0]
        assert "/foo/bar" == mock_generator.mock_calls[1][1][0]

    def test_make_safe_label_value(self):
        for dag_id, task_id in self._cases():
            safe_dag_id = pod_generator.make_safe_label_value(dag_id)
            assert self._is_safe_label_value(safe_dag_id)
            safe_task_id = pod_generator.make_safe_label_value(task_id)
            assert self._is_safe_label_value(safe_task_id)
            dag_id = "my_dag_id"
            assert dag_id == pod_generator.make_safe_label_value(dag_id)
            dag_id = "my_dag_id_" + "a" * 64
            assert "my_dag_id_" + "a" * 43 + "-0ce114c45" == pod_generator.make_safe_label_value(dag_id)

    def test_execution_date_serialize_deserialize(self):
        datetime_obj = datetime.now()
        serialized_datetime = pod_generator.datetime_to_label_safe_datestring(datetime_obj)
        new_datetime_obj = pod_generator.label_safe_datestring_to_datetime(serialized_datetime)

        assert datetime_obj == new_datetime_obj


class TestKubernetesExecutor(unittest.TestCase):
    """
    Tests if an ApiException from the Kube Client will cause the task to
    be rescheduled.
    """

    def setUp(self) -> None:
        self.kubernetes_executor = KubernetesExecutor()
        self.kubernetes_executor.job_id = "5"

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_run_next_exception(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        import sys

        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'

        # When a quota is exceeded this is the ApiException we get
        response = HTTPResponse(
            body='{"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure", '
            '"message": "pods \\"podname\\" is forbidden: exceeded quota: compute-resources, '
            'requested: limits.memory=4Gi, used: limits.memory=6508Mi, limited: limits.memory=10Gi", '
            '"reason": "Forbidden", "details": {"name": "podname", "kind": "pods"}, "code": 403}'
        )
        response.status = 403
        response.reason = "Forbidden"

        # A mock kube_client that throws errors when making a pod
        mock_kube_client = mock.patch('kubernetes.client.CoreV1Api', autospec=True)
        mock_kube_client.create_namespaced_pod = mock.MagicMock(side_effect=ApiException(http_resp=response))
        mock_get_kube_client.return_value = mock_kube_client
        mock_api_client = mock.MagicMock()
        mock_api_client.sanitize_for_serialization.return_value = {}
        mock_kube_client.api_client = mock_api_client
        config = {
            ('kubernetes', 'pod_template_file'): path,
        }
        with conf_vars(config):

            kubernetes_executor = self.kubernetes_executor
            kubernetes_executor.start()
            # Execute a task while the Api Throws errors
            try_number = 1
            kubernetes_executor.execute_async(
                key=('dag', 'task', datetime.utcnow(), try_number),
                queue=None,
                command=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
            )
            kubernetes_executor.sync()
            kubernetes_executor.sync()

            assert mock_kube_client.create_namespaced_pod.called
            assert not kubernetes_executor.task_queue.empty()

            # Disable the ApiException
            mock_kube_client.create_namespaced_pod.side_effect = None

            # Execute the task without errors should empty the queue
            kubernetes_executor.sync()
            assert mock_kube_client.create_namespaced_pod.called
            assert kubernetes_executor.task_queue.empty()

    @mock.patch('airflow.executors.kubernetes_executor.KubeConfig')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync, mock_kube_config):
        executor = self.kubernetes_executor
        executor.heartbeat()
        calls = [
            mock.call('executor.open_slots', mock.ANY),
            mock.call('executor.queued_tasks', mock.ANY),
            mock.call('executor.running_tasks', mock.ANY),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_invalid_executor_config(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()

        assert executor.event_buffer == {}
        executor.execute_async(
            key=('dag', 'task', datetime.utcnow(), 1),
            queue=None,
            command=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
            executor_config=k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[k8s.V1Container(name="base", image="myimage", image_pull_policy="Always")]
                )
            ),
        )

        assert list(executor.event_buffer.values())[0][1] == "Invalid executor_config passed"

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_change_state_running(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        key = ('dag_id', 'task_id', 'ex_time', 'try_number1')
        executor._change_state(key, State.RUNNING, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.RUNNING

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_success(self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        test_time = timezone.utcnow()
        key = ('dag_id', 'task_id', test_time, 'try_number2')
        executor._change_state(key, State.SUCCESS, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.SUCCESS
        mock_delete_pod.assert_called_once_with('pod_id', 'default')

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_failed_no_deletion(
        self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False
        executor.start()
        test_time = timezone.utcnow()
        key = ('dag_id', 'task_id', test_time, 'try_number3')
        executor._change_state(key, State.FAILED, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.FAILED
        mock_delete_pod.assert_not_called()

    # pylint: enable=unused-argument

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_skip_pod_deletion(
        self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        test_time = timezone.utcnow()
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False

        executor.start()
        key = ('dag_id', 'task_id', test_time, 'try_number2')
        executor._change_state(key, State.SUCCESS, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.SUCCESS
        mock_delete_pod.assert_not_called()

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_failed_pod_deletion(
        self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods_on_failure = True

        executor.start()
        key = ('dag_id', 'task_id', 'ex_time', 'try_number2')
        executor._change_state(key, State.FAILED, 'pod_id', 'test-namespace')
        assert executor.event_buffer[key][0] == State.FAILED
        mock_delete_pod.assert_called_once_with('pod_id', 'test-namespace')

    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_adopt_launched_task(self, mock_kube_client):
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        pod_ids = {"dagtask": {}}
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo", labels={"airflow-worker": "bar", "dag_id": "dag", "task_id": "task"}
            )
        )
        executor.adopt_launched_task(mock_kube_client, pod=pod, pod_ids=pod_ids)
        assert mock_kube_client.patch_namespaced_pod.call_args[1] == {
            'body': {
                'metadata': {
                    'labels': {'airflow-worker': 'modified', 'dag_id': 'dag', 'task_id': 'task'},
                    'name': 'foo',
                }
            },
            'name': 'foo',
            'namespace': None,
        }
        assert pod_ids == {}

    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_not_adopt_unassigned_task(self, mock_kube_client):
        """
        We should not adopt any tasks that were not assigned by the scheduler.
        This ensures that there is no contention over pod management.
        """

        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        pod_ids = {"foobar": {}}
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo", labels={"airflow-worker": "bar", "dag_id": "dag", "task_id": "task"}
            )
        )
        executor.adopt_launched_task(mock_kube_client, pod=pod, pod_ids=pod_ids)
        assert not mock_kube_client.patch_namespaced_pod.called
        assert pod_ids == {"foobar": {}}


class TestKubernetesJobWatcher(unittest.TestCase):
    def setUp(self):
        self.watcher = KubernetesJobWatcher(
            namespace="airflow",
            multi_namespace_mode=False,
            watcher_queue=mock.MagicMock(),
            resource_version="0",
            scheduler_job_id="123",
            kube_config=mock.MagicMock(),
        )
        self.kube_client = mock.MagicMock()
        self.core_annotations = {
            "dag_id": "dag",
            "task_id": "task",
            "execution_date": "dt",
            "try_number": "1",
        }
        self.pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo",
                annotations={"airflow-worker": "bar", **self.core_annotations},
                namespace="airflow",
                resource_version="456",
            ),
            status=k8s.V1PodStatus(phase="Pending"),
        )
        self.events = []

    def _run(self):
        with mock.patch('airflow.executors.kubernetes_executor.watch') as mock_watch:
            mock_watch.Watch.return_value.stream.return_value = self.events
            latest_resource_version = self.watcher._run(
                self.kube_client,
                self.watcher.resource_version,
                self.watcher.scheduler_job_id,
                self.watcher.kube_config,
            )
            assert self.pod.metadata.resource_version == latest_resource_version

    def assert_watcher_queue_called_once_with_state(self, state):
        self.watcher.watcher_queue.put.assert_called_once_with(
            (
                self.pod.metadata.name,
                self.watcher.namespace,
                state,
                self.core_annotations,
                self.pod.metadata.resource_version,
            )
        )

    def test_process_status_pending(self):
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    def test_process_status_pending_deleted(self):
        self.events.append({"type": 'DELETED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_failed(self):
        self.pod.status.phase = "Failed"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_succeeded(self):
        self.pod.status.phase = "Succeeded"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(None)

    def test_process_status_running(self):
        self.pod.status.phase = "Running"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    def test_process_status_catchall(self):
        self.pod.status.phase = "Unknown"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()
