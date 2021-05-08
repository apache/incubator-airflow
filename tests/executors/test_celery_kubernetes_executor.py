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
from unittest import mock

from parameterized import parameterized

from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.celery_kubernetes_executor import CeleryKubernetesExecutor
from airflow.executors.kubernetes_executor import KubernetesExecutor

KUBERNETES_QUEUE = CeleryKubernetesExecutor.KUBERNETES_QUEUE


class TestCeleryKubernetesExecutor:
    def test_queued_tasks(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        celery_queued_tasks = {('dag_id', 'task_id', '2020-08-30', 1): 'queued_command'}
        k8s_queued_tasks = {('dag_id_2', 'task_id_2', '2020-08-30', 2): 'queued_command'}

        celery_executor_mock.queued_tasks = celery_queued_tasks
        k8s_executor_mock.queued_tasks = k8s_queued_tasks

        expected_queued_tasks = {**celery_queued_tasks, **k8s_queued_tasks}
        assert cke.queued_tasks == expected_queued_tasks

    def test_running(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        celery_running_tasks = {('dag_id', 'task_id', '2020-08-30', 1)}
        k8s_running_tasks = {('dag_id_2', 'task_id_2', '2020-08-30', 2)}

        celery_executor_mock.running = celery_running_tasks
        k8s_executor_mock.running = k8s_running_tasks

        assert cke.running == celery_running_tasks.union(k8s_running_tasks)

    def test_start(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        cke.start()

        celery_executor_mock.start.assert_called()
        k8s_executor_mock.start.assert_called()

    @parameterized.expand(
        [
            ('any-other-queue',),
            (KUBERNETES_QUEUE,),
        ]
    )
    @mock.patch.object(CeleryExecutor, 'queue_command')
    @mock.patch.object(KubernetesExecutor, 'queue_command')
    def test_queue_command(self, test_queue, k8s_queue_cmd, celery_queue_cmd):
        kwargs = dict(
            command=['airflow', 'run', 'dag'],
            priority=1,
            queue='default',
        )
        kwarg_values = kwargs.values()
        cke = CeleryKubernetesExecutor(CeleryExecutor(), KubernetesExecutor())

        simple_task_instance = mock.MagicMock()
        simple_task_instance.queue = test_queue

        cke.queue_command(simple_task_instance, **kwargs)

        if test_queue == KUBERNETES_QUEUE:
            k8s_queue_cmd.assert_called_once_with(simple_task_instance, *kwarg_values)
            celery_queue_cmd.assert_not_called()
        else:
            celery_queue_cmd.assert_called_once_with(simple_task_instance, *kwarg_values)
            k8s_queue_cmd.assert_not_called()

    @parameterized.expand(
        [
            ('any-other-queue',),
            (KUBERNETES_QUEUE,),
        ]
    )
    def test_queue_task_instance(self, test_queue):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        ti = mock.MagicMock()
        ti.queue = test_queue

        kwargs = dict(
            task_instance=ti,
            mark_success=False,
            pickle_id=None,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pool=None,
            cfg_path=None,
        )
        kwarg_values = kwargs.values()
        cke.queue_task_instance(**kwargs)
        if test_queue == KUBERNETES_QUEUE:
            k8s_executor_mock.queue_task_instance.assert_called_once_with(*kwarg_values)
            celery_executor_mock.queue_task_instance.assert_not_called()
        else:
            celery_executor_mock.queue_task_instance.assert_called_once_with(*kwarg_values)
            k8s_executor_mock.queue_task_instance.assert_not_called()

    @parameterized.expand(
        [
            (True, True, True),
            (False, True, True),
            (True, False, True),
            (False, False, False),
        ]
    )
    def test_has_tasks(self, celery_has, k8s_has, cke_has):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        celery_executor_mock.has_task.return_value = celery_has
        k8s_executor_mock.has_task.return_value = k8s_has
        ti = mock.MagicMock()
        assert cke.has_task(ti) == cke_has
        celery_executor_mock.has_task.assert_called_once_with(ti)
        if not celery_has:
            k8s_executor_mock.has_task.assert_called_once_with(ti)

    @parameterized.expand([(1, 0), (0, 1), (2, 1)])
    def test_adopt_tasks(self, num_k8s, num_celery):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()

        def mock_ti(queue):
            ti = mock.MagicMock()
            ti.queue = queue
            return ti

        celery_tis = [mock_ti('default') for _ in range(num_celery)]
        k8s_tis = [mock_ti(KUBERNETES_QUEUE) for _ in range(num_k8s)]

        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)
        cke.try_adopt_task_instances(celery_tis + k8s_tis)
        celery_executor_mock.try_adopt_task_instances.assert_called_once_with(celery_tis)
        k8s_executor_mock.try_adopt_task_instances.assert_called_once_with(k8s_tis)

    def test_get_event_buffer(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        dag_ids = ['dag_ids']

        events_in_celery = {('dag_id', 'task_id', '2020-08-30', 1): ('failed', 'failed task')}
        events_in_k8s = {('dag_id_2', 'task_id_2', '2020-08-30', 1): ('success', None)}

        celery_executor_mock.get_event_buffer.return_value = events_in_celery
        k8s_executor_mock.get_event_buffer.return_value = events_in_k8s

        events = cke.get_event_buffer(dag_ids)

        assert events == {**events_in_celery, **events_in_k8s}

        celery_executor_mock.get_event_buffer.assert_called_once_with(dag_ids)
        k8s_executor_mock.get_event_buffer.assert_called_once_with(dag_ids)

    def test_end(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        cke.end()

        celery_executor_mock.end.assert_called_once()
        k8s_executor_mock.end.assert_called_once()

    def test_terminate(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        cke.terminate()

        celery_executor_mock.terminate.assert_called_once()
        k8s_executor_mock.terminate.assert_called_once()

    def test_job_id_setter(self):
        cel_exec = CeleryExecutor()
        k8s_exec = KubernetesExecutor()
        cel_k8s_exec = CeleryKubernetesExecutor(cel_exec, k8s_exec)
        job_id = 'this-job-id'
        cel_k8s_exec.job_id = job_id
        assert cel_exec.job_id == k8s_exec.job_id == cel_k8s_exec.job_id == job_id
