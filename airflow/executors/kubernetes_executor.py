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
"""
KubernetesExecutor

.. seealso::
    For more information on how the KubernetesExecutor works, take a look at the guide:
    :ref:`executor:KubernetesExecutor`
"""

import functools
import json
import multiprocessing
import time
from queue import Empty, Queue  # pylint: disable=unused-import
from typing import Any, Dict, List, Optional, Tuple

import kubernetes
from dateutil import parser
from kubernetes import client, watch
from kubernetes.client import Configuration, models as k8s
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError

from airflow.exceptions import AirflowException
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, BaseExecutor, CommandType
from airflow.kubernetes import pod_generator
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.kube_config import KubeConfig
from airflow.kubernetes.kubernetes_helper_functions import create_pod_id
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.models import TaskInstance
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State

# TaskInstance key, command, configuration, pod_template_file
KubernetesJobType = Tuple[TaskInstanceKey, CommandType, Any, Optional[str]]

# key, state, pod_id, namespace, resource_version
KubernetesResultsType = Tuple[TaskInstanceKey, Optional[str], str, str, str]

# pod_id, namespace, state, annotations, resource_version
KubernetesWatchType = Tuple[str, str, Optional[str], Dict[str, str], str]


class ResourceVersion:
    """Singleton for tracking resourceVersion from Kubernetes"""

    _instance = None
    resource_version = "0"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin):
    """Watches for Kubernetes jobs"""

    def __init__(
        self,
        namespace: Optional[str],
        multi_namespace_mode: bool,
        watcher_queue: 'Queue[KubernetesWatchType]',
        resource_version: Optional[str],
        scheduler_job_id: Optional[str],
        kube_config: Configuration,
    ):
        super().__init__()
        self.namespace = namespace
        self.multi_namespace_mode = multi_namespace_mode
        self.scheduler_job_id = scheduler_job_id
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version
        self.kube_config = kube_config

    def run(self) -> None:
        """Performs watching"""
        kube_client: client.CoreV1Api = get_kube_client()
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        while True:
            try:
                self.resource_version = self._run(
                    kube_client, self.resource_version, self.scheduler_job_id, self.kube_config
                )
            except ReadTimeoutError:
                self.log.warning(
                    "There was a timeout error accessing the Kube API. Retrying request.", exc_info=True
                )
                time.sleep(1)
            except Exception:
                self.log.exception('Unknown error in KubernetesJobWatcher. Failing')
                raise
            else:
                self.log.warning(
                    'Watch died gracefully, starting back up with: last resource_version: %s',
                    self.resource_version,
                )

    def _run(
        self,
        kube_client: client.CoreV1Api,
        resource_version: Optional[str],
        scheduler_job_id: str,
        kube_config: Any,
    ) -> Optional[str]:
        self.log.info('Event: and now my watch begins starting at resource_version: %s', resource_version)
        watcher = watch.Watch()

        kwargs = {'label_selector': f'airflow-worker={scheduler_job_id}'}
        if resource_version:
            kwargs['resource_version'] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version: Optional[str] = None
        if self.multi_namespace_mode:
            list_worker_pods = functools.partial(
                watcher.stream, kube_client.list_pod_for_all_namespaces, **kwargs
            )
        else:
            list_worker_pods = functools.partial(
                watcher.stream, kube_client.list_namespaced_pod, self.namespace, **kwargs
            )
        for event in list_worker_pods():
            task = event['object']
            self.log.info('Event: %s had an event of type %s', task.metadata.name, event['type'])
            if event['type'] == 'ERROR':
                return self.process_error(event)
            annotations = task.metadata.annotations
            task_instance_related_annotations = {
                'dag_id': annotations['dag_id'],
                'task_id': annotations['task_id'],
                'execution_date': annotations['execution_date'],
                'try_number': annotations['try_number'],
            }

            self.process_status(
                pod_id=task.metadata.name,
                namespace=task.metadata.namespace,
                status=task.status.phase,
                annotations=task_instance_related_annotations,
                resource_version=task.metadata.resource_version,
                event=event,
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_error(self, event: Any) -> str:
        """Process error response"""
        self.log.error('Encountered Error response from k8s list namespaced pod stream => %s', event)
        raw_object = event['raw_object']
        if raw_object['code'] == 410:
            self.log.info(
                'Kubernetes resource version is too old, must reset to 0 => %s', (raw_object['message'],)
            )
            # Return resource version 0
            return '0'
        raise AirflowException(
            'Kubernetes failure for %s with code %s and message: %s'
            % (raw_object['reason'], raw_object['code'], raw_object['message'])
        )

    def process_status(
        self,
        pod_id: str,
        namespace: str,
        status: str,
        annotations: Dict[str, str],
        resource_version: str,
        event: Any,
    ) -> None:
        """Process status response"""
        if status == 'Pending':
            if event['type'] == 'DELETED':
                self.log.info('Event: Failed to start pod %s', pod_id)
                self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.info('Event: %s Pending', pod_id)
        elif status == 'Failed':
            self.log.error('Event: %s Failed', pod_id)
            self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
        elif status == 'Succeeded':
            self.log.info('Event: %s Succeeded', pod_id)
            self.watcher_queue.put((pod_id, namespace, None, annotations, resource_version))
        elif status == 'Running':
            self.log.info('Event: %s is Running', pod_id)
        else:
            self.log.warning(
                'Event: Invalid state: %s on pod: %s in namespace %s with annotations: %s with '
                'resource_version: %s',
                status,
                pod_id,
                namespace,
                annotations,
                resource_version,
            )


class AirflowKubernetesScheduler(LoggingMixin):
    """Airflow Scheduler for Kubernetes"""

    def __init__(
        self,
        kube_config: Any,
        task_queue: 'Queue[KubernetesJobType]',
        result_queue: 'Queue[KubernetesResultsType]',
        kube_client: client.CoreV1Api,
        scheduler_job_id: str,
    ):
        super().__init__()
        self.log.debug("Creating Kubernetes executor")
        self.kube_config = kube_config
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.log.debug("Kubernetes using namespace %s", self.namespace)
        self.kube_client = kube_client
        self.launcher = PodLauncher(kube_client=self.kube_client)
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.scheduler_job_id = scheduler_job_id
        self.kube_watcher = self._make_kube_watcher()

    def _make_kube_watcher(self) -> KubernetesJobWatcher:
        resource_version = ResourceVersion().resource_version
        watcher = KubernetesJobWatcher(
            watcher_queue=self.watcher_queue,
            namespace=self.kube_config.kube_namespace,
            multi_namespace_mode=self.kube_config.multi_namespace_mode,
            resource_version=resource_version,
            scheduler_job_id=self.scheduler_job_id,
            kube_config=self.kube_config,
        )
        watcher.start()
        return watcher

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            self.log.debug("KubeJobWatcher alive, continuing")
        else:
            self.log.error(
                'Error while health checking kube watcher process. Process died for unknown reasons'
            )
            self.kube_watcher = self._make_kube_watcher()

    def run_next(self, next_job: KubernetesJobType) -> None:
        """
        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevant info in the current_jobs map so we can track the job's
        status
        """
        self.log.info('Kubernetes job is %s', str(next_job))
        key, command, kube_executor_config, pod_template_file = next_job
        dag_id, task_id, execution_date, try_number = key

        if command[0:3] != ["airflow", "tasks", "run"]:
            raise ValueError('The command must start with ["airflow", "tasks", "run"].')

        base_worker_pod = get_base_pod_from_template(pod_template_file, self.kube_config)

        if not base_worker_pod:
            raise AirflowException(
                f"could not find a valid worker template yaml at {self.kube_config.pod_template_file}"
            )

        pod = PodGenerator.construct_pod(
            namespace=self.namespace,
            scheduler_job_id=self.scheduler_job_id,
            pod_id=create_pod_id(dag_id, task_id),
            dag_id=dag_id,
            task_id=task_id,
            kube_image=self.kube_config.kube_image,
            try_number=try_number,
            date=execution_date,
            args=command,
            pod_override_object=kube_executor_config,
            base_worker_pod=base_worker_pod,
        )
        # Reconcile the pod generated by the Operator and the Pod
        # generated by the .cfg file
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", pod.spec.containers[0].image)

        # the watcher will monitor pods, so we do not block.
        self.launcher.run_pod_async(pod, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_id: str, namespace: str) -> None:
        """Deletes POD"""
        try:
            self.log.debug("Deleting pod %s in namespace %s", pod_id, namespace)
            self.kube_client.delete_namespaced_pod(
                pod_id,
                namespace,
                body=client.V1DeleteOptions(**self.kube_config.delete_option_kwargs),
                **self.kube_config.kube_client_request_args,
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def sync(self) -> None:
        """
        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, its status is placed in the result queue to
        be sent back to the scheduler.

        :return:

        """
        self.log.debug("Syncing KubernetesExecutor")
        self._health_check_kube_watcher()
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                try:
                    self.log.debug("Processing task %s", task)
                    self.process_watcher_task(task)
                finally:
                    self.watcher_queue.task_done()
            except Empty:
                break

    def process_watcher_task(self, task: KubernetesWatchType) -> None:
        """Process the task by watcher."""
        pod_id, namespace, state, annotations, resource_version = task
        self.log.info(
            'Attempting to finish pod; pod_id: %s; state: %s; annotations: %s', pod_id, state, annotations
        )
        key = self._annotations_to_key(annotations=annotations)
        if key:
            self.log.debug('finishing job %s - %s (%s)', key, state, pod_id)
            self.result_queue.put((key, state, pod_id, namespace, resource_version))

    def _annotations_to_key(self, annotations: Dict[str, str]) -> Optional[TaskInstanceKey]:
        self.log.debug("Creating task key for annotations %s", annotations)
        dag_id = annotations['dag_id']
        task_id = annotations['task_id']
        try_number = int(annotations['try_number'])
        execution_date = parser.parse(annotations['execution_date'])

        return TaskInstanceKey(dag_id, task_id, execution_date, try_number)

    def _flush_watcher_queue(self) -> None:
        self.log.debug('Executor shutting down, watcher_queue approx. size=%d', self.watcher_queue.qsize())
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                # Ignoring it since it can only have either FAILED or SUCCEEDED pods
                self.log.warning('Executor shutting down, IGNORING watcher task=%s', task)
                self.watcher_queue.task_done()
            except Empty:
                break

    def terminate(self) -> None:
        """Terminates the watcher."""
        self.log.debug("Terminating kube_watcher...")
        self.kube_watcher.terminate()
        self.kube_watcher.join()
        self.log.debug("kube_watcher=%s", self.kube_watcher)
        self.log.debug("Flushing watcher_queue...")
        self._flush_watcher_queue()
        # Queue should be empty...
        self.watcher_queue.join()
        self.log.debug("Shutting down manager...")
        self._manager.shutdown()


def get_base_pod_from_template(pod_template_file: Optional[str], kube_config: Any) -> k8s.V1Pod:
    """
    Reads either the pod_template_file set in the executor_config or the base pod_template_file
    set in the airflow.cfg to craft a "base pod" that will be used by the KubernetesExecutor

    :param pod_template_file: absolute path to a pod_template_file.yaml or None
    :param kube_config: The KubeConfig class generated by airflow that contains all kube metadata
    :return: a V1Pod that can be used as the base pod for k8s tasks
    """
    if pod_template_file:
        return PodGenerator.deserialize_model_file(pod_template_file)
    else:
        return PodGenerator.deserialize_model_file(kube_config.pod_template_file)


class KubernetesExecutor(BaseExecutor, LoggingMixin):
    """Executor for Kubernetes"""

    def __init__(self):
        self.kube_config = KubeConfig()
        self._manager = multiprocessing.Manager()
        self.task_queue: 'Queue[KubernetesJobType]' = self._manager.Queue()
        self.result_queue: 'Queue[KubernetesResultsType]' = self._manager.Queue()
        self.kube_scheduler: Optional[AirflowKubernetesScheduler] = None
        self.kube_client: Optional[client.CoreV1Api] = None
        self.scheduler_job_id: Optional[str] = None
        super().__init__(parallelism=self.kube_config.parallelism)

    @provide_session
    def clear_not_launched_queued_tasks(self, session=None) -> None:
        """
        If the airflow scheduler restarts with pending "Queued" tasks, the tasks may or
        may not
        have been launched. Thus on starting up the scheduler let's check every
        "Queued" task to
        see if it has been launched (ie: if there is a corresponding pod on kubernetes)

        If it has been launched then do nothing, otherwise reset the state to "None" so
        the task
        will be rescheduled

        This will not be necessary in a future version of airflow in which there is
        proper support
        for State.LAUNCHED
        """
        self.log.debug("Clearing tasks that have not been launched")
        if not self.kube_client:
            raise AirflowException(NOT_STARTED_MESSAGE)
        queued_tasks = session.query(TaskInstance).filter(TaskInstance.state == State.QUEUED).all()
        self.log.info('When executor started up, found %s queued task instances', len(queued_tasks))

        for task in queued_tasks:
            # pylint: disable=protected-access
            self.log.debug("Checking task %s", task)
            dict_string = "dag_id={},task_id={},execution_date={},airflow-worker={}".format(
                pod_generator.make_safe_label_value(task.dag_id),
                pod_generator.make_safe_label_value(task.task_id),
                pod_generator.datetime_to_label_safe_datestring(task.execution_date),
                pod_generator.make_safe_label_value(str(self.scheduler_job_id)),
            )
            # pylint: enable=protected-access
            kwargs = dict(label_selector=dict_string)
            if self.kube_config.kube_client_request_args:
                for key, value in self.kube_config.kube_client_request_args.items():
                    kwargs[key] = value
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if not pod_list.items:
                self.log.info(
                    'TaskInstance: %s found in queued state but was not launched, rescheduling', task
                )
                session.query(TaskInstance).filter(
                    TaskInstance.dag_id == task.dag_id,
                    TaskInstance.task_id == task.task_id,
                    TaskInstance.execution_date == task.execution_date,
                ).update({TaskInstance.state: State.NONE})

    def start(self) -> None:
        """Starts the executor"""
        self.log.info('Start Kubernetes executor')
        if not self.job_id:
            raise AirflowException("Could not get scheduler_job_id")
        self.scheduler_job_id = self.job_id
        self.log.debug('Start with scheduler_job_id: %s', self.scheduler_job_id)
        self.kube_client = get_kube_client()
        self.kube_scheduler = AirflowKubernetesScheduler(
            self.kube_config, self.task_queue, self.result_queue, self.kube_client, self.scheduler_job_id
        )
        self.clear_not_launched_queued_tasks()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:
        """Executes task asynchronously"""
        self.log.info('Add task %s with command %s with executor_config %s', key, command, executor_config)
        try:
            kube_executor_config = PodGenerator.from_obj(executor_config)
        except Exception:  # pylint: disable=broad-except
            self.log.error("Invalid executor_config for %s", key)
            self.fail(key=key, info="Invalid executor_config passed")
            return

        if executor_config:
            pod_template_file = executor_config.get("pod_template_override", None)
        else:
            pod_template_file = None
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.event_buffer[key] = (State.QUEUED, self.scheduler_job_id)
        self.task_queue.put((key, command, kube_executor_config, pod_template_file))

    def sync(self) -> None:
        """Synchronize task state."""
        if self.running:
            self.log.debug('self.running: %s', self.running)
        if self.queued_tasks:
            self.log.debug('self.queued: %s', self.queued_tasks)
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.kube_scheduler:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.kube_config:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.result_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.kube_scheduler.sync()

        last_resource_version = None
        while True:  # pylint: disable=too-many-nested-blocks
            try:
                results = self.result_queue.get_nowait()
                try:
                    key, state, pod_id, namespace, resource_version = results
                    last_resource_version = resource_version
                    self.log.info('Changing state of %s to %s', results, state)
                    try:
                        self._change_state(key, state, pod_id, namespace)
                    except Exception as e:  # pylint: disable=broad-except
                        self.log.exception(
                            "Exception: %s when attempting to change state of %s to %s, re-queueing.",
                            e,
                            results,
                            state,
                        )
                        self.result_queue.put(results)
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

        resource_instance = ResourceVersion()
        resource_instance.resource_version = last_resource_version or resource_instance.resource_version

        # pylint: disable=too-many-nested-blocks
        for _ in range(self.kube_config.worker_pods_creation_batch_size):
            try:
                task = self.task_queue.get_nowait()
                try:
                    self.kube_scheduler.run_next(task)
                except ApiException as e:
                    if e.reason == "BadRequest":
                        self.log.error("Request was invalid. Failing task")
                        key, _, _, _ = task
                        self.change_state(key, State.FAILED, e)
                    else:
                        self.log.warning(
                            'ApiException when attempting to run task, re-queueing. Message: %s',
                            json.loads(e.body)['message'],
                        )
                        self.task_queue.put(task)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break
        # pylint: enable=too-many-nested-blocks

    def _change_state(self, key: TaskInstanceKey, state: Optional[str], pod_id: str, namespace: str) -> None:
        if state != State.RUNNING:
            if self.kube_config.delete_worker_pods:
                if not self.kube_scheduler:
                    raise AirflowException(NOT_STARTED_MESSAGE)
                if state != State.FAILED or self.kube_config.delete_worker_pods_on_failure:
                    self.kube_scheduler.delete_pod(pod_id, namespace)
                    self.log.info('Deleted pod: %s in namespace %s', str(key), str(namespace))
            try:
                self.running.remove(key)
            except KeyError:
                self.log.debug('Could not find key: %s', str(key))
        self.event_buffer[key] = state, None

    def try_adopt_task_instances(self, tis: List[TaskInstance]) -> List[TaskInstance]:
        tis_to_flush = [ti for ti in tis if not ti.external_executor_id]
        scheduler_job_ids = [ti.external_executor_id for ti in tis]
        pod_ids = {
            create_pod_id(
                dag_id=pod_generator.make_safe_label_value(ti.dag_id),
                task_id=pod_generator.make_safe_label_value(ti.task_id),
            ): ti
            for ti in tis
            if ti.external_executor_id
        }
        kube_client: client.CoreV1Api = self.kube_client
        for scheduler_job_id in scheduler_job_ids:
            scheduler_job_id = pod_generator.make_safe_label_value(str(scheduler_job_id))
            kwargs = {'label_selector': f'airflow-worker={scheduler_job_id}'}
            pod_list = kube_client.list_namespaced_pod(namespace=self.kube_config.kube_namespace, **kwargs)
            for pod in pod_list.items:
                self.adopt_launched_task(kube_client, pod, pod_ids)
        self._adopt_completed_pods(kube_client)
        tis_to_flush.extend(pod_ids.values())
        return tis_to_flush

    def adopt_launched_task(self, kube_client, pod, pod_ids: dict):
        """
        Patch existing pod so that the current KubernetesJobWatcher can monitor it via label selectors

        :param kube_client: kubernetes client for speaking to kube API
        :param pod: V1Pod spec that we will patch with new label
        :param pod_ids: pod_ids we expect to patch.
        """
        self.log.info("attempting to adopt pod %s", pod.metadata.name)
        pod.metadata.labels['airflow-worker'] = pod_generator.make_safe_label_value(
            str(self.scheduler_job_id)
        )
        dag_id = pod.metadata.labels['dag_id']
        task_id = pod.metadata.labels['task_id']
        pod_id = create_pod_id(dag_id=dag_id, task_id=task_id)
        if pod_id not in pod_ids:
            self.log.error(
                "attempting to adopt task %s in dag %s which was not specified by database",
                task_id,
                dag_id,
            )
        else:
            try:
                kube_client.patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body=PodGenerator.serialize_pod(pod),
                )
                pod_ids.pop(pod_id)
            except ApiException as e:
                self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)

    def _adopt_completed_pods(self, kube_client: kubernetes.client.CoreV1Api):
        """

        Patch completed pod so that the KubernetesJobWatcher can delete it.

        :param kube_client: kubernetes client for speaking to kube API
        """
        kwargs = {
            'field_selector': "status.phase=Succeeded",
            'label_selector': 'kubernetes_executor=True',
        }
        pod_list = kube_client.list_namespaced_pod(namespace=self.kube_config.kube_namespace, **kwargs)
        for pod in pod_list.items:
            self.log.info("Attempting to adopt pod %s", pod.metadata.name)
            pod.metadata.labels['airflow-worker'] = pod_generator.make_safe_label_value(
                str(self.scheduler_job_id)
            )
            try:
                kube_client.patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body=PodGenerator.serialize_pod(pod),
                )
            except ApiException as e:
                self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)

    def _flush_task_queue(self) -> None:
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.debug('Executor shutting down, task_queue approximate size=%d', self.task_queue.qsize())
        while True:
            try:
                task = self.task_queue.get_nowait()
                # This is a new task to run thus ok to ignore.
                self.log.warning('Executor shutting down, will NOT run task=%s', task)
                self.task_queue.task_done()
            except Empty:
                break

    def _flush_result_queue(self) -> None:
        if not self.result_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.debug('Executor shutting down, result_queue approximate size=%d', self.result_queue.qsize())
        while True:  # pylint: disable=too-many-nested-blocks
            try:
                results = self.result_queue.get_nowait()
                self.log.warning('Executor shutting down, flushing results=%s', results)
                try:
                    key, state, pod_id, namespace, resource_version = results
                    self.log.info(
                        'Changing state of %s to %s : resource_version=%d', results, state, resource_version
                    )
                    try:
                        self._change_state(key, state, pod_id, namespace)
                    except Exception as e:  # pylint: disable=broad-except
                        self.log.exception(
                            'Ignoring exception: %s when attempting to change state of %s to %s.',
                            e,
                            results,
                            state,
                        )
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

    def end(self) -> None:
        """Called when the executor shuts down"""
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.result_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.kube_scheduler:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.info('Shutting down Kubernetes executor')
        self.log.debug('Flushing task_queue...')
        self._flush_task_queue()
        self.log.debug('Flushing result_queue...')
        self._flush_result_queue()
        # Both queues should be empty...
        self.task_queue.join()
        self.result_queue.join()
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
