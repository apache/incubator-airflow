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
"""Launches PODs"""
import json
import math
import time
from datetime import datetime as dt
from typing import Optional, Tuple

import pendulum
import tenacity
from kubernetes import client, watch
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from requests.exceptions import BaseHTTPError

from airflow.exceptions import AirflowException
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.pod_generator import PodDefaults
from airflow.settings import pod_mutation_hook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


class PodStatus:
    """Status of the PODs"""

    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'


class PodLauncher(LoggingMixin):
    """Launches PODS"""

    def __init__(
        self,
        kube_client: client.CoreV1Api = None,
        in_cluster: bool = True,
        cluster_context: Optional[str] = None,
        extract_xcom: bool = False,
    ):
        """
        Creates the launcher.

        :param kube_client: kubernetes client
        :param in_cluster: whether we are in cluster
        :param cluster_context: context of the cluster
        :param extract_xcom: whether we should extract xcom
        """
        super().__init__()
        self._client = kube_client or get_kube_client(in_cluster=in_cluster, cluster_context=cluster_context)
        self._watch = watch.Watch()
        self.extract_xcom = extract_xcom

    def run_pod_async(self, pod: V1Pod, **kwargs):
        """Runs POD asynchronously"""
        pod_mutation_hook(pod)

        sanitized_pod = self._client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug('Pod Creation Request: \n%s', json_pod)
        try:
            resp = self._client.create_namespaced_pod(
                body=sanitized_pod, namespace=pod.metadata.namespace, **kwargs
            )
            self.log.debug('Pod Creation Response: %s', resp)
        except Exception as e:
            self.log.exception('Exception when attempting to create Namespaced Pod: %s', json_pod)
            raise e
        return resp

    def delete_pod(self, pod: V1Pod):
        """Deletes POD"""
        try:
            self._client.delete_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace, body=client.V1DeleteOptions()
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def start_pod(self, pod: V1Pod, startup_timeout: int = 120):
        """
        Launches the pod synchronously and waits for completion.

        :param pod:
        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :return:
        """
        resp = self.run_pod_async(pod)
        curr_time = dt.now()
        if resp.status.start_time is None:
            while self.pod_not_started(pod):
                self.log.warning("Pod not yet started: %s", pod.metadata.name)
                delta = dt.now() - curr_time
                if delta.total_seconds() >= startup_timeout:
                    raise AirflowException("Pod took too long to start")
                time.sleep(1)

    def monitor_pod(self, pod: V1Pod, get_logs: bool) -> Tuple[State, Optional[str]]:
        """
        Monitors a pod and returns the final state

        :param pod: pod spec that will be monitored
        :type pod : V1Pod
        :param get_logs: whether to read the logs locally
        :return:  Tuple[State, Optional[str]]
        """
        if get_logs:
            read_logs_since_sec = None
            last_log_time = None
            while True:
                logs = self.read_pod_logs(pod, timestamps=True, since_seconds=read_logs_since_sec)
                for line in logs:
                    timestamp, message = self.parse_log_line(line.decode('utf-8'))
                    last_log_time = pendulum.parse(timestamp)
                    self.log.info(message)
                time.sleep(1)

                if not self.base_container_is_running(pod):
                    break

                self.log.warning('Pod %s log read interrupted', pod.metadata.name)
                if last_log_time:
                    delta = pendulum.now() - last_log_time
                    # Prefer logs duplication rather than loss
                    read_logs_since_sec = math.ceil(delta.total_seconds())
        result = None
        if self.extract_xcom:
            while self.base_container_is_running(pod):
                self.log.info('Container %s has state %s', pod.metadata.name, State.RUNNING)
                time.sleep(2)
            result = self._extract_xcom(pod)
            self.log.info(result)
            result = json.loads(result)
        while self.pod_is_running(pod):
            self.log.info('Pod %s has state %s', pod.metadata.name, State.RUNNING)
            time.sleep(2)
        return self._task_status(self.read_pod(pod)), result

    def parse_log_line(self, line: str) -> Tuple[str, str]:
        """
        Parse K8s log line and returns the final state

        :param line: k8s log line
        :type line: str
        :return: timestamp and log message
        :rtype: Tuple[str, str]
        """
        split_at = line.find(' ')
        if split_at == -1:
            raise Exception(f'Log not in "{{timestamp}} {{log}}" format. Got: {line}')
        timestamp = line[:split_at]
        message = line[split_at + 1 :].rstrip()
        return timestamp, message

    def _task_status(self, event):
        self.log.info('Event: %s had an event of type %s', event.metadata.name, event.status.phase)
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod: V1Pod):
        """Tests if pod has not started"""
        state = self._task_status(self.read_pod(pod))
        return state == State.QUEUED

    def pod_is_running(self, pod: V1Pod):
        """Tests if pod is running"""
        state = self._task_status(self.read_pod(pod))
        return state not in (State.SUCCESS, State.FAILED)

    def base_container_is_running(self, pod: V1Pod):
        """Tests if base container is running"""
        event = self.read_pod(pod)
        status = next(iter(filter(lambda s: s.name == 'base', event.status.container_statuses)), None)
        if not status:
            return False
        return status.state.running is not None

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_logs(
        self,
        pod: V1Pod,
        tail_lines: Optional[int] = None,
        timestamps: bool = False,
        since_seconds: Optional[int] = None,
    ):
        """Reads log from the POD"""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs['since_seconds'] = since_seconds

        if tail_lines:
            additional_kwargs['tail_lines'] = tail_lines

        try:
            return self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container='base',
                follow=True,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except BaseHTTPError as e:
            raise AirflowException(f'There was an error reading the kubernetes API: {e}')

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_events(self, pod):
        """Reads events from the POD"""
        try:
            return self._client.list_namespaced_event(
                namespace=pod.metadata.namespace, field_selector=f"involvedObject.name={pod.metadata.name}"
            )
        except BaseHTTPError as e:
            raise AirflowException(f'There was an error reading the kubernetes API: {e}')

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod(self, pod: V1Pod):
        """Read POD information"""
        try:
            return self._client.read_namespaced_pod(pod.metadata.name, pod.metadata.namespace)
        except BaseHTTPError as e:
            raise AirflowException(f'There was an error reading the kubernetes API: {e}')

    def _extract_xcom(self, pod: V1Pod):
        resp = kubernetes_stream(
            self._client.connect_get_namespaced_pod_exec,
            pod.metadata.name,
            pod.metadata.namespace,
            container=PodDefaults.SIDECAR_CONTAINER_NAME,
            command=['/bin/sh'],
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False,
            _preload_content=False,
        )
        try:
            result = self._exec_pod_command(resp, f'cat {PodDefaults.XCOM_MOUNT_PATH}/return.json')
            self._exec_pod_command(resp, 'kill -s SIGINT 1')
        finally:
            resp.close()
        if result is None:
            raise AirflowException(f'Failed to extract xcom from pod: {pod.metadata.name}')
        return result

    def _exec_pod_command(self, resp, command):
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    return resp.read_stdout()
                if resp.peek_stderr():
                    self.log.info(resp.read_stderr())
                    break
        return None

    def process_status(self, job_id, status):
        """Process status information for the JOB"""
        status = status.lower()
        if status == PodStatus.PENDING:
            return State.QUEUED
        elif status == PodStatus.FAILED:
            self.log.error('Event with job id %s Failed', job_id)
            return State.FAILED
        elif status == PodStatus.SUCCEEDED:
            self.log.info('Event with job id %s Succeeded', job_id)
            return State.SUCCESS
        elif status == PodStatus.RUNNING:
            return State.RUNNING
        else:
            self.log.error('Event: Invalid state %s on job %s', status, job_id)
            return State.FAILED
