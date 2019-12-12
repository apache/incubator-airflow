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
import time
from datetime import datetime as dt
from typing import Tuple, Optional

from requests.exceptions import BaseHTTPError

import tenacity

from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream

from airflow.settings import pod_mutation_hook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow import AirflowException

from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.kubernetes_request_factory import \
    pod_request_factory as pod_factory

from .kube_client import get_kube_client


class PodStatus(object):
    """Status of the PODs"""
    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'


class PodLauncher(LoggingMixin):
    """Launches PODS"""
    def __init__(self,
                 kube_client=None,
                 in_cluster=True,
                 cluster_context=None,
                 extract_xcom=False):
        """
        Creates the launcher.

        :param kube_client: kubernetes client
        :param in_cluster: whether we are in cluster
        :param cluster_context: context of the cluster
        :param extract_xcom: whether we should extract xcom
        """
        super(PodLauncher, self).__init__()
        self._client = kube_client or get_kube_client(in_cluster=in_cluster,
                                                      cluster_context=cluster_context)
        self._watch = watch.Watch()
        self.extract_xcom = extract_xcom
        self.kube_req_factory = pod_factory.ExtractXcomPodRequestFactory(
        ) if extract_xcom else pod_factory.SimplePodRequestFactory()

    def run_pod_async(self, pod, **kwargs):
        """Runs POD asynchronously"""
        pod_mutation_hook(pod)

        req = self.kube_req_factory.create(pod)
        self.log.debug('Pod Creation Request: \n%s', json.dumps(req, indent=2))
        try:
            resp = self._client.create_namespaced_pod(body=req, namespace=pod.namespace, **kwargs)
            self.log.debug('Pod Creation Response: %s', resp)
        except ApiException:
            self.log.exception('Exception when attempting to create Namespaced Pod.')
            raise
        return resp

    def delete_pod(self, pod):
        """Deletes POD"""
        try:
            self._client.delete_namespaced_pod(
                pod.name, pod.namespace, body=client.V1DeleteOptions())
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def run_pod(self, pod, startup_timeout=120, get_logs=True):
        # type: (Pod, int, bool) -> Tuple[State, Optional[str]]
        """
        Launches the pod synchronously and waits for completion.
        Args:
            pod (Pod):
            startup_timeout (int): Timeout for startup of the pod (if pod is pending for
             too long, considers task a failure
        """
        resp = self.run_pod_async(pod)
        curr_time = dt.now()
        if resp.status.start_time is None:
            while self.pod_not_started(pod):
                delta = dt.now() - curr_time
                if delta.seconds >= startup_timeout:
                    raise AirflowException("Pod took too long to start")
                time.sleep(1)
            self.log.debug('Pod not yet started')

        return self._monitor_pod(pod, get_logs)

    def _monitor_pod(self, pod, get_logs):
        # type: (Pod, bool) -> Tuple[State, Optional[str]]

        if get_logs:
            logs = self.read_pod_logs(pod)
            for line in logs:
                self.log.info(line)
        result = None
        if self.extract_xcom:
            while self.base_container_is_running(pod):
                self.log.info('Container %s has state %s', pod.name, State.RUNNING)
                time.sleep(2)
            result = self._extract_xcom(pod)
            self.log.info(result)
            result = json.loads(result)
        while self.pod_is_running(pod):
            self.log.info('Pod %s has state %s', pod.name, State.RUNNING)
            time.sleep(2)
        return self._task_status(self.read_pod(pod)), result

    def _task_status(self, event):
        self.log.info(
            'Event: %s had an event of type %s',
            event.metadata.name, event.status.phase)
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod):
        """Tests if pod has not started"""
        state = self._task_status(self.read_pod(pod))
        return state == State.QUEUED

    def pod_is_running(self, pod):
        """Tests if pod is running"""
        state = self._task_status(self.read_pod(pod))
        return state not in (State.SUCCESS, State.FAILED)

    def base_container_is_running(self, pod):
        """Tests if base container is running"""
        event = self.read_pod(pod)
        status = next(iter(filter(lambda s: s.name == 'base',
                                  event.status.container_statuses)), None)
        return status.state.running is not None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod_logs(self, pod):
        """Reads log from the POD"""
        try:
            return self._client.read_namespaced_pod_log(
                name=pod.name,
                namespace=pod.namespace,
                container='base',
                follow=True,
                tail_lines=10,
                _preload_content=False
            )
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod(self, pod):
        """Read POD information"""
        try:
            return self._client.read_namespaced_pod(pod.name, pod.namespace)
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    def _extract_xcom(self, pod):
        resp = kubernetes_stream(self._client.connect_get_namespaced_pod_exec,
                                 pod.name, pod.namespace,
                                 container=self.kube_req_factory.SIDECAR_CONTAINER_NAME,
                                 command=['/bin/sh'], stdin=True, stdout=True,
                                 stderr=True, tty=False,
                                 _preload_content=False)
        try:
            result = self._exec_pod_command(
                resp, 'cat {}/return.json'.format(self.kube_req_factory.XCOM_MOUNT_PATH))
            self._exec_pod_command(resp, 'kill -s SIGINT 1')
        finally:
            resp.close()
        if result is None:
            raise AirflowException('Failed to extract xcom from pod: {}'.format(pod.name))
        return result

    def _exec_pod_command(self, resp, command):
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            resp.run_forever(timeout=2)
            if resp.peek_stdout():
                return resp.read_stdout()
            if resp.peek_stderr():
                self.log.info(resp.read_stderr())
        return None

    def process_status(self, job_id, status):
        """Process status infomration for the JOB"""
        status = status.lower()
        if status == PodStatus.PENDING:
            return State.QUEUED
        elif status == PodStatus.FAILED:
            self.log.info('Event with job id %s Failed', job_id)
            return State.FAILED
        elif status == PodStatus.SUCCEEDED:
            self.log.info('Event with job id %s Succeeded', job_id)
            return State.SUCCESS
        elif status == PodStatus.RUNNING:
            return State.RUNNING
        else:
            self.log.info('Event: Invalid state %s on job %s', status, job_id)
            return State.FAILED
