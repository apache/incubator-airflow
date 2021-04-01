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

"""This module contains the Apache Livy operator."""
from time import sleep
from typing import Any, Dict, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.utils.decorators import apply_defaults


class LivyOperator(BaseOperator):
    """
    This operator wraps the Apache Livy batch REST API, allowing to submit a Spark
    application to the underlying cluster.

    :param file: path of the file containing the application to execute (required).
    :type file: str
    :param class_name: name of the application Java/Spark main class.
    :type class_name: str
    :param args: application command line arguments.
    :type args: list
    :param jars: jars to be used in this sessions.
    :type jars: list
    :param py_files: python files to be used in this session.
    :type py_files: list
    :param files: files to be used in this session.
    :type files: list
    :param driver_memory: amount of memory to use for the driver process.
    :type driver_memory: str
    :param driver_cores: number of cores to use for the driver process.
    :type driver_cores: str, int
    :param executor_memory: amount of memory to use per executor process.
    :type executor_memory: str
    :param executor_cores: number of cores to use for each executor.
    :type executor_cores: str, int
    :param num_executors: number of executors to launch for this session.
    :type num_executors: str, int
    :param archives: archives to be used in this session.
    :type archives: list
    :param queue: name of the YARN queue to which the application is submitted.
    :type queue: str
    :param name: name of this session.
    :type name: str
    :param conf: Spark configuration properties.
    :type conf: dict
    :param proxy_user: user to impersonate when running the job.
    :type proxy_user: str
    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :type livy_conn_id: str
    :param polling_interval: time in seconds between polling for job completion. Don't poll for values >=0
    :type polling_interval: int
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('spark_params',)

    @apply_defaults
    def __init__(
        self,
        *,
        file: str,
        class_name: Optional[str] = None,
        args: Optional[Sequence[Union[str, int, float]]] = None,
        conf: Optional[Dict[Any, Any]] = None,
        jars: Optional[Sequence[str]] = None,
        py_files: Optional[Sequence[str]] = None,
        files: Optional[Sequence[str]] = None,
        driver_memory: Optional[str] = None,
        driver_cores: Optional[Union[int, str]] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[Union[int, str]] = None,
        num_executors: Optional[Union[int, str]] = None,
        archives: Optional[Sequence[str]] = None,
        queue: Optional[str] = None,
        name: Optional[str] = None,
        proxy_user: Optional[str] = None,
        livy_conn_id: str = 'livy_default',
        polling_interval: int = 0,
        extra_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        # pylint: disable-msg=too-many-arguments

        super().__init__(**kwargs)

        self.spark_params = {
            'file': file,
            'class_name': class_name,
            'args': args,
            'jars': jars,
            'py_files': py_files,
            'files': files,
            'driver_memory': driver_memory,
            'driver_cores': driver_cores,
            'executor_memory': executor_memory,
            'executor_cores': executor_cores,
            'num_executors': num_executors,
            'archives': archives,
            'queue': queue,
            'name': name,
            'conf': conf,
            'proxy_user': proxy_user,
        }

        self._livy_conn_id = livy_conn_id
        self._polling_interval = polling_interval
        self._extra_options = extra_options or {}

        self._livy_hook: Optional[LivyHook] = None
        self._batch_id: Union[int, str]

    def get_hook(self) -> LivyHook:
        """
        Get valid hook.

        :return: hook
        :rtype: LivyHook
        """
        if self._livy_hook is None or not isinstance(self._livy_hook, LivyHook):
            self._livy_hook = LivyHook(livy_conn_id=self._livy_conn_id, extra_options=self._extra_options)
        return self._livy_hook

    def execute(self, context: Dict[Any, Any]) -> Any:
        self._batch_id = self.get_hook().post_batch(**self.spark_params)

        if self._polling_interval > 0:
            self.poll_for_termination(self._batch_id)

        return self._batch_id

    def poll_for_termination(self, batch_id: Union[int, str]) -> None:
        """
        Pool Livy for batch termination.

        :param batch_id: id of the batch session to monitor.
        :type batch_id: int
        """
        hook = self.get_hook()
        state = hook.get_batch_state(batch_id)
        while state not in hook.TERMINAL_STATES:
            self.log.debug('Batch with id %s is in state: %s', batch_id, state.value)
            sleep(self._polling_interval)
            state = hook.get_batch_state(batch_id)
        self.log.info("Batch with id %s terminated with state: %s", batch_id, state.value)
        if state != BatchState.SUCCESS:
            raise AirflowException(f"Batch {batch_id} did not succeed")

    def on_kill(self) -> None:
        self.kill()

    def kill(self) -> None:
        """Delete the current batch session."""
        if self._batch_id is not None:
            self.get_hook().delete_batch(self._batch_id)
