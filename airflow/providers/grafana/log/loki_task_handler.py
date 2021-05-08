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

"""Loki logging handler for tasks"""
import time
from typing import Dict, Optional, Tuple, Union

import logging_loki
import requests
from cached_property import cached_property

from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

DEFAULT_LOGGER_NAME = "airflow"


class LokiTaskHandler(FileTaskHandler, LoggingMixin):
    """
    LokiTaskHandler that directly makes Loki logging API calls while reading and writing logs.
    This is a Python standard ``logging`` handler using that can be used to route Python standard
    logging messages directly to the Loki Logging API. It can also be used to save logs for
    executing tasks. To do this, you should set as a handler with the name "tasks". In this case,
    it will also be used to read the log for display in Web UI.
    :param base_log_folder: Base log folder to place logs (incase Loki is down).
    :type base_log_folder: str
    :param filename_template: template filename string (incase Loki is down)
    :type filename_template: str
    :param loki_conn_id: Connection ID that will be used for authorization to the Loki Platform.
    :type loki_conn_id: str
    :param name: the name of the custom log in Loki Logging. Defaults to 'airflow'.
    :type name: str
    :param labels: (Optional) Mapping of labels for the entry.
    :type labels: dict
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        base_log_folder: str,
        filename_template: str,
        loki_conn_id: str,
        name: str = DEFAULT_LOGGER_NAME,
        labels: Optional[Dict[str, str]] = None,
    ):
        super().__init__(base_log_folder, filename_template)
        self.loki_conn_id = loki_conn_id
        self.name: str = name
        self.timestamp_pattern = "%Y-%m-%dT%H:%M:%S"
        self.labels = labels
        self._session: Optional[requests.Session] = None

    @cached_property
    def get_conn(self):
        """Loki connection for client"""
        return BaseHook.get_connection(self.loki_conn_id)

    @property
    def session(self) -> requests.Session:
        """Create HTTP session"""
        if self._session is None:
            self._session = requests.Session()
            self._session.auth = (self.get_conn.login, self.get_conn.password) or None
        return self._session

    def is_loki_alive(self):
        """Checks whether Loki is ready for pushing/pulling logs"""
        try:
            status = self.session.get(
                f"{self.get_conn.host}/ready",
            )
            return status.status_code
        except ConnectionError as error_msg:
            self.log.exception(error_msg)
            return None

    @staticmethod
    def _task_label(task_instance: TaskInstance) -> Dict[str, str]:
        """
        Returns task instance labels for Loki which will use while reading
        and writing logs from loki.
        :param task_instance: task instance object
        :type: task_instance: TaskInstance
        """
        # Not adding execution date since it violates Loki label standards
        # https://grafana.com/blog/2020/08/27/the-concise-guide-to-labels-in-loki/

        return {
            "airflow_dag_id": task_instance.dag_id,
            "airflow_task_id": task_instance.task_id,
            "airflow_try_number": str(task_instance.try_number),
        }

    def get_label(self, task_instance: TaskInstance) -> Dict[str, str]:
        """
        Update task_labels with optional labels and return Loki labels.
        :param task_instance: task instance object
        :type: task_instance: TaskInstance
        """
        tags = {}
        task_labels = self._task_label(task_instance)
        if self.labels:
            tags.update(self.labels)
            tags.update(task_labels)
            return tags
        return task_labels

    def client(self, task_instance: TaskInstance) -> logging_loki.handlers.LokiHandler:
        """
        Returns Loki logger handler using connection and label to write logs
        :param task_instance: task instance object
        :type: task_instance: TaskInstance
        """
        return logging_loki.LokiHandler(
            url=f"{self.get_conn.host}/loki/api/v1/push",
            tags=self.get_label(task_instance),
            version="1",
            auth=(f"{self.get_conn.login}", f"{self.get_conn.password}"),
        )

    def set_context(self, task_instance: TaskInstance) -> None:
        """
        Provide task_instance context to airflow task handler.
        If Loki is not available then will use Filehandler context
        to avoid losing log data.
        :param ti: task instance object
        :type: task_instance: TaskInstance
        """
        if self.is_loki_alive() == 200:
            self.handler: logging_loki.handlers.LokiHandler = self.client(task_instance)
            if self.formatter:
                self.handler.setFormatter(self.formatter)
                self.handler.setLevel(self.level)
        else:
            super().set_context(task_instance)  # Filehandler Context

    def convert_epoch_time(self, timestamp: str) -> int:
        """Convert timestamp to epoch"""
        return int(time.mktime(time.strptime(timestamp[:19], self.timestamp_pattern)))

    def query_time_range(
        self,
        task_instance: TaskInstance,
    ) -> Tuple[int, int]:
        """
        Return task start and end time in epoch format for query.
        If Task's try_number > 1 then it will use task execution time as start time.
        :param ti: task instance object
        :type: task_instance: TaskInstance
        """
        start_time, end_time, execution_time = (
            str(task_instance.start_date.isoformat()),
            task_instance.end_date,
            str(task_instance.execution_date.isoformat()),
        )
        next_try = task_instance.next_try_number
        start_epoch_time = self.convert_epoch_time(start_time)

        if end_time and next_try < 3:
            end_epoch_time = self.convert_epoch_time(end_time.isoformat())
            return (start_epoch_time - 60, end_epoch_time + 60 * 2)
        if end_time:
            end_epoch_time = self.convert_epoch_time(end_time.isoformat())
            execution_time_epoch_time = self.convert_epoch_time(execution_time)
            return (execution_time_epoch_time - 60, end_epoch_time + 60 * 2)
        return (start_epoch_time - 60, start_epoch_time + 60 * 60 * 24)

    def get_read_parameters(self, task_instance, try_number) -> Dict[str, Union[str, int]]:
        """
        Construct query(LogQL) and update parameters for loki read request.
        :param ti: task instance object
        :type: task_instance: TaskInstance
        :param try_number: current try_number to read log from
        :type: try_number: int
        """
        start_time, end_time = self.query_time_range(task_instance)
        task_lables = self.get_label(task_instance)
        del task_lables["airflow_try_number"]  # try_number will come from read method

        # converting task_lables(dict) to LogQL format
        # {"dag_id":"airflow"} --> {dag_id = "airflow"}
        query = "{"
        for key, value in task_lables.items():
            query += f"""{key}="{value}", """
        query += f"""airflow_try_number="{try_number}"}} != "WARNING" """

        return {
            "direction": "forward",
            "start": start_time,
            "end": end_time,
            "query": query,
            "limit": 5000,
        }

    def _read(self, task_instance, try_number, metadata=None) -> Tuple[str, Dict[str, bool]]:
        """
        Return list of logs string for each task try number by streaming it from Loki
        :param ti: task instance object
        :type: task_instance: TaskInstance
        :param try_number: current try_number to read log from
        :type: try_number: int
        """
        logs = ""
        if self.is_loki_alive() == 200:
            query_parameters = self.get_read_parameters(task_instance, try_number)
            logs += f"*** Loki Query:{query_parameters['query']} \n"
            logs += (
                f"*** Epoch start_time: {query_parameters['start']} "
                f"and end_time {query_parameters['end']}\n"
            )
            try:
                response = self.session.get(
                    f"{self.get_conn.host}/loki/api/v1/query_range",
                    params=query_parameters,
                ).json()
                results = response["data"]["result"]
                logs_list = [value for result in results for value in result["values"]]
                # sorting with list of values with timestamp.
                logs += "\n".join([f"{log_line[1]}" for log_line in sorted(logs_list, key=lambda x: x[0])])
            except ConnectionError as error_msg:
                logs += "*** Failed to load log file from Loki \n"
                self.log.exception(error_msg)
            self.session.close()
            return logs, {"end_of_log": True}
        return super()._read(task_instance, try_number)
