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
"""Handler that integrates with Stackdriver"""
import logging
from typing import Collection, Dict, List, Optional, Tuple, Type
from urllib.parse import urlencode

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.gapic_v1.client_info import ClientInfo
from google.auth.credentials import Credentials
from google.cloud import logging as gcp_logging
from google.cloud.logging import Resource
from google.cloud.logging.handlers.transports import BackgroundThreadTransport, Transport
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse

from airflow import version
from airflow.models import TaskInstance
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id

DEFAULT_LOGGER_NAME = "airflow"
_GLOBAL_RESOURCE = Resource(type="global", labels={})

_DEFAULT_SCOPESS = frozenset(
    ["https://www.googleapis.com/auth/logging.read", "https://www.googleapis.com/auth/logging.write"]
)


class StackdriverTaskHandler(logging.Handler):
    """Handler that directly makes Stackdriver logging API calls.

    This is a Python standard ``logging`` handler using that can be used to
    route Python standard logging messages directly to the Stackdriver
    Logging API.

    It can also be used to save logs for executing tasks. To do this, you should set as a handler with
    the name "tasks". In this case, it will also be used to read the log for display in Web UI.

    This handler supports both an asynchronous and synchronous transport.

    :param gcp_key_path: Path to Google Cloud Credential JSON file.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :type gcp_key_path: str
    :param scopes: OAuth scopes for the credentials,
    :type scopes: Sequence[str]
    :param name: the name of the custom log in Stackdriver Logging. Defaults
        to 'airflow'. The name of the Python logger will be represented
        in the ``python_logger`` field.
    :type name: str
    :param transport: Class for creating new transport objects. It should
        extend from the base :class:`google.cloud.logging.handlers.Transport` type and
        implement :meth`google.cloud.logging.handlers.Transport.send`. Defaults to
        :class:`google.cloud.logging.handlers.BackgroundThreadTransport`. The other
        option is :class:`google.cloud.logging.handlers.SyncTransport`.
    :type transport: :class:`type`
    :param resource: (Optional) Monitored resource of the entry, defaults
                     to the global resource type.
    :type resource: :class:`~google.cloud.logging.resource.Resource`
    :param labels: (Optional) Mapping of labels for the entry.
    :type labels: dict
    """

    LABEL_TASK_ID = "task_id"
    LABEL_DAG_ID = "dag_id"
    LABEL_EXECUTION_DATE = "execution_date"
    LABEL_TRY_NUMBER = "try_number"
    LOG_VIEWER_BASE_URL = "https://console.cloud.google.com/logs/viewer"
    LOG_NAME = 'Google Stackdriver'

    def __init__(
        self,
        gcp_key_path: Optional[str] = None,
        # See: https://github.com/PyCQA/pylint/issues/2377
        scopes: Optional[Collection[str]] = _DEFAULT_SCOPESS,  # pylint: disable=unsubscriptable-object
        name: str = DEFAULT_LOGGER_NAME,
        transport: Type[Transport] = BackgroundThreadTransport,
        resource: Resource = _GLOBAL_RESOURCE,
        labels: Optional[Dict[str, str]] = None,
    ):
        super().__init__()
        self.gcp_key_path: Optional[str] = gcp_key_path
        # See: https://github.com/PyCQA/pylint/issues/2377
        self.scopes: Optional[Collection[str]] = scopes  # pylint: disable=unsubscriptable-object
        self.name: str = name
        self.transport_type: Type[Transport] = transport
        self.resource: Resource = resource
        self.labels: Optional[Dict[str, str]] = labels
        self.task_instance_labels: Optional[Dict[str, str]] = {}
        self.task_instance_hostname = 'default-hostname'

    @cached_property
    def _credentials_and_project(self) -> Tuple[Credentials, str]:
        credentials, project = get_credentials_and_project_id(
            key_path=self.gcp_key_path, scopes=self.scopes, disable_logging=True
        )
        return credentials, project

    @property
    def _client(self) -> gcp_logging.Client:
        """The Cloud Library API client"""
        credentials, project = self._credentials_and_project
        client = gcp_logging.Client(
            credentials=credentials,
            project=project,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version),
        )
        return client

    @property
    def _logging_service_client(self) -> LoggingServiceV2Client:
        """The Cloud logging service v2 client."""
        credentials, _ = self._credentials_and_project
        client = LoggingServiceV2Client(
            credentials=credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version),
        )
        return client

    @cached_property
    def _transport(self) -> Transport:
        """Object responsible for sending data to Stackdriver"""
        return self.transport_type(self._client, self.name)

    def emit(self, record: logging.LogRecord) -> None:
        """Actually log the specified logging record.

        :param record: The record to be logged.
        :type record: logging.LogRecord
        """
        message = self.format(record)
        labels: Optional[Dict[str, str]]
        if self.labels and self.task_instance_labels:
            labels = {}
            labels.update(self.labels)
            labels.update(self.task_instance_labels)
        elif self.labels:
            labels = self.labels
        elif self.task_instance_labels:
            labels = self.task_instance_labels
        else:
            labels = None
        self._transport.send(record, message, resource=self.resource, labels=labels)

    def set_context(self, task_instance: TaskInstance) -> None:
        """
        Configures the logger to add information with information about the current task

        :param task_instance: Currently executed task
        :type task_instance:  :class:`airflow.models.TaskInstance`
        """
        self.task_instance_labels = self._task_instance_to_labels(task_instance)
        self.task_instance_hostname = task_instance.hostname

    def read(
        self, task_instance: TaskInstance, try_number: Optional[int] = None, metadata: Optional[Dict] = None
    ) -> Tuple[List[Tuple[Tuple[str, str]]], List[Dict[str, str]]]:
        """
        Read logs of given task instance from Stackdriver logging.

        :param task_instance: task instance object
        :type task_instance: :class:`airflow.models.TaskInstance`
        :param try_number: task instance try_number to read logs from. If None
           it returns all logs
        :type try_number: Optional[int]
        :param metadata: log metadata. It is used for steaming log reading and auto-tailing.
        :type metadata: Dict
        :return: a tuple of (
            list of (one element tuple with two element tuple - hostname and logs)
            and list of metadata)
        :rtype: Tuple[List[Tuple[Tuple[str, str]]], List[Dict[str, str]]]
        """
        if try_number is not None and try_number < 1:
            logs = f"Error fetching the logs. Try number {try_number} is invalid."
            return [((self.task_instance_hostname, logs),)], [{"end_of_log": "true"}]

        if not metadata:
            metadata = {}

        ti_labels = self._task_instance_to_labels(task_instance)

        if try_number is not None:
            ti_labels[self.LABEL_TRY_NUMBER] = str(try_number)
        else:
            del ti_labels[self.LABEL_TRY_NUMBER]

        log_filter = self._prepare_log_filter(ti_labels)
        next_page_token = metadata.get("next_page_token", None)
        all_pages = 'download_logs' in metadata and metadata['download_logs']

        messages, end_of_log, next_page_token = self._read_logs(log_filter, next_page_token, all_pages)

        new_metadata = {"end_of_log": end_of_log}

        if next_page_token:
            new_metadata['next_page_token'] = next_page_token

        return [((self.task_instance_hostname, messages),)], [new_metadata]

    def _prepare_log_filter(self, ti_labels: Dict[str, str]) -> str:
        """
        Prepares the filter that chooses which log entries to fetch.

        More information:
        https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list#body.request_body.FIELDS.filter
        https://cloud.google.com/logging/docs/view/advanced-queries

        :param ti_labels: Task Instance's labels that will be used to search for logs
        :type: Dict[str, str]
        :return: logs filter
        """

        def escape_label_key(key: str) -> str:
            return f'"{key}"' if "." in key else key

        def escale_label_value(value: str) -> str:
            escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped_value}"'

        _, project = self._credentials_and_project
        log_filters = [
            f'resource.type={escale_label_value(self.resource.type)}',
            f'logName="projects/{project}/logs/{self.name}"',
        ]

        for key, value in self.resource.labels.items():
            log_filters.append(f'resource.labels.{escape_label_key(key)}={escale_label_value(value)}')

        for key, value in ti_labels.items():
            log_filters.append(f'labels.{escape_label_key(key)}={escale_label_value(value)}')
        return "\n".join(log_filters)

    def _read_logs(
        self, log_filter: str, next_page_token: Optional[str], all_pages: bool
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Sends requests to the Stackdriver service and downloads logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param next_page_token: The token of the page from which the log download will start.
            If None is passed, it will start from the first page.
        :param all_pages: If True is passed, all subpages will be downloaded. Otherwise, only the first
            page will be downloaded
        :return: A token that contains the following items:
            * string with logs
            * Boolean value describing whether there are more logs,
            * token of the next page
        :rtype: Tuple[str, bool, str]
        """
        messages = []
        new_messages, next_page_token = self._read_single_logs_page(
            log_filter=log_filter,
            page_token=next_page_token,
        )
        messages.append(new_messages)
        if all_pages:
            while next_page_token:
                new_messages, next_page_token = self._read_single_logs_page(
                    log_filter=log_filter, page_token=next_page_token
                )
                messages.append(new_messages)
                if not messages:
                    break

            end_of_log = True
            next_page_token = None
        else:
            end_of_log = not bool(next_page_token)
        return "\n".join(messages), end_of_log, next_page_token

    def _read_single_logs_page(self, log_filter: str, page_token: Optional[str] = None) -> Tuple[str, str]:
        """
        Sends requests to the Stackdriver service and downloads single pages with logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param page_token: The token of the page to be downloaded. If None is passed, the first page will be
            downloaded.
        :type page_token: str
        :return: Downloaded logs and next page token
        :rtype: Tuple[str, str]
        """
        _, project = self._credentials_and_project
        request = ListLogEntriesRequest(
            resource_names=[f'projects/{project}'],
            filter=log_filter,
            page_token=page_token,
            order_by='timestamp asc',
            page_size=1000,
        )
        response = self._logging_service_client.list_log_entries(request=request)
        page: ListLogEntriesResponse = next(response.pages)
        messages = []
        for entry in page.entries:
            if "message" in entry.json_payload:
                messages.append(entry.json_payload["message"])
        return "\n".join(messages), page.next_page_token

    @classmethod
    def _task_instance_to_labels(cls, ti: TaskInstance) -> Dict[str, str]:
        return {
            cls.LABEL_TASK_ID: ti.task_id,
            cls.LABEL_DAG_ID: ti.dag_id,
            cls.LABEL_EXECUTION_DATE: str(ti.execution_date.isoformat()),
            cls.LABEL_TRY_NUMBER: str(ti.try_number),
        }

    @property
    def log_name(self):
        """Return log name."""
        return self.LOG_NAME

    @cached_property
    def _resource_path(self):
        segments = [self.resource.type]

        for key, value in self.resource.labels:
            segments += [key]
            segments += [value]

        return "/".join(segments)

    def get_external_log_url(self, task_instance: TaskInstance, try_number: int) -> str:
        """
        Creates an address for an external log collecting service.
        :param task_instance: task instance object
        :type: task_instance: TaskInstance
        :param try_number: task instance try_number to read logs from.
        :type try_number: Optional[int]
        :return: URL to the external log collection service
        :rtype: str
        """
        _, project_id = self._credentials_and_project

        ti_labels = self._task_instance_to_labels(task_instance)
        ti_labels[self.LABEL_TRY_NUMBER] = str(try_number)

        log_filter = self._prepare_log_filter(ti_labels)

        url_query_string = {
            'project': project_id,
            'interval': 'NO_LIMIT',
            'resource': self._resource_path,
            'advancedFilter': log_filter,
        }

        url = f"{self.LOG_VIEWER_BASE_URL}?{urlencode(url_query_string)}"
        return url

    def close(self) -> None:
        self._transport.flush()
