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

"""This module contains the Apache Livy hook."""
import json
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Union

import requests

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.log.logging_mixin import LoggingMixin


class BatchState(Enum):
    """Batch session states"""

    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    RUNNING = 'running'
    IDLE = 'idle'
    BUSY = 'busy'
    SHUTTING_DOWN = 'shutting_down'
    ERROR = 'error'
    DEAD = 'dead'
    KILLED = 'killed'
    SUCCESS = 'success'


class LivyHook(HttpHook, LoggingMixin):
    """
    Hook for Apache Livy through the REST API.

    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :type livy_conn_id: str

    .. seealso::
        For more details refer to the Apache Livy API reference:
        https://livy.apache.org/docs/latest/rest-api.html
    """

    TERMINAL_STATES = {
        BatchState.SUCCESS,
        BatchState.DEAD,
        BatchState.KILLED,
        BatchState.ERROR,
    }

    _def_headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    conn_name_attr = 'livy_conn_id'
    default_conn_name = 'livy_default'
    conn_type = 'livy'
    hook_name = 'Apache Livy'

    def __init__(
        self, livy_conn_id: str = default_conn_name, extra_options: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(http_conn_id=livy_conn_id)
        self.extra_options = extra_options or {}

    def get_conn(self, headers: Optional[Dict[str, Any]] = None) -> Any:
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :return: requests session
        :rtype: requests.Session
        """
        tmp_headers = self._def_headers.copy()  # setting default headers
        if headers:
            tmp_headers.update(headers)
        return super().get_conn(tmp_headers)

    def run_method(
        self,
        endpoint: str,
        method: str = 'GET',
        data: Optional[Any] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Wrapper for HttpHook, allows to change method on the same HttpHook

        :param method: http method
        :type method: str
        :param endpoint: endpoint
        :type endpoint: str
        :param data: request payload
        :type data: dict
        :param headers: headers
        :type headers: dict
        :return: http response
        :rtype: requests.Response
        """
        if method not in ('GET', 'POST', 'PUT', 'DELETE', 'HEAD'):
            raise ValueError(f"Invalid http method '{method}'")
        if not self.extra_options:
            self.extra_options = {'check_response': False}

        back_method = self.method
        self.method = method
        try:
            result = self.run(endpoint, data, headers, self.extra_options)
        finally:
            self.method = back_method
        return result

    def post_batch(self, *args: Any, **kwargs: Any) -> Any:
        """
        Perform request to submit batch

        :return: batch session id
        :rtype: int
        """
        batch_submit_body = json.dumps(self.build_post_batch_body(*args, **kwargs))

        if self.base_url is None:
            # need to init self.base_url
            self.get_conn()
        self.log.info("Submitting job %s to %s", batch_submit_body, self.base_url)

        response = self.run_method(method='POST', endpoint='/batches', data=batch_submit_body)
        self.log.debug("Got response: %s", response.text)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise AirflowException(
                "Could not submit batch. Status code: {}. Message: '{}'".format(
                    err.response.status_code, err.response.text
                )
            )

        batch_id = self._parse_post_response(response.json())
        if batch_id is None:
            raise AirflowException("Unable to parse the batch session id")
        self.log.info("Batch submitted with session id: %d", batch_id)

        return batch_id

    def get_batch(self, session_id: Union[int, str]) -> Any:
        """
        Fetch info about the specified batch

        :param session_id: identifier of the batch sessions
        :type session_id: int
        :return: response body
        :rtype: dict
        """
        self._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session %d", session_id)
        response = self.run_method(endpoint=f'/batches/{session_id}')

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )

        return response.json()

    def get_batch_state(self, session_id: Union[int, str]) -> BatchState:
        """
        Fetch the state of the specified batch

        :param session_id: identifier of the batch sessions
        :type session_id: Union[int, str]
        :return: batch state
        :rtype: BatchState
        """
        self._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session %d", session_id)
        response = self.run_method(endpoint=f'/batches/{session_id}/state')

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )

        jresp = response.json()
        if 'state' not in jresp:
            raise AirflowException(f"Unable to get state for batch with id: {session_id}")
        return BatchState(jresp['state'])

    def delete_batch(self, session_id: Union[int, str]) -> Any:
        """
        Delete the specified batch

        :param session_id: identifier of the batch sessions
        :type session_id: int
        :return: response body
        :rtype: dict
        """
        self._validate_session_id(session_id)

        self.log.info("Deleting batch session %d", session_id)
        response = self.run_method(method='DELETE', endpoint=f'/batches/{session_id}')

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warning("Got status code %d for session %d", err.response.status_code, session_id)
            raise AirflowException(
                "Could not kill the batch with session id: {}. Message: {}".format(
                    session_id, err.response.text
                )
            )

        return response.json()

    @staticmethod
    def _validate_session_id(session_id: Union[int, str]) -> None:
        """
        Validate session id is a int

        :param session_id: session id
        :type session_id: Union[int, str]
        """
        try:
            int(session_id)
        except (TypeError, ValueError):
            raise TypeError("'session_id' must be an integer")

    @staticmethod
    def _parse_post_response(response: Dict[Any, Any]) -> Any:
        """
        Parse batch response for batch id

        :param response: response body
        :type response: dict
        :return: session id
        :rtype: int
        """
        return response.get('id')

    @staticmethod
    def build_post_batch_body(
        file: str,
        args: Optional[Sequence[Union[str, int, float]]] = None,
        class_name: Optional[str] = None,
        jars: Optional[List[str]] = None,
        py_files: Optional[List[str]] = None,
        files: Optional[List[str]] = None,
        archives: Optional[List[str]] = None,
        name: Optional[str] = None,
        driver_memory: Optional[str] = None,
        driver_cores: Optional[Union[int, str]] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        num_executors: Optional[Union[int, str]] = None,
        queue: Optional[str] = None,
        proxy_user: Optional[str] = None,
        conf: Optional[Dict[Any, Any]] = None,
    ) -> Any:
        """
        Build the post batch request body.
        For more information about the format refer to
        .. seealso:: https://livy.apache.org/docs/latest/rest-api.html

        :param file: Path of the file containing the application to execute (required).
        :type file: str
        :param proxy_user: User to impersonate when running the job.
        :type proxy_user: str
        :param class_name: Application Java/Spark main class string.
        :type class_name: str
        :param args: Command line arguments for the application s.
        :type args: Sequence[Union[str, int, float]]
        :param jars: jars to be used in this sessions.
        :type jars: Sequence[str]
        :param py_files: Python files to be used in this session.
        :type py_files: Sequence[str]
        :param files: files to be used in this session.
        :type files: Sequence[str]
        :param driver_memory: Amount of memory to use for the driver process  string.
        :type driver_memory: str
        :param driver_cores: Number of cores to use for the driver process int.
        :type driver_cores: Union[str, int]
        :param executor_memory: Amount of memory to use per executor process  string.
        :type executor_memory: str
        :param executor_cores: Number of cores to use for each executor  int.
        :type executor_cores: Union[int, str]
        :param num_executors: Number of executors to launch for this session  int.
        :type num_executors: Union[str, int]
        :param archives: Archives to be used in this session.
        :type archives: Sequence[str]
        :param queue: The name of the YARN queue to which submitted string.
        :type queue: str
        :param name: The name of this session string.
        :type name: str
        :param conf: Spark configuration properties.
        :type conf: dict
        :return: request body
        :rtype: dict
        """
        # pylint: disable-msg=too-many-arguments

        body: Dict[str, Any] = {'file': file}

        if proxy_user:
            body['proxyUser'] = proxy_user
        if class_name:
            body['className'] = class_name
        if args and LivyHook._validate_list_of_stringables(args):
            body['args'] = [str(val) for val in args]
        if jars and LivyHook._validate_list_of_stringables(jars):
            body['jars'] = jars
        if py_files and LivyHook._validate_list_of_stringables(py_files):
            body['pyFiles'] = py_files
        if files and LivyHook._validate_list_of_stringables(files):
            body['files'] = files
        if driver_memory and LivyHook._validate_size_format(driver_memory):
            body['driverMemory'] = driver_memory
        if driver_cores:
            body['driverCores'] = driver_cores
        if executor_memory and LivyHook._validate_size_format(executor_memory):
            body['executorMemory'] = executor_memory
        if executor_cores:
            body['executorCores'] = executor_cores
        if num_executors:
            body['numExecutors'] = num_executors
        if archives and LivyHook._validate_list_of_stringables(archives):
            body['archives'] = archives
        if queue:
            body['queue'] = queue
        if name:
            body['name'] = name
        if conf and LivyHook._validate_extra_conf(conf):
            body['conf'] = conf

        return body

    @staticmethod
    def _validate_size_format(size: str) -> bool:
        """
        Validate size format.

        :param size: size value
        :type size: str
        :return: true if valid format
        :rtype: bool
        """
        if size and not (isinstance(size, str) and re.match(r'^\d+[kmgt]b?$', size, re.IGNORECASE)):
            raise ValueError(f"Invalid java size format for string'{size}'")
        return True

    @staticmethod
    def _validate_list_of_stringables(vals: Sequence[Union[str, int, float]]) -> bool:
        """
        Check the values in the provided list can be converted to strings.

        :param vals: list to validate
        :type vals: Sequence[Union[str, int, float]]
        :return: true if valid
        :rtype: bool
        """
        if (
            vals is None
            or not isinstance(vals, (tuple, list))
            or any(1 for val in vals if not isinstance(val, (str, int, float)))
        ):
            raise ValueError("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf: Dict[Any, Any]) -> bool:
        """
        Check configuration values are either strings or ints.

        :param conf: configuration variable
        :type conf: dict
        :return: true if valid
        :rtype: bool
        """
        if conf:
            if not isinstance(conf, dict):
                raise ValueError("'conf' argument must be a dict")
            if any(True for k, v in conf.items() if not (v and isinstance(v, str) or isinstance(v, int))):
                raise ValueError("'conf' values must be either strings or ints")
        return True
