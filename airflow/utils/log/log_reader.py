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

import logging
from typing import Dict, Iterator, List, Optional, Tuple

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.configuration import conf
from airflow.models import TaskInstance
from airflow.utils.helpers import render_log_filename
from airflow.utils.log.logging_mixin import ExternalLoggingMixin


class TaskLogReader:
    """Task log reader"""

    def read_log_chunks(
        self, ti: TaskInstance, try_number: Optional[int], metadata
    ) -> Tuple[List[Tuple[Tuple[str, str]]], Dict[str, str]]:
        """
        Reads chunks of Task Instance logs.

        :param ti: The taskInstance
        :type ti: TaskInstance
        :param try_number: If provided, logs for the given try will be returned.
            Otherwise, logs from all attempts are returned.
        :type try_number: Optional[int]
        :param metadata: A dictionary containing information about how to read the task log
        :type metadata: dict
        :rtype: Tuple[List[Tuple[Tuple[str, str]]], Dict[str, str]]

        The following is an example of how to use this method to read log:

        .. code-block:: python

            logs, metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)
            logs = logs[0] if try_number is not None else logs

        where task_log_reader is an instance of TaskLogReader. The metadata will always
        contain information about the task log which can enable you read logs to the
        end.
        """
        logs, metadatas = self.log_handler.read(ti, try_number, metadata=metadata)
        metadata = metadatas[0]
        return logs, metadata

    def read_log_stream(self, ti: TaskInstance, try_number: Optional[int], metadata: dict) -> Iterator[str]:
        """
        Used to continuously read log to the end

        :param ti: The Task Instance
        :type ti: TaskInstance
        :param try_number: the task try number
        :type try_number: Optional[int]
        :param metadata: A dictionary containing information about how to read the task log
        :type metadata: dict
        :rtype: Iterator[str]
        """
        if try_number is None:
            next_try = ti.next_try_number
            try_numbers = list(range(1, next_try))
        else:
            try_numbers = [try_number]
        for current_try_number in try_numbers:
            metadata.pop('end_of_log', None)
            metadata.pop('max_offset', None)
            metadata.pop('offset', None)
            while 'end_of_log' not in metadata or not metadata['end_of_log']:
                logs, metadata = self.read_log_chunks(ti, current_try_number, metadata)
                for host, log in logs[0]:
                    yield "\n".join([host, log]) + "\n"

    @cached_property
    def log_handler(self):
        """Log handler, which is configured to read logs."""
        logger = logging.getLogger('airflow.task')
        task_log_reader = conf.get('logging', 'task_log_reader')
        handler = next((handler for handler in logger.handlers if handler.name == task_log_reader), None)
        return handler

    @property
    def supports_read(self):
        """Checks if a read operation is supported by a current log handler."""
        return hasattr(self.log_handler, 'read')

    @property
    def supports_external_link(self):
        """Check if the logging handler supports external links (e.g. to Elasticsearch, Stackdriver, etc)."""
        return isinstance(self.log_handler, ExternalLoggingMixin)

    def render_log_filename(self, ti: TaskInstance, try_number: Optional[int] = None):
        """
        Renders the log attachment filename

        :param ti: The task instance
        :type ti: TaskInstance
        :param try_number: The task try number
        :type try_number: Optional[int]
        :rtype: str
        """
        filename_template = conf.get('logging', 'LOG_FILENAME_TEMPLATE')
        attachment_filename = render_log_filename(
            ti=ti, try_number="all" if try_number is None else try_number, filename_template=filename_template
        )
        return attachment_filename
