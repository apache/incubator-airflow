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
#
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Exceptions used by Airflow"""
import datetime
from typing import Any, Dict, List, NamedTuple, Optional

from airflow.utils.code_utils import prepare_code_snippet
from airflow.utils.platform import is_tty


class AirflowException(Exception):
    """
    Base class for all Airflow's errors.
    Each custom exception should be derived from this class
    """

    status_code = 500


class AirflowBadRequest(AirflowException):
    """Raise when the application or server cannot handle the request"""

    status_code = 400


class AirflowNotFoundException(AirflowException):
    """Raise when the requested object/resource is not available in the system"""

    status_code = 404


class AirflowConfigException(AirflowException):
    """Raise when there is configuration problem"""


class AirflowSensorTimeout(AirflowException):
    """Raise when there is a timeout on sensor polling"""


class AirflowRescheduleException(AirflowException):
    """
    Raise when the task should be re-scheduled at a later time.

    :param reschedule_date: The date when the task should be rescheduled
    :type reschedule_date: datetime.datetime
    """

    def __init__(self, reschedule_date):
        super().__init__()
        self.reschedule_date = reschedule_date


class AirflowSmartSensorException(AirflowException):
    """
    Raise after the task register itself in the smart sensor service
    It should exit without failing a task
    """


class InvalidStatsNameException(AirflowException):
    """Raise when name of the stats is invalid"""


class AirflowTaskTimeout(AirflowException):
    """Raise when the task execution times-out"""


class AirflowWebServerTimeout(AirflowException):
    """Raise when the web server times out"""


class AirflowSkipException(AirflowException):
    """Raise when the task should be skipped"""


class AirflowFailException(AirflowException):
    """Raise when the task should be failed without retrying"""


class AirflowDagCycleException(AirflowException):
    """Raise when there is a cycle in Dag definition"""


class AirflowDagDuplicatedIdException(AirflowException):
    """Raise when a Dag's ID is already used by another Dag"""

    def __init__(self, dag_id: str, incoming: str, existing: str) -> None:
        super().__init__(dag_id, incoming, existing)
        self.dag_id = dag_id
        self.incoming = incoming
        self.existing = existing

    def __str__(self) -> str:
        return f"Ignoring DAG {self.dag_id} from {self.incoming} - also found in {self.existing}"


class AirflowClusterPolicyViolation(AirflowException):
    """Raise when there is a violation of a Cluster Policy in Dag definition"""


class DagNotFound(AirflowNotFoundException):
    """Raise when a DAG is not available in the system"""


class DagCodeNotFound(AirflowNotFoundException):
    """Raise when a DAG code is not available in the system"""


class DagRunNotFound(AirflowNotFoundException):
    """Raise when a DAG Run is not available in the system"""


class DagRunAlreadyExists(AirflowBadRequest):
    """Raise when creating a DAG run for DAG which already has DAG run entry"""


class DagFileExists(AirflowBadRequest):
    """Raise when a DAG ID is still in DagBag i.e., DAG file is in DAG folder"""


class DuplicateTaskIdFound(AirflowException):
    """Raise when a Task with duplicate task_id is defined in the same DAG"""


class SerializedDagNotFound(DagNotFound):
    """Raise when DAG is not found in the serialized_dags table in DB"""


class SerializationError(AirflowException):
    """A problem occurred when trying to serialize a DAG"""


class TaskNotFound(AirflowNotFoundException):
    """Raise when a Task is not available in the system"""


class TaskInstanceNotFound(AirflowNotFoundException):
    """Raise when a Task Instance is not available in the system"""


class PoolNotFound(AirflowNotFoundException):
    """Raise when a Pool is not available in the system"""


class NoAvailablePoolSlot(AirflowException):
    """Raise when there is not enough slots in pool"""


class DagConcurrencyLimitReached(AirflowException):
    """Raise when DAG concurrency limit is reached"""


class TaskConcurrencyLimitReached(AirflowException):
    """Raise when task concurrency limit is reached"""


class BackfillUnfinished(AirflowException):
    """
    Raises when not all tasks succeed in backfill.

    :param message: The human-readable description of the exception
    :param ti_status: The information about all task statuses
    """

    def __init__(self, message, ti_status):
        super().__init__(message)
        self.ti_status = ti_status


class FileSyntaxError(NamedTuple):
    """Information about a single error in a file."""

    line_no: Optional[int]
    message: str

    def __str__(self):
        return f"{self.message}. Line number: s{str(self.line_no)},"


class AirflowFileParseException(AirflowException):
    """
    Raises when connection or variable file can not be parsed

    :param msg: The human-readable description of the exception
    :param file_path: A processed file that contains errors
    :param parse_errors: File syntax errors
    """

    def __init__(self, msg: str, file_path: str, parse_errors: List[FileSyntaxError]) -> None:
        super().__init__(msg)
        self.msg = msg
        self.file_path = file_path
        self.parse_errors = parse_errors

    def __str__(self):
        result = f"{self.msg}\nFilename: {self.file_path}\n\n"

        for error_no, parse_error in enumerate(self.parse_errors, 1):
            result += "=" * 20 + f" Parse error {error_no:3} " + "=" * 20 + "\n"
            result += f"{parse_error.message}\n"
            if parse_error.line_no:
                result += f"Line number:  {parse_error.line_no}\n"
                if parse_error.line_no and is_tty():
                    result += "\n" + prepare_code_snippet(self.file_path, parse_error.line_no) + "\n"

        return result


class ConnectionNotUnique(AirflowException):
    """Raise when multiple values are found for the same conn_id"""


class TaskDeferred(BaseException):
    """
    Special exception raised to signal that the operator it was raised from
    wishes to defer until a trigger fires.
    """

    def __init__(
        self,
        *,
        trigger,
        method_name: str,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[datetime.timedelta] = None,
    ):
        super().__init__()
        self.trigger = trigger
        self.method_name = method_name
        self.kwargs = kwargs
        self.timeout = timeout
        # Check timeout type at runtime
        if self.timeout is not None and not hasattr(self.timeout, "total_seconds"):
            raise ValueError("Timeout value must be a timedelta")

    def __repr__(self) -> str:
        return f"<OperatorDeferred trigger={self.trigger} method={self.method_name}>"
