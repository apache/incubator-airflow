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

"""Interact with AWS DataSync, using the AWS ``boto3`` library."""

import time
from typing import List, Optional

from airflow.exceptions import AirflowBadRequest, AirflowException, AirflowTaskTimeout
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AWSDataSyncHook(AwsBaseHook):
    """
    Interact with AWS DataSync.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        :class:`~airflow.providers.amazon.aws.operators.datasync.AWSDataSyncOperator`

    :param wait_interval_seconds: Time to wait between two
        consecutive calls to check TaskExecution status. Defaults to 5 seconds.
    :type wait_interval_seconds: Optional[int]
    :raises ValueError: If wait_interval_seconds is not between 0 and 15*60 seconds.
    """

    TASK_EXECUTION_INTERMEDIATE_STATES = (
        "INITIALIZING",
        "QUEUED",
        "LAUNCHING",
        "PREPARING",
        "TRANSFERRING",
        "VERIFYING",
    )
    TASK_EXECUTION_FAILURE_STATES = ("ERROR",)
    TASK_EXECUTION_SUCCESS_STATES = ("SUCCESS",)

    def __init__(self, wait_interval_seconds: int = 5, *args, **kwargs) -> None:
        super().__init__(client_type='datasync', *args, **kwargs)  # type: ignore[misc]
        self.locations: list = []
        self.tasks: list = []
        # wait_interval_seconds = 0 is used during unit tests
        if wait_interval_seconds < 0 or wait_interval_seconds > 15 * 60:
            raise ValueError(f"Invalid wait_interval_seconds {wait_interval_seconds}")
        self.wait_interval_seconds = wait_interval_seconds

    def create_location(self, location_uri: str, **create_location_kwargs) -> str:
        r"""Creates a new location.

        :param str location_uri: Location URI used to determine the location type (S3, SMB, NFS, EFS).
        :param create_location_kwargs: Passed to ``boto.create_location_xyz()``.
            See AWS boto3 datasync documentation.
        :return str: LocationArn of the created Location.
        :raises AirflowException: If location type (prefix from ``location_uri``) is invalid.
        """
        typ = location_uri.split(":")[0]
        if typ == "smb":
            location = self.get_conn().create_location_smb(**create_location_kwargs)
        elif typ == "s3":
            location = self.get_conn().create_location_s3(**create_location_kwargs)
        elif typ == "nfs":
            location = self.get_conn().create_loction_nfs(**create_location_kwargs)
        elif typ == "efs":
            location = self.get_conn().create_loction_efs(**create_location_kwargs)
        else:
            raise AirflowException(f"Invalid location type: {typ}")
        self._refresh_locations()
        return location["LocationArn"]

    def get_location_arns(
        self, location_uri: str, case_sensitive: bool = False, ignore_trailing_slash: bool = True
    ) -> List[str]:
        """
        Return all LocationArns which match a LocationUri.

        :param str location_uri: Location URI to search for, eg ``s3://mybucket/mypath``
        :param bool case_sensitive: Do a case sensitive search for location URI.
        :param bool ignore_trailing_slash: Ignore / at the end of URI when matching.
        :return: List of LocationArns.
        :rtype: list(str)
        :raises AirflowBadRequest: if ``location_uri`` is empty
        """
        if not location_uri:
            raise AirflowBadRequest("location_uri not specified")
        if not self.locations:
            self._refresh_locations()
        result = []

        if not case_sensitive:
            location_uri = location_uri.lower()
        if ignore_trailing_slash and location_uri.endswith("/"):
            location_uri = location_uri[:-1]

        for location_from_aws in self.locations:
            location_uri_from_aws = location_from_aws["LocationUri"]
            if not case_sensitive:
                location_uri_from_aws = location_uri_from_aws.lower()
            if ignore_trailing_slash and location_uri_from_aws.endswith("/"):
                location_uri_from_aws = location_uri_from_aws[:-1]
            if location_uri == location_uri_from_aws:
                result.append(location_from_aws["LocationArn"])
        return result

    def _refresh_locations(self) -> None:
        """Refresh the local list of Locations."""
        self.locations = []
        next_token = None
        while True:
            if next_token:
                locations = self.get_conn().list_locations(NextToken=next_token)
            else:
                locations = self.get_conn().list_locations()
            self.locations.extend(locations["Locations"])
            if "NextToken" not in locations:
                break
            next_token = locations["NextToken"]

    def create_task(
        self, source_location_arn: str, destination_location_arn: str, **create_task_kwargs
    ) -> str:
        r"""Create a Task between the specified source and destination LocationArns.

        :param str source_location_arn: Source LocationArn. Must exist already.
        :param str destination_location_arn: Destination LocationArn. Must exist already.
        :param create_task_kwargs: Passed to ``boto.create_task()``. See AWS boto3 datasync documentation.
        :return: TaskArn of the created Task
        :rtype: str
        """
        task = self.get_conn().create_task(
            SourceLocationArn=source_location_arn,
            DestinationLocationArn=destination_location_arn,
            **create_task_kwargs,
        )
        self._refresh_tasks()
        return task["TaskArn"]

    def update_task(self, task_arn: str, **update_task_kwargs) -> None:
        r"""Update a Task.

        :param str task_arn: The TaskArn to update.
        :param update_task_kwargs: Passed to ``boto.update_task()``, See AWS boto3 datasync documentation.
        """
        self.get_conn().update_task(TaskArn=task_arn, **update_task_kwargs)

    def delete_task(self, task_arn: str) -> None:
        r"""Delete a Task.

        :param str task_arn: The TaskArn to delete.
        """
        self.get_conn().delete_task(TaskArn=task_arn)

    def _refresh_tasks(self) -> None:
        """Refreshes the local list of Tasks"""
        self.tasks = []
        next_token = None
        while True:
            if next_token:
                tasks = self.get_conn().list_tasks(NextToken=next_token)
            else:
                tasks = self.get_conn().list_tasks()
            self.tasks.extend(tasks["Tasks"])
            if "NextToken" not in tasks:
                break
            next_token = tasks["NextToken"]

    def get_task_arns_for_location_arns(
        self,
        source_location_arns: list,
        destination_location_arns: list,
    ) -> list:
        """
        Return list of TaskArns for which use any one of the specified
        source LocationArns and any one of the specified destination LocationArns.

        :param list source_location_arns: List of source LocationArns.
        :param list destination_location_arns: List of destination LocationArns.
        :return: list
        :rtype: list(TaskArns)
        :raises AirflowBadRequest: if ``source_location_arns`` or ``destination_location_arns`` are empty.
        """
        if not source_location_arns:
            raise AirflowBadRequest("source_location_arns not specified")
        if not destination_location_arns:
            raise AirflowBadRequest("destination_location_arns not specified")
        if not self.tasks:
            self._refresh_tasks()

        result = []
        for task in self.tasks:
            task_arn = task["TaskArn"]
            task_description = self.get_task_description(task_arn)
            if task_description["SourceLocationArn"] in source_location_arns:
                if task_description["DestinationLocationArn"] in destination_location_arns:
                    result.append(task_arn)
        return result

    def start_task_execution(self, task_arn: str, **kwargs) -> str:
        r"""
        Start a TaskExecution for the specified task_arn.
        Each task can have at most one TaskExecution.

        :param str task_arn: TaskArn
        :return: TaskExecutionArn
        :param kwargs: kwargs sent to ``boto3.start_task_execution()``
        :rtype: str
        :raises ClientError: If a TaskExecution is already busy running for this ``task_arn``.
        :raises AirflowBadRequest: If ``task_arn`` is empty.
        """
        if not task_arn:
            raise AirflowBadRequest("task_arn not specified")
        task_execution = self.get_conn().start_task_execution(TaskArn=task_arn, **kwargs)
        return task_execution["TaskExecutionArn"]

    def cancel_task_execution(self, task_execution_arn: str) -> None:
        """
        Cancel a TaskExecution for the specified ``task_execution_arn``.

        :param str task_execution_arn: TaskExecutionArn.
        :raises AirflowBadRequest: If ``task_execution_arn`` is empty.
        """
        if not task_execution_arn:
            raise AirflowBadRequest("task_execution_arn not specified")
        self.get_conn().cancel_task_execution(TaskExecutionArn=task_execution_arn)

    def get_task_description(self, task_arn: str) -> dict:
        """
        Get description for the specified ``task_arn``.

        :param str task_arn: TaskArn
        :return: AWS metadata about a task.
        :rtype: dict
        :raises AirflowBadRequest: If ``task_arn`` is empty.
        """
        if not task_arn:
            raise AirflowBadRequest("task_arn not specified")
        return self.get_conn().describe_task(TaskArn=task_arn)

    def describe_task_execution(self, task_execution_arn: str) -> dict:
        """
        Get description for the specified ``task_execution_arn``.

        :param str task_execution_arn: TaskExecutionArn
        :return: AWS metadata about a task execution.
        :rtype: dict
        :raises AirflowBadRequest: If ``task_execution_arn`` is empty.
        """
        return self.get_conn().describe_task_execution(TaskExecutionArn=task_execution_arn)

    def get_current_task_execution_arn(self, task_arn: str) -> Optional[str]:
        """
        Get current TaskExecutionArn (if one exists) for the specified ``task_arn``.

        :param str task_arn: TaskArn
        :return: CurrentTaskExecutionArn for this ``task_arn`` or None.
        :rtype: str
        :raises AirflowBadRequest: if ``task_arn`` is empty.
        """
        if not task_arn:
            raise AirflowBadRequest("task_arn not specified")
        task_description = self.get_task_description(task_arn)
        if "CurrentTaskExecutionArn" in task_description:
            return task_description["CurrentTaskExecutionArn"]
        return None

    def wait_for_task_execution(self, task_execution_arn: str, max_iterations: int = 2 * 180) -> bool:
        """
        Wait for Task Execution status to be complete (SUCCESS/ERROR).
        The ``task_execution_arn`` must exist, or a boto3 ClientError will be raised.

        :param str task_execution_arn: TaskExecutionArn
        :param int max_iterations: Maximum number of iterations before timing out.
        :return: Result of task execution.
        :rtype: bool
        :raises AirflowTaskTimeout: If maximum iterations is exceeded.
        :raises AirflowBadRequest: If ``task_execution_arn`` is empty.
        """
        if not task_execution_arn:
            raise AirflowBadRequest("task_execution_arn not specified")

        status = None
        iterations = max_iterations
        while status is None or status in self.TASK_EXECUTION_INTERMEDIATE_STATES:
            task_execution = self.get_conn().describe_task_execution(TaskExecutionArn=task_execution_arn)
            status = task_execution["Status"]
            self.log.info("status=%s", status)
            iterations -= 1
            if status in self.TASK_EXECUTION_FAILURE_STATES:
                break
            if status in self.TASK_EXECUTION_SUCCESS_STATES:
                break
            if iterations <= 0:
                break
            time.sleep(self.wait_interval_seconds)

        if status in self.TASK_EXECUTION_SUCCESS_STATES:
            return True
        if status in self.TASK_EXECUTION_FAILURE_STATES:
            return False
        if iterations <= 0:
            raise AirflowTaskTimeout("Max iterations exceeded!")
        raise AirflowException(f"Unknown status: {status}")  # Should never happen
