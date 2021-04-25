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

"""Create, get, update, execute and delete an AWS DataSync Task."""

import logging
import random
from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.datasync import AWSDataSyncHook
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes, too-many-arguments
class AWSDataSyncOperator(BaseOperator):
    r"""Find, Create, Update, Execute and Delete AWS DataSync Tasks.

    If ``do_xcom_push`` is True, then the DataSync TaskArn and TaskExecutionArn
    which were executed will be pushed to an XCom.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AWSDataSyncOperator`

    .. note:: There may be 0, 1, or many existing DataSync Tasks defined in your AWS
        environment. The default behavior is to create a new Task if there are 0, or
        execute the Task if there was 1 Task, or fail if there were many Tasks.

    :param aws_conn_id: AWS connection to use.
    :type aws_conn_id: str
    :param wait_interval_seconds: Time to wait between two
        consecutive calls to check TaskExecution status.
    :type wait_interval_seconds: int
    :param task_arn: AWS DataSync TaskArn to use. If None, then this operator will
        attempt to either search for an existing Task or attempt to create a new Task.
    :type task_arn: str
    :param source_location_uri: Source location URI to search for. All DataSync
        Tasks with a LocationArn with this URI will be considered.
        Example: ``smb://server/subdir``
    :type source_location_uri: str
    :param destination_location_uri: Destination location URI to search for.
        All DataSync Tasks with a LocationArn with this URI will be considered.
        Example: ``s3://airflow_bucket/stuff``
    :type destination_location_uri: str
    :param allow_random_task_choice: If multiple Tasks match, one must be chosen to
        execute. If allow_random_task_choice is True then a random one is chosen.
    :type allow_random_task_choice: bool
    :param allow_random_location_choice: If multiple Locations match, one must be chosen
        when creating a task. If allow_random_location_choice is True then a random one is chosen.
    :type allow_random_location_choice: bool
    :param create_task_kwargs: If no suitable TaskArn is identified,
        it will be created if ``create_task_kwargs`` is defined.
        ``create_task_kwargs`` is then used internally like this:
        ``boto3.create_task(**create_task_kwargs)``
        Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ..., 'Tags': ...}``
    :type create_task_kwargs: dict
    :param create_source_location_kwargs: If no suitable LocationArn is found,
        a Location will be created if ``create_source_location_kwargs`` is defined.
        ``create_source_location_kwargs`` is then used internally like this:
        ``boto3.create_location_xyz(**create_source_location_kwargs)``
        The xyz is determined from the prefix of source_location_uri, eg ``smb:/...`` or ``s3:/...``
        Example:  ``{'Subdirectory': ..., 'ServerHostname': ..., ...}``
    :type create_source_location_kwargs: dict
    :param create_destination_location_kwargs: If no suitable LocationArn is found,
        a Location will be created if ``create_destination_location_kwargs`` is defined.
        ``create_destination_location_kwargs`` is used internally like this:
        ``boto3.create_location_xyz(**create_destination_location_kwargs)``
        The xyz is determined from the prefix of destination_location_uri, eg ``smb:/...` or ``s3:/...``
        Example:  ``{'S3BucketArn': ..., 'S3Config': {'BucketAccessRoleArn': ...}, ...}``
    :type create_destination_location_kwargs: dict
    :param update_task_kwargs:  If a suitable TaskArn is found or created,
        it will be updated if ``update_task_kwargs`` is defined.
        ``update_task_kwargs`` is used internally like this:
        ``boto3.update_task(TaskArn=task_arn, **update_task_kwargs)``
        Example:  ``{'Name': 'xyz', 'Options': ..., 'Excludes': ...}``
    :type update_task_kwargs: dict
    :param task_execution_kwargs: Additional kwargs passed directly when starting the
        Task execution, used internally like this:
        ``boto3.start_task_execution(TaskArn=task_arn, **task_execution_kwargs)``
    :type task_execution_kwargs: dict
    :param  delete_task_after_execution: If True then the TaskArn which was executed
        will be deleted from AWS DataSync on successful completion.
    :type delete_task_after_execution: bool
    :raises AirflowException: If ``task_arn`` was not specified, or if
        either ``source_location_uri`` or ``destination_location_uri`` were
        not specified.
    :raises AirflowException: If source or destination Location were not found
        and could not be created.
    :raises AirflowException: If ``choose_task`` or ``choose_location`` fails.
    :raises AirflowException: If Task creation, update, execution or delete fails.
    """

    template_fields = (
        "task_arn",
        "source_location_uri",
        "destination_location_uri",
        "create_task_kwargs",
        "create_source_location_kwargs",
        "create_destination_location_kwargs",
        "update_task_kwargs",
        "task_execution_kwargs",
    )
    ui_color = "#44b5e2"

    @apply_defaults
    def __init__(
        self,
        *,
        aws_conn_id: str = "aws_default",
        wait_interval_seconds: int = 5,
        task_arn: Optional[str] = None,
        source_location_uri: Optional[str] = None,
        destination_location_uri: Optional[str] = None,
        allow_random_task_choice: bool = False,
        allow_random_location_choice: bool = False,
        create_task_kwargs: Optional[dict] = None,
        create_source_location_kwargs: Optional[dict] = None,
        create_destination_location_kwargs: Optional[dict] = None,
        update_task_kwargs: Optional[dict] = None,
        task_execution_kwargs: Optional[dict] = None,
        delete_task_after_execution: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Assignments
        self.aws_conn_id = aws_conn_id
        self.wait_interval_seconds = wait_interval_seconds

        self.task_arn = task_arn

        self.source_location_uri = source_location_uri
        self.destination_location_uri = destination_location_uri
        self.allow_random_task_choice = allow_random_task_choice
        self.allow_random_location_choice = allow_random_location_choice

        self.create_task_kwargs = create_task_kwargs if create_task_kwargs else {}
        self.create_source_location_kwargs = {}
        if create_source_location_kwargs:
            self.create_source_location_kwargs = create_source_location_kwargs
        self.create_destination_location_kwargs = {}
        if create_destination_location_kwargs:
            self.create_destination_location_kwargs = create_destination_location_kwargs

        self.update_task_kwargs = update_task_kwargs if update_task_kwargs else {}
        self.task_execution_kwargs = task_execution_kwargs if task_execution_kwargs else {}
        self.delete_task_after_execution = delete_task_after_execution

        # Validations
        valid = False
        if self.task_arn:
            valid = True
        if self.source_location_uri and self.destination_location_uri:
            valid = True
        if not valid:
            raise AirflowException(
                "Either specify task_arn or both source_location_uri and destination_location_uri. "
                "task_arn={} source_location_uri={} destination_location_uri={}".format(
                    task_arn, source_location_uri, destination_location_uri
                )
            )

        # Others
        self.hook: Optional[AWSDataSyncHook] = None
        # Candidates - these are found in AWS as possible things
        # for us to use
        self.candidate_source_location_arns: Optional[List[str]] = None
        self.candidate_destination_location_arns: Optional[List[str]] = None
        self.candidate_task_arns: Optional[List[str]] = None
        # Actuals
        self.source_location_arn: Optional[str] = None
        self.destination_location_arn: Optional[str] = None
        self.task_execution_arn: Optional[str] = None

    def get_hook(self) -> AWSDataSyncHook:
        """Create and return AWSDataSyncHook.

        :return AWSDataSyncHook: An AWSDataSyncHook instance.
        """
        if self.hook:
            return self.hook

        self.hook = AWSDataSyncHook(
            aws_conn_id=self.aws_conn_id,
            wait_interval_seconds=self.wait_interval_seconds,
        )
        return self.hook

    def execute(self, context):
        # If task_arn was not specified then try to
        # find 0, 1 or many candidate DataSync Tasks to run
        if not self.task_arn:
            self._get_tasks_and_locations()

        # If some were found, identify which one to run
        if self.candidate_task_arns:
            self.task_arn = self.choose_task(self.candidate_task_arns)

        # If we could not find one then try to create one
        if not self.task_arn and self.create_task_kwargs:
            self._create_datasync_task()

        if not self.task_arn:
            raise AirflowException("DataSync TaskArn could not be identified or created.")

        self.log.info("Using DataSync TaskArn %s", self.task_arn)

        # Update the DataSync Task
        if self.update_task_kwargs:
            self._update_datasync_task()

        # Execute the DataSync Task
        self._execute_datasync_task()

        if not self.task_execution_arn:
            raise AirflowException("Nothing was executed")

        # Delete the DataSyncTask
        if self.delete_task_after_execution:
            self._delete_datasync_task()

        return {"TaskArn": self.task_arn, "TaskExecutionArn": self.task_execution_arn}

    def _get_tasks_and_locations(self) -> None:
        """Find existing DataSync Task based on source and dest Locations."""
        hook = self.get_hook()

        self.candidate_source_location_arns = self._get_location_arns(self.source_location_uri)

        self.candidate_destination_location_arns = self._get_location_arns(self.destination_location_uri)

        if not self.candidate_source_location_arns:
            self.log.info("No matching source Locations")
            return

        if not self.candidate_destination_location_arns:
            self.log.info("No matching destination Locations")
            return

        self.log.info("Finding DataSync TaskArns that have these LocationArns")
        self.candidate_task_arns = hook.get_task_arns_for_location_arns(
            self.candidate_source_location_arns,
            self.candidate_destination_location_arns,
        )
        self.log.info("Found candidate DataSync TaskArns %s", self.candidate_task_arns)

    def choose_task(self, task_arn_list: list) -> Optional[str]:
        """Select 1 DataSync TaskArn from a list"""
        if not task_arn_list:
            return None
        if len(task_arn_list) == 1:
            return task_arn_list[0]
        if self.allow_random_task_choice:
            # Items are unordered so we don't want to just take
            # the [0] one as it implies ordered items were received
            # from AWS and might lead to confusion. Rather explicitly
            # choose a random one
            return random.choice(task_arn_list)
        raise AirflowException(f"Unable to choose a Task from {task_arn_list}")

    def choose_location(self, location_arn_list: Optional[List[str]]) -> Optional[str]:
        """Select 1 DataSync LocationArn from a list"""
        if not location_arn_list:
            return None
        if len(location_arn_list) == 1:
            return location_arn_list[0]
        if self.allow_random_location_choice:
            # Items are unordered so we don't want to just take
            # the [0] one as it implies ordered items were received
            # from AWS and might lead to confusion. Rather explicitly
            # choose a random one
            return random.choice(location_arn_list)
        raise AirflowException(f"Unable to choose a Location from {location_arn_list}")

    def _create_datasync_task(self) -> None:
        """Create a AWS DataSyncTask."""
        hook = self.get_hook()

        self.source_location_arn = self.choose_location(self.candidate_source_location_arns)
        if not self.source_location_arn and self.source_location_uri and self.create_source_location_kwargs:
            self.log.info('Attempting to create source Location')
            self.source_location_arn = hook.create_location(
                self.source_location_uri, **self.create_source_location_kwargs
            )
        if not self.source_location_arn:
            raise AirflowException(
                "Unable to determine source LocationArn. Does a suitable DataSync Location exist?"
            )

        self.destination_location_arn = self.choose_location(self.candidate_destination_location_arns)
        if (
            not self.destination_location_arn
            and self.destination_location_uri
            and self.create_destination_location_kwargs
        ):
            self.log.info('Attempting to create destination Location')
            self.destination_location_arn = hook.create_location(
                self.destination_location_uri, **self.create_destination_location_kwargs
            )
        if not self.destination_location_arn:
            raise AirflowException(
                "Unable to determine destination LocationArn. Does a suitable DataSync Location exist?"
            )

        self.log.info("Creating a Task.")
        self.task_arn = hook.create_task(
            self.source_location_arn, self.destination_location_arn, **self.create_task_kwargs
        )
        if not self.task_arn:
            raise AirflowException("Task could not be created")
        self.log.info("Created a Task with TaskArn %s", self.task_arn)

    def _update_datasync_task(self) -> None:
        """Update a AWS DataSyncTask."""
        if not self.task_arn:
            return

        hook = self.get_hook()
        self.log.info("Updating TaskArn %s", self.task_arn)
        hook.update_task(self.task_arn, **self.update_task_kwargs)
        self.log.info("Updated TaskArn %s", self.task_arn)

    def _execute_datasync_task(self) -> None:
        """Create and monitor an AWSDataSync TaskExecution for a Task."""
        if not self.task_arn:
            raise AirflowException("Missing TaskArn")

        hook = self.get_hook()

        # Create a task execution:
        self.log.info("Starting execution for TaskArn %s", self.task_arn)
        self.task_execution_arn = hook.start_task_execution(self.task_arn, **self.task_execution_kwargs)
        self.log.info("Started TaskExecutionArn %s", self.task_execution_arn)

        # Wait for task execution to complete
        self.log.info("Waiting for TaskExecutionArn %s", self.task_execution_arn)
        result = hook.wait_for_task_execution(self.task_execution_arn)
        self.log.info("Completed TaskExecutionArn %s", self.task_execution_arn)
        task_execution_description = hook.describe_task_execution(task_execution_arn=self.task_execution_arn)
        self.log.info("task_execution_description=%s", task_execution_description)

        # Log some meaningful statuses
        level = logging.ERROR if not result else logging.INFO
        self.log.log(level, 'Status=%s', task_execution_description['Status'])
        if 'Result' in task_execution_description:
            for k, v in task_execution_description['Result'].items():
                if 'Status' in k or 'Error' in k:
                    self.log.log(level, '%s=%s', k, v)

        if not result:
            raise AirflowException(f"Failed TaskExecutionArn {self.task_execution_arn}")

    def on_kill(self) -> None:
        """Cancel the submitted DataSync task."""
        hook = self.get_hook()
        if self.task_execution_arn:
            self.log.info("Cancelling TaskExecutionArn %s", self.task_execution_arn)
            hook.cancel_task_execution(task_execution_arn=self.task_execution_arn)
            self.log.info("Cancelled TaskExecutionArn %s", self.task_execution_arn)

    def _delete_datasync_task(self) -> None:
        """Deletes an AWS DataSync Task."""
        if not self.task_arn:
            return

        hook = self.get_hook()
        # Delete task:
        self.log.info("Deleting Task with TaskArn %s", self.task_arn)
        hook.delete_task(self.task_arn)
        self.log.info("Task Deleted")

    def _get_location_arns(self, location_uri) -> List[str]:
        location_arns = self.get_hook().get_location_arns(location_uri)
        self.log.info("Found LocationArns %s for LocationUri %s", location_arns, location_uri)
        return location_arns
