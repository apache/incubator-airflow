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

"""
An Airflow operator for AWS Batch services

.. seealso::

    - http://boto3.readthedocs.io/en/latest/guide/configuration.html
    - http://boto3.readthedocs.io/en/latest/reference/services/batch.html
    - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.batch_client import AwsBatchClientHook


class AwsBatchOperator(BaseOperator):
    """
    Execute a job on AWS Batch

    :param job_name: the name for the job that will run on AWS Batch (templated)
    :type job_name: str

    :param job_definition: the job definition name on AWS Batch
    :type job_definition: str

    :param job_queue: the queue name on AWS Batch
    :type job_queue: str

    :param overrides: the `containerOverrides` parameter for boto3 (templated)
    :type overrides: Optional[dict]

    :param array_properties: the `arrayProperties` parameter for boto3
    :type array_properties: Optional[dict]

    :param parameters: the `parameters` for boto3 (templated)
    :type parameters: Optional[dict]

    :param job_id: the job ID, usually unknown (None) until the
        submit_job operation gets the jobId defined by AWS Batch
    :type job_id: Optional[str]

    :param waiters: an :py:class:`.AwsBatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :type waiters: Optional[AwsBatchWaiters]

    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :type max_retries: int

    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :type status_retries: int

    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :type aws_conn_id: str

    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str

    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted
    :type tags: dict

    .. note::
        Any custom waiters must return a waiter for these calls:
        .. code-block:: python

            waiter = waiters.get_waiter("JobExists")
            waiter = waiters.get_waiter("JobRunning")
            waiter = waiters.get_waiter("JobComplete")
    """

    ui_color = "#c3dae0"
    arn = None  # type: Optional[str]
    template_fields = (
        "job_name",
        "overrides",
        "parameters",
    )
    template_fields_renderers = {"overrides": "py", "parameters": "py"}

    def __init__(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_queue: str,
        overrides: dict,
        array_properties: Optional[dict] = None,
        parameters: Optional[dict] = None,
        job_id: Optional[str] = None,
        waiters: Optional[Any] = None,
        max_retries: Optional[int] = None,
        status_retries: Optional[int] = None,
        aws_conn_id: Optional[str] = None,
        region_name: Optional[str] = None,
        tags: Optional[dict] = None,
        **kwargs,
    ):  # pylint: disable=too-many-arguments

        BaseOperator.__init__(self, **kwargs)
        self.job_id = job_id
        self.job_name = job_name
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.overrides = overrides or {}
        self.array_properties = array_properties or {}
        self.parameters = parameters or {}
        self.waiters = waiters
        self.tags = tags or {}
        self.hook = AwsBatchClientHook(
            max_retries=max_retries,
            status_retries=status_retries,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def execute(self, context: Dict):
        """
        Submit and monitor an AWS Batch job

        :raises: AirflowException
        """
        self.submit_job(context)
        self.monitor_job(context)

    def on_kill(self):
        response = self.hook.client.terminate_job(jobId=self.job_id, reason="Task killed by the user")
        self.log.info("AWS Batch job (%s) terminated: %s", self.job_id, response)

    def submit_job(self, context: Dict):  # pylint: disable=unused-argument
        """
        Submit an AWS Batch job

        :raises: AirflowException
        """
        self.log.info(
            "Running AWS Batch job - job definition: %s - on queue %s",
            self.job_definition,
            self.job_queue,
        )
        self.log.info("AWS Batch job - container overrides: %s", self.overrides)

        try:
            response = self.hook.client.submit_job(
                jobName=self.job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                arrayProperties=self.array_properties,
                parameters=self.parameters,
                containerOverrides=self.overrides,
                tags=self.tags,
            )
            self.job_id = response["jobId"]

            self.log.info("AWS Batch job (%s) started: %s", self.job_id, response)
        except Exception as e:
            self.log.error("AWS Batch job (%s) failed submission", self.job_id)
            raise AirflowException(e)

    def monitor_job(self, context: Dict):  # pylint: disable=unused-argument
        """
        Monitor an AWS Batch job
        monitor_job can raise an exception or an AirflowTaskTimeout can be raised if execution_timeout
        is given while creating the task. These exceptions should be handled in taskinstance.py
        instead of here like it was previously done

        :raises: AirflowException
        """
        if not self.job_id:
            raise AirflowException('AWS Batch job - job_id was not found')

        if self.waiters:
            self.waiters.wait_for_job(self.job_id)
        else:
            self.hook.wait_for_job(self.job_id)

        self.hook.check_job_success(self.job_id)
        self.log.info("AWS Batch job (%s) succeeded", self.job_id)
