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
"""This module contains a Google Dataflow Hook."""
import functools
import json
import re
import shlex
import subprocess
import time
import uuid
import warnings
from copy import deepcopy
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Set, TypeVar, Union, cast

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType, beam_options_to_args
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timeout import timeout

# This is the default location
# https://cloud.google.com/dataflow/pipelines/specifying-exec-params
DEFAULT_DATAFLOW_LOCATION = "us-central1"


JOB_ID_PATTERN = re.compile(
    r"Submitted job: (?P<job_id_java>.*)|Created job with id: \[(?P<job_id_python>.*)\]"
)

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def process_line_and_extract_dataflow_job_id_callback(
    on_new_job_id_callback: Optional[Callable[[str], None]]
) -> Callable[[str], None]:
    """
    Returns callback which triggers function passed as `on_new_job_id_callback` when Dataflow job_id is found.
    To be used for `process_line_callback` in
    :py:class:`~airflow.providers.apache.beam.hooks.beam.BeamCommandRunner`

    :param on_new_job_id_callback: Callback called when the job ID is known
    :type on_new_job_id_callback: callback
    """

    def _process_line_and_extract_job_id(
        line: str,
        # on_new_job_id_callback: Optional[Callable[[str], None]]
    ) -> None:
        # Job id info: https://goo.gl/SE29y9.
        matched_job = JOB_ID_PATTERN.search(line)
        if matched_job:
            job_id = matched_job.group("job_id_java") or matched_job.group("job_id_python")
            if on_new_job_id_callback:
                on_new_job_id_callback(job_id)

    def wrap(line: str):
        return _process_line_and_extract_job_id(line)

    return wrap


def _fallback_variable_parameter(parameter_name: str, variable_key_name: str) -> Callable[[T], T]:
    def _wrapper(func: T) -> T:
        """
        Decorator that provides fallback for location from `region` key in `variables` parameters.

        :param func: function to wrap
        :return: result of the function call
        """

        @functools.wraps(func)
        def inner_wrapper(self: "DataflowHook", *args, **kwargs):
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )

            parameter_location = kwargs.get(parameter_name)
            variables_location = kwargs.get("variables", {}).get(variable_key_name)

            if parameter_location and variables_location:
                raise AirflowException(
                    f"The mutually exclusive parameter `{parameter_name}` and `{variable_key_name}` key "
                    f"in `variables` parameter are both present. Please remove one."
                )
            if parameter_location or variables_location:
                kwargs[parameter_name] = parameter_location or variables_location
            if variables_location:
                copy_variables = deepcopy(kwargs["variables"])
                del copy_variables[variable_key_name]
                kwargs["variables"] = copy_variables

            return func(self, *args, **kwargs)

        return cast(T, inner_wrapper)

    return _wrapper


_fallback_to_location_from_variables = _fallback_variable_parameter("location", "region")
_fallback_to_project_id_from_variables = _fallback_variable_parameter("project_id", "project")


class DataflowJobStatus:
    """
    Helper class with Dataflow job statuses.
    Reference: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    """

    JOB_STATE_DONE = "JOB_STATE_DONE"
    JOB_STATE_UNKNOWN = "JOB_STATE_UNKNOWN"
    JOB_STATE_STOPPED = "JOB_STATE_STOPPED"
    JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
    JOB_STATE_FAILED = "JOB_STATE_FAILED"
    JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
    JOB_STATE_UPDATED = "JOB_STATE_UPDATED"
    JOB_STATE_DRAINING = "JOB_STATE_DRAINING"
    JOB_STATE_DRAINED = "JOB_STATE_DRAINED"
    JOB_STATE_PENDING = "JOB_STATE_PENDING"
    JOB_STATE_CANCELLING = "JOB_STATE_CANCELLING"
    JOB_STATE_QUEUED = "JOB_STATE_QUEUED"
    FAILED_END_STATES = {JOB_STATE_FAILED, JOB_STATE_CANCELLED}
    SUCCEEDED_END_STATES = {JOB_STATE_DONE, JOB_STATE_UPDATED, JOB_STATE_DRAINED}
    TERMINAL_STATES = SUCCEEDED_END_STATES | FAILED_END_STATES
    AWAITING_STATES = {
        JOB_STATE_RUNNING,
        JOB_STATE_PENDING,
        JOB_STATE_QUEUED,
        JOB_STATE_CANCELLING,
        JOB_STATE_DRAINING,
        JOB_STATE_STOPPED,
    }


class DataflowJobType:
    """Helper class with Dataflow job types."""

    JOB_TYPE_UNKNOWN = "JOB_TYPE_UNKNOWN"
    JOB_TYPE_BATCH = "JOB_TYPE_BATCH"
    JOB_TYPE_STREAMING = "JOB_TYPE_STREAMING"


class _DataflowJobsController(LoggingMixin):
    """
    Interface for communication with Google API.

    It's not use Apache Beam, but only Google Dataflow API.

    :param dataflow: Discovery resource
    :param project_number: The Google Cloud Project ID.
    :param location: Job location.
    :param poll_sleep: The status refresh rate for pending operations.
    :param name: The Job ID prefix used when the multiple_jobs option is passed is set to True.
    :param job_id: ID of a single job.
    :param num_retries: Maximum number of retries in case of connection problems.
    :param multiple_jobs: If set to true this task will be searched by name prefix (``name`` parameter),
        not by specific job ID, then actions will be performed on all matching jobs.
    :param drain_pipeline: Optional, set to True if want to stop streaming job by draining it
        instead of canceling.
    :param cancel_timeout: wait time in seconds for successful job canceling
    :param wait_until_finished: If True, wait for the end of pipeline execution before exiting. If False,
        it only submits job and check once is job not in terminal state.

        The default behavior depends on the type of pipeline:

        * for the streaming pipeline, wait for jobs to start,
        * for the batch pipeline, wait for the jobs to complete.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        dataflow: Any,
        project_number: str,
        location: str,
        poll_sleep: int = 10,
        name: Optional[str] = None,
        job_id: Optional[str] = None,
        num_retries: int = 0,
        multiple_jobs: bool = False,
        drain_pipeline: bool = False,
        cancel_timeout: Optional[int] = 5 * 60,
        wait_until_finished: Optional[bool] = None,
    ) -> None:

        super().__init__()
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_location = location
        self._multiple_jobs = multiple_jobs
        self._job_id = job_id
        self._num_retries = num_retries
        self._poll_sleep = poll_sleep
        self._cancel_timeout = cancel_timeout
        self._jobs: Optional[List[dict]] = None
        self.drain_pipeline = drain_pipeline
        self._wait_until_finished = wait_until_finished
        self._jobs: Optional[List[dict]] = None

    def is_job_running(self) -> bool:
        """
        Helper method to check if jos is still running in dataflow

        :return: True if job is running.
        :rtype: bool
        """
        self._refresh_jobs()
        if not self._jobs:
            return False

        for job in self._jobs:
            if job["currentState"] not in DataflowJobStatus.TERMINAL_STATES:
                return True
        return False

    # pylint: disable=too-many-nested-blocks
    def _get_current_jobs(self) -> List[dict]:
        """
        Helper method to get list of jobs that start with job name or id

        :return: list of jobs including id's
        :rtype: list
        """
        if not self._multiple_jobs and self._job_id:
            return [self.fetch_job_by_id(self._job_id)]
        elif self._job_name:
            jobs = self._fetch_jobs_by_prefix_name(self._job_name.lower())
            if len(jobs) == 1:
                self._job_id = jobs[0]["id"]
            return jobs
        else:
            raise Exception("Missing both dataflow job ID and name.")

    def fetch_job_by_id(self, job_id: str) -> dict:
        """
        Helper method to fetch the job with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :return: the Job
        :rtype: dict
        """
        return (
            self._dataflow.projects()
            .locations()
            .jobs()
            .get(
                projectId=self._project_number,
                location=self._job_location,
                jobId=job_id,
            )
            .execute(num_retries=self._num_retries)
        )

    def fetch_job_metrics_by_id(self, job_id: str) -> dict:
        """
        Helper method to fetch the job metrics with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :return: the JobMetrics. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
        :rtype: dict
        """
        result = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .getMetrics(projectId=self._project_number, location=self._job_location, jobId=job_id)
            .execute(num_retries=self._num_retries)
        )

        self.log.debug("fetch_job_metrics_by_id %s:\n%s", job_id, result)
        return result

    def _fetch_list_job_messages_responses(self, job_id: str) -> Generator[dict, None, None]:
        """
        Helper method to fetch ListJobMessagesResponse with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :return: yields the ListJobMessagesResponse. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse
        :rtype: Generator[dict, None, None]
        """
        request = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .messages()
            .list(projectId=self._project_number, location=self._job_location, jobId=job_id)
        )

        while request is not None:
            response = request.execute(num_retries=self._num_retries)
            yield response

            request = (
                self._dataflow.projects()
                .locations()
                .jobs()
                .messages()
                .list_next(previous_request=request, previous_response=response)
            )

    def fetch_job_messages_by_id(self, job_id: str) -> List[dict]:
        """
        Helper method to fetch the job messages with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :return: the list of JobMessages. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#JobMessage
        :rtype: List[dict]
        """
        messages: List[dict] = []
        for response in self._fetch_list_job_messages_responses(job_id=job_id):
            messages.extend(response.get("jobMessages", []))
        return messages

    def fetch_job_autoscaling_events_by_id(self, job_id: str) -> List[dict]:
        """
        Helper method to fetch the job autoscaling events with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :return: the list of AutoscalingEvents. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#autoscalingevent
        :rtype: List[dict]
        """
        autoscaling_events: List[dict] = []
        for response in self._fetch_list_job_messages_responses(job_id=job_id):
            autoscaling_events.extend(response.get("autoscalingEvents", []))
        return autoscaling_events

    def _fetch_all_jobs(self) -> List[dict]:
        request = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .list(projectId=self._project_number, location=self._job_location)
        )
        jobs: List[dict] = []
        while request is not None:
            response = request.execute(num_retries=self._num_retries)
            jobs.extend(response["jobs"])

            request = (
                self._dataflow.projects()
                .locations()
                .jobs()
                .list_next(previous_request=request, previous_response=response)
            )
        return jobs

    def _fetch_jobs_by_prefix_name(self, prefix_name: str) -> List[dict]:
        jobs = self._fetch_all_jobs()
        jobs = [job for job in jobs if job["name"].startswith(prefix_name)]
        return jobs

    def _refresh_jobs(self) -> None:
        """
        Helper method to get all jobs by name

        :return: jobs
        :rtype: list
        """
        self._jobs = self._get_current_jobs()

        if self._jobs:
            for job in self._jobs:
                self.log.info(
                    "Google Cloud DataFlow job %s is state: %s",
                    job["name"],
                    job["currentState"],
                )
        else:
            self.log.info("Google Cloud DataFlow job not available yet..")

    def _check_dataflow_job_state(self, job) -> bool:
        """
        Helper method to check the state of one job in dataflow for this task
        if job failed raise exception

        :return: True if job is done.
        :rtype: bool
        :raise: Exception
        """
        if self._wait_until_finished is None:
            wait_for_running = job.get('type') == DataflowJobType.JOB_TYPE_STREAMING
        else:
            wait_for_running = not self._wait_until_finished

        if job['currentState'] == DataflowJobStatus.JOB_STATE_DONE:
            return True
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_FAILED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} has failed.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_CANCELLED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} was cancelled.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_DRAINED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} was drained.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_UPDATED:
            raise Exception(f"Google Cloud Dataflow job {job['name']} was updated.")
        elif job['currentState'] == DataflowJobStatus.JOB_STATE_RUNNING and wait_for_running:
            return True
        elif job['currentState'] in DataflowJobStatus.AWAITING_STATES:
            return self._wait_until_finished is False
        self.log.debug("Current job: %s", str(job))
        raise Exception(f"Google Cloud Dataflow job {job['name']} was unknown state: {job['currentState']}")

    def wait_for_done(self) -> None:
        """Helper method to wait for result of submitted job."""
        self.log.info("Start waiting for done.")
        self._refresh_jobs()
        while self._jobs and not all(self._check_dataflow_job_state(job) for job in self._jobs):
            self.log.info("Waiting for done. Sleep %s s", self._poll_sleep)
            time.sleep(self._poll_sleep)
            self._refresh_jobs()

    def get_jobs(self, refresh: bool = False) -> List[dict]:
        """
        Returns Dataflow jobs.

        :param refresh: Forces the latest data to be fetched.
        :type refresh: bool
        :return: list of jobs
        :rtype: list
        """
        if not self._jobs or refresh:
            self._refresh_jobs()
        if not self._jobs:
            raise ValueError("Could not read _jobs")

        return self._jobs

    def _wait_for_states(self, expected_states: Set[str]):
        """Waiting for the jobs to reach a certain state."""
        if not self._jobs:
            raise ValueError("The _jobs should be set")
        while True:
            self._refresh_jobs()
            job_states = {job['currentState'] for job in self._jobs}
            if not job_states.difference(expected_states):
                return
            unexpected_failed_end_states = expected_states - DataflowJobStatus.FAILED_END_STATES
            if unexpected_failed_end_states.intersection(job_states):
                unexpected_failed_jobs = {
                    job for job in self._jobs if job['currentState'] in unexpected_failed_end_states
                }
                raise AirflowException(
                    "Jobs failed: "
                    + ", ".join(
                        f"ID: {job['id']} name: {job['name']} state: {job['currentState']}"
                        for job in unexpected_failed_jobs
                    )
                )
            time.sleep(self._poll_sleep)

    def cancel(self) -> None:
        """Cancels or drains current job"""
        jobs = self.get_jobs()
        job_ids = [job["id"] for job in jobs if job["currentState"] not in DataflowJobStatus.TERMINAL_STATES]
        if job_ids:
            batch = self._dataflow.new_batch_http_request()
            self.log.info("Canceling jobs: %s", ", ".join(job_ids))
            for job in jobs:
                requested_state = (
                    DataflowJobStatus.JOB_STATE_DRAINED
                    if self.drain_pipeline and job["type"] == DataflowJobType.JOB_TYPE_STREAMING
                    else DataflowJobStatus.JOB_STATE_CANCELLED
                )
                batch.add(
                    self._dataflow.projects()
                    .locations()
                    .jobs()
                    .update(
                        projectId=self._project_number,
                        location=self._job_location,
                        jobId=job["id"],
                        body={"requestedState": requested_state},
                    )
                )
            batch.execute()
            if self._cancel_timeout and isinstance(self._cancel_timeout, int):
                timeout_error_message = "Canceling jobs failed due to timeout ({}s): {}".format(
                    self._cancel_timeout, ", ".join(job_ids)
                )
                with timeout(seconds=self._cancel_timeout, error_message=timeout_error_message):
                    self._wait_for_states({DataflowJobStatus.JOB_STATE_CANCELLED})
        else:
            self.log.info("No jobs to cancel")


class DataflowHook(GoogleBaseHook):
    """
    Hook for Google Dataflow.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        poll_sleep: int = 10,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        drain_pipeline: bool = False,
        cancel_timeout: Optional[int] = 5 * 60,
        wait_until_finished: Optional[bool] = None,
    ) -> None:
        self.poll_sleep = poll_sleep
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.job_id: Optional[str] = None
        self.beam_hook = BeamHook(BeamRunnerType.DataflowRunner)
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> build:
        """Returns a Google Cloud Dataflow service object."""
        http_authorized = self._authorize()
        return build("dataflow", "v1b3", http=http_authorized, cache_discovery=False)

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_java_dataflow(
        self,
        job_name: str,
        variables: dict,
        jar: str,
        project_id: str,
        job_class: Optional[str] = None,
        append_job_name: bool = True,
        multiple_jobs: bool = False,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        """
        Starts Dataflow java job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: dict
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param jar: Name of the jar for the job
        :type job_class: str
        :param job_class: Name of the java class for the job.
        :type job_class: str
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param multiple_jobs: True if to check for multiple job in dataflow
        :type multiple_jobs: bool
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :param location: Job location.
        :type location: str
        """
        warnings.warn(
            """"This method is deprecated.
            Please use `airflow.providers.apache.beam.hooks.beam.start.start_java_pipeline`
            to start pipeline and `providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done`
            to wait for the required pipeline state.
            """,
            DeprecationWarning,
            stacklevel=3,
        )

        name = self.build_dataflow_job_name(job_name, append_job_name)

        variables["jobName"] = name
        variables["region"] = location
        variables["project"] = project_id

        if "labels" in variables:
            variables["labels"] = json.dumps(variables["labels"], separators=(",", ":"))

        self.beam_hook.start_java_pipeline(
            variables=variables,
            jar=jar,
            job_class=job_class,
            process_line_callback=process_line_and_extract_dataflow_job_id_callback(on_new_job_id_callback),
        )
        self.wait_for_done(  # pylint: disable=no-value-for-parameter
            job_name=name,
            location=location,
            job_id=self.job_id,
            multiple_jobs=multiple_jobs,
        )

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_template_dataflow(
        self,
        job_name: str,
        variables: dict,
        parameters: dict,
        dataflow_template: str,
        project_id: str,
        append_job_name: bool = True,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        environment: Optional[dict] = None,
    ) -> dict:
        """
        Starts Dataflow template job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Map of job runtime environment options.
            It will update environment argument if passed.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        :type variables: dict
        :param parameters: Parameters fot the template
        :type parameters: dict
        :param dataflow_template: GCS path to the template.
        :type dataflow_template: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :param location: Job location.
        :type location: str
        :type environment: Optional, Map of job runtime environment options.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        :type environment: Optional[dict]
        """
        name = self.build_dataflow_job_name(job_name, append_job_name)

        environment = environment or {}
        # available keys for runtime environment are listed here:
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment_keys = [
            "numWorkers",
            "maxWorkers",
            "zone",
            "serviceAccountEmail",
            "tempLocation",
            "bypassTempDirValidation",
            "machineType",
            "additionalExperiments",
            "network",
            "subnetwork",
            "additionalUserLabels",
            "kmsKeyName",
            "ipConfiguration",
            "workerRegion",
            "workerZone",
        ]

        for key in variables:
            if key in environment_keys:
                if key in environment:
                    self.log.warning(
                        "'%s' parameter in 'variables' will override of "
                        "the same one passed in 'environment'!",
                        key,
                    )
                environment.update({key: variables[key]})

        service = self.get_conn()
        # pylint: disable=no-member
        request = (
            service.projects()
            .locations()
            .templates()
            .launch(
                projectId=project_id,
                location=location,
                gcsPath=dataflow_template,
                body={
                    "jobName": name,
                    "parameters": parameters,
                    "environment": environment,
                },
            )
        )
        response = request.execute(num_retries=self.num_retries)

        job_id = response["job"]["id"]
        if on_new_job_id_callback:
            on_new_job_id_callback(job_id)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
        )
        jobs_controller.wait_for_done()
        return response["job"]

    @GoogleBaseHook.fallback_to_default_project_id
    def start_flex_template(
        self,
        body: dict,
        location: str,
        project_id: str,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
    ):
        """
        Starts flex templates with the Dataflow  pipeline.

        :param body: The request body. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#request-body
        :param location: The location of the Dataflow job (for example europe-west1)
        :type location: str
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param on_new_job_id_callback: A callback that is called when a Job ID is detected.
        :return: the Job
        """
        service = self.get_conn()
        request = (
            service.projects()  # pylint: disable=no-member
            .locations()
            .flexTemplates()
            .launch(projectId=project_id, body=body, location=location)
        )
        response = request.execute(num_retries=self.num_retries)
        job_id = response["job"]["id"]

        if on_new_job_id_callback:
            on_new_job_id_callback(job_id)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            cancel_timeout=self.cancel_timeout,
        )
        jobs_controller.wait_for_done()

        return jobs_controller.get_jobs(refresh=True)[0]

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_python_dataflow(  # pylint: disable=too-many-arguments
        self,
        job_name: str,
        variables: dict,
        dataflow: str,
        py_options: List[str],
        project_id: str,
        py_interpreter: str = "python3",
        py_requirements: Optional[List[str]] = None,
        py_system_site_packages: bool = False,
        append_job_name: bool = True,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ):
        """
        Starts Dataflow job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: Dict
        :param dataflow: Name of the Dataflow process.
        :type dataflow: str
        :param py_options: Additional options.
        :type py_options: List[str]
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param py_interpreter: Python version of the beam pipeline.
            If None, this defaults to the python3.
            To track python versions supported by beam and related
            issues check: https://issues.apache.org/jira/browse/BEAM-1251
        :param py_requirements: Additional python package(s) to install.
            If a value is passed to this parameter, a new virtual environment has been created with
            additional packages installed.

            You could also install the apache-beam package if it is not installed on your system or you want
            to use a different version.
        :type py_requirements: List[str]
        :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
            See virtualenv documentation for more information.

            This option is only relevant if the ``py_requirements`` parameter is not None.
        :type py_interpreter: str
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :param location: Job location.
        :type location: str
        """
        warnings.warn(
            """This method is deprecated.
            Please use `airflow.providers.apache.beam.hooks.beam.start.start_python_pipeline`
            to start pipeline and `providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done`
            to wait for the required pipeline state.
            """,
            DeprecationWarning,
            stacklevel=3,
        )

        name = self.build_dataflow_job_name(job_name, append_job_name)
        variables["job_name"] = name
        variables["region"] = location
        variables["project"] = project_id

        self.beam_hook.start_python_pipeline(
            variables=variables,
            py_file=dataflow,
            py_options=py_options,
            py_interpreter=py_interpreter,
            py_requirements=py_requirements,
            py_system_site_packages=py_system_site_packages,
            process_line_callback=process_line_and_extract_dataflow_job_id_callback(on_new_job_id_callback),
        )

        self.wait_for_done(  # pylint: disable=no-value-for-parameter
            job_name=name,
            location=location,
            job_id=self.job_id,
        )

    @staticmethod
    def build_dataflow_job_name(job_name: str, append_job_name: bool = True) -> str:
        """Builds Dataflow job name."""
        base_job_name = str(job_name).replace("_", "-")

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                "Invalid job_name ({}); the name must consist of"
                "only the characters [-a-z0-9], starting with a "
                "letter and ending with a letter or number ".format(base_job_name)
            )

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def is_job_dataflow_running(
        self,
        name: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        variables: Optional[dict] = None,
    ) -> bool:
        """
        Helper method to check if jos is still running in dataflow

        :param name: The name of the job.
        :type name: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param location: Job location.
        :type location: str
        :return: True if job is running.
        :rtype: bool
        """
        if variables:
            warnings.warn(
                "The variables parameter has been deprecated. You should pass location using "
                "the location parameter.",
                DeprecationWarning,
                stacklevel=4,
            )
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            location=location,
            poll_sleep=self.poll_sleep,
            drain_pipeline=self.drain_pipeline,
            num_retries=self.num_retries,
            cancel_timeout=self.cancel_timeout,
        )
        return jobs_controller.is_job_running()

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        project_id: str,
        job_name: Optional[str] = None,
        job_id: Optional[str] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        """
        Cancels the job with the specified name prefix or Job ID.

        Parameter ``name`` and ``job_id`` are mutually exclusive.

        :param job_name: Name prefix specifying which jobs are to be canceled.
        :type job_name: str
        :param job_id: Job ID specifying which jobs are to be canceled.
        :type job_id: str
        :param location: Job location.
        :type location: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=job_name,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            drain_pipeline=self.drain_pipeline,
            num_retries=self.num_retries,
            cancel_timeout=self.cancel_timeout,
        )
        jobs_controller.cancel()

    @GoogleBaseHook.fallback_to_default_project_id
    def start_sql_job(
        self,
        job_name: str,
        query: str,
        options: Dict[str, Any],
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
    ):
        """
        Starts Dataflow SQL query.

        :param job_name: The unique name to assign to the Cloud Dataflow job.
        :type job_name: str
        :param query: The SQL query to execute.
        :type query: str
        :param options: Job parameters to be executed.
            For more information, look at:
            `https://cloud.google.com/sdk/gcloud/reference/beta/dataflow/sql/query
            <gcloud beta dataflow sql query>`__
            command reference
        :param location: The location of the Dataflow job (for example europe-west1)
        :type location: str
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :return: the new job object
        """
        cmd = [
            "gcloud",
            "dataflow",
            "sql",
            "query",
            query,
            f"--project={project_id}",
            "--format=value(job.id)",
            f"--job-name={job_name}",
            f"--region={location}",
            *(beam_options_to_args(options)),
        ]
        self.log.info("Executing command: %s", " ".join([shlex.quote(c) for c in cmd]))
        with self.provide_authorized_gcloud():
            proc = subprocess.run(  # pylint: disable=subprocess-run-check
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        self.log.info("Output: %s", proc.stdout.decode())
        self.log.warning("Stderr: %s", proc.stderr.decode())
        self.log.info("Exit code %d", proc.returncode)
        if proc.returncode != 0:
            raise AirflowException(f"Process exit with non-zero exit code. Exit code: {proc.returncode}")
        job_id = proc.stdout.decode().strip()

        self.log.info("Created job ID: %s", job_id)
        if on_new_job_id_callback:
            on_new_job_id_callback(job_id)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            drain_pipeline=self.drain_pipeline,
        )
        jobs_controller.wait_for_done()

        return jobs_controller.get_jobs(refresh=True)[0]

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Gets the job with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        :param location: The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        :return: the Job
        :rtype: dict
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_job_metrics_by_id(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Gets the job metrics with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        :param location: The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        :return: the JobMetrics. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
        :rtype: dict
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_metrics_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_job_messages_by_id(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> List[dict]:
        """
        Gets the job messages with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        :param location: Job location.
        :type location: str
        :return: the list of JobMessages. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#JobMessage
        :rtype: List[dict]
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_messages_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_job_autoscaling_events_by_id(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> List[dict]:
        """
        Gets the job autoscaling events with the specified Job ID.

        :param job_id: Job ID to get.
        :type job_id: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        :param location: Job location.
        :type location: str
        :return: the list of AutoscalingEvents. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#autoscalingevent
        :rtype: List[dict]
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_autoscaling_events_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_done(
        self,
        job_name: str,
        location: str,
        project_id: str,
        job_id: Optional[str] = None,
        multiple_jobs: bool = False,
    ) -> None:
        """
        Wait for Dataflow job.

        :param job_name: The 'jobName' to use when executing the DataFlow job
            (templated). This ends up being set in the pipeline options, so any entry
            with key ``'jobName'`` in ``options`` will be overwritten.
        :type job_name: str
        :param location: location the job is running
        :type location: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        :param job_id: a Dataflow job ID
        :type job_id: str
        :param multiple_jobs: If pipeline creates multiple jobs then monitor all jobs
        :type multiple_jobs: boolean
        """
        job_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=job_name,
            location=location,
            poll_sleep=self.poll_sleep,
            job_id=job_id or self.job_id,
            num_retries=self.num_retries,
            multiple_jobs=multiple_jobs,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
        )
        job_controller.wait_for_done()
