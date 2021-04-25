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
"""This module contains a Google Dataprep operator."""

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dataprep import GoogleDataprepHook
from airflow.utils.decorators import apply_defaults


class DataprepGetJobsForJobGroupOperator(BaseOperator):
    """
    Get information about the batch jobs within a Cloud Dataprep job.
    API documentation https://clouddataprep.com/documentation/api#section/Overview

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepGetJobsForJobGroupOperator`

    :param job_id The ID of the job that will be requests
    :type job_id: int
    """

    template_fields = ("job_id",)

    @apply_defaults
    def __init__(self, *, dataprep_conn_id: str = "dataprep_default", job_id: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.dataprep_conn_id = (dataprep_conn_id,)
        self.job_id = job_id

    def execute(self, context: dict) -> dict:
        self.log.info("Fetching data for job with id: %d ...", self.job_id)
        hook = GoogleDataprepHook(
            dataprep_conn_id="dataprep_default",
        )
        response = hook.get_jobs_for_job_group(job_id=self.job_id)
        return response


class DataprepGetJobGroupOperator(BaseOperator):
    """
    Get the specified job group.
    A job group is a job that is executed from a specific node in a flow.
    API documentation https://clouddataprep.com/documentation/api#section/Overview

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepGetJobGroupOperator`

    :param job_group_id: The ID of the job that will be requests
    :type job_group_id: int
    :param embed: Comma-separated list of objects to pull in as part of the response
    :type embed: string
    :param include_deleted: if set to "true", will include deleted objects
    :type include_deleted: bool
    """

    template_fields = ("job_group_id", "embed")

    @apply_defaults
    def __init__(
        self,
        *,
        dataprep_conn_id: str = "dataprep_default",
        job_group_id: int,
        embed: str,
        include_deleted: bool,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataprep_conn_id: str = dataprep_conn_id
        self.job_group_id = job_group_id
        self.embed = embed
        self.include_deleted = include_deleted

    def execute(self, context: dict) -> dict:
        self.log.info("Fetching data for job with id: %d ...", self.job_group_id)
        hook = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        response = hook.get_job_group(
            job_group_id=self.job_group_id,
            embed=self.embed,
            include_deleted=self.include_deleted,
        )
        return response


class DataprepRunJobGroupOperator(BaseOperator):
    """
    Create a ``jobGroup``, which launches the specified job as the authenticated user.
    This performs the same action as clicking on the Run Job button in the application.
    To get recipe_id please follow the Dataprep API documentation
    https://clouddataprep.com/documentation/api#operation/runJobGroup

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepRunJobGroupOperator`

    :param dataprep_conn_id: The Dataprep connection ID
    :type dataprep_conn_id: str
    :param body_request:  Passed as the body_request to GoogleDataprepHook's run_job_group,
        where it's the identifier for the recipe to run
    :type body_request: dict
    """

    template_fields = ("body_request",)

    def __init__(self, *, dataprep_conn_id: str = "dataprep_default", body_request: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.body_request = body_request
        self.dataprep_conn_id = dataprep_conn_id

    def execute(self, context: None) -> dict:
        self.log.info("Creating a job...")
        hook = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        response = hook.run_job_group(body_request=self.body_request)
        return response
