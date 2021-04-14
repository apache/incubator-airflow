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
"""Operators that interact with Google Cloud Life Sciences service."""

from typing import Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.life_sciences import LifeSciencesHook
from airflow.utils.decorators import apply_defaults


class LifeSciencesRunPipelineOperator(BaseOperator):
    """
    Runs a Life Sciences Pipeline

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LifeSciencesRunPipelineOperator`

    :param body: The request body
    :type body: dict
    :param location: The location of the project
    :type location: str
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :type gcp_conn_id: str
    :param api_version: API version used (for example v2beta).
    :type api_version: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "body",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        body: dict,
        location: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v2beta",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.body = body
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain

    def _validate_inputs(self) -> None:
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")
        if not self.location:
            raise AirflowException("The required parameter 'location' is missing")

    def execute(self, context) -> dict:
        hook = LifeSciencesHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        return hook.run_pipeline(body=self.body, location=self.location, project_id=self.project_id)
