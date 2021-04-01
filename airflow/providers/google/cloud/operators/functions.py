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
"""This module contains Google Cloud Functions operators."""

import re
from typing import Any, Dict, List, Optional, Sequence, Union

from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.functions import CloudFunctionsHook
from airflow.providers.google.cloud.utils.field_validator import (
    GcpBodyFieldValidator,
    GcpFieldValidationException,
)
from airflow.utils.decorators import apply_defaults
from airflow.version import version


def _validate_available_memory_in_mb(value):
    if int(value) <= 0:
        raise GcpFieldValidationException("The available memory has to be greater than 0")


def _validate_max_instances(value):
    if int(value) <= 0:
        raise GcpFieldValidationException("The max instances parameter has to be greater than 0")


CLOUD_FUNCTION_VALIDATION = [
    dict(name="name", regexp="^.+$"),
    dict(name="description", regexp="^.+$", optional=True),
    dict(name="entryPoint", regexp=r'^.+$', optional=True),
    dict(name="runtime", regexp=r'^.+$', optional=True),
    dict(name="timeout", regexp=r'^.+$', optional=True),
    dict(name="availableMemoryMb", custom_validation=_validate_available_memory_in_mb, optional=True),
    dict(name="labels", optional=True),
    dict(name="environmentVariables", optional=True),
    dict(name="network", regexp=r'^.+$', optional=True),
    dict(name="maxInstances", optional=True, custom_validation=_validate_max_instances),
    dict(
        name="source_code",
        type="union",
        fields=[
            dict(name="sourceArchiveUrl", regexp=r'^.+$'),
            dict(name="sourceRepositoryUrl", regexp=r'^.+$', api_version='v1beta2'),
            dict(name="sourceRepository", type="dict", fields=[dict(name="url", regexp=r'^.+$')]),
            dict(name="sourceUploadUrl"),
        ],
    ),
    dict(
        name="trigger",
        type="union",
        fields=[
            dict(
                name="httpsTrigger",
                type="dict",
                fields=[
                    # This dict should be empty at input (url is added at output)
                ],
            ),
            dict(
                name="eventTrigger",
                type="dict",
                fields=[
                    dict(name="eventType", regexp=r'^.+$'),
                    dict(name="resource", regexp=r'^.+$'),
                    dict(name="service", regexp=r'^.+$', optional=True),
                    dict(
                        name="failurePolicy",
                        type="dict",
                        optional=True,
                        fields=[dict(name="retry", type="dict", optional=True)],
                    ),
                ],
            ),
        ],
    ),
]  # type: List[Dict[str, Any]]


class CloudFunctionDeployFunctionOperator(BaseOperator):
    """
    Creates a function in Google Cloud Functions.
    If a function with this name already exists, it will be updated.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFunctionDeployFunctionOperator`

    :param location: Google Cloud region where the function should be created.
    :type location: str
    :param body: Body of the Cloud Functions definition. The body must be a
        Cloud Functions dictionary as described in:
        https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions
        . Different API versions require different variants of the Cloud Functions
        dictionary.
    :type body: dict or google.cloud.functions.v1.CloudFunction
    :param project_id: (Optional) Google Cloud project ID where the function
        should be created.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
        Default 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: (Optional) API version used (for example v1 - default -  or
        v1beta1).
    :type api_version: str
    :param zip_path: Path to zip file containing source code of the function. If the path
        is set, the sourceUploadUrl should not be specified in the body or it should
        be empty. Then the zip file will be uploaded using the upload URL generated
        via generateUploadUrl from the Cloud Functions API.
    :type zip_path: str
    :param validate_body: If set to False, body validation is not performed.
    :type validate_body: bool
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

    # [START gcf_function_deploy_template_fields]
    template_fields = (
        'body',
        'project_id',
        'location',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcf_function_deploy_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        location: str,
        body: Dict,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        zip_path: Optional[str] = None,
        validate_body: bool = True,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.location = location
        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.zip_path = zip_path
        self.zip_path_preprocessor = ZipPathPreprocessor(body, zip_path)
        self._field_validator = None  # type: Optional[GcpBodyFieldValidator]
        self.impersonation_chain = impersonation_chain
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(CLOUD_FUNCTION_VALIDATION, api_version=api_version)
        self._validate_inputs()
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if not self.location:
            raise AirflowException("The required parameter 'location' is missing")
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")
        self.zip_path_preprocessor.preprocess_body()

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def _create_new_function(self, hook) -> None:
        hook.create_new_function(project_id=self.project_id, location=self.location, body=self.body)

    def _update_function(self, hook) -> None:
        hook.update_function(self.body['name'], self.body, self.body.keys())

    def _check_if_function_exists(self, hook) -> bool:
        name = self.body.get('name')
        if not name:
            raise GcpFieldValidationException(f"The 'name' field should be present in body: '{self.body}'.")
        try:
            hook.get_function(name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                return False
            raise e
        return True

    def _upload_source_code(self, hook):
        return hook.upload_function_zip(
            project_id=self.project_id, location=self.location, zip_path=self.zip_path
        )

    def _set_airflow_version_label(self) -> None:
        if 'labels' not in self.body.keys():
            self.body['labels'] = {}
        self.body['labels'].update({'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})

    def execute(self, context):
        hook = CloudFunctionsHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        if self.zip_path_preprocessor.should_upload_function():
            self.body[GCF_SOURCE_UPLOAD_URL] = self._upload_source_code(hook)
        self._validate_all_body_fields()
        self._set_airflow_version_label()
        if not self._check_if_function_exists(hook):
            self._create_new_function(hook)
        else:
            self._update_function(hook)


GCF_SOURCE_ARCHIVE_URL = 'sourceArchiveUrl'
GCF_SOURCE_UPLOAD_URL = 'sourceUploadUrl'
SOURCE_REPOSITORY = 'sourceRepository'
GCF_ZIP_PATH = 'zip_path'


class ZipPathPreprocessor:
    """
    Pre-processes zip path parameter.

    Responsible for checking if the zip path parameter is correctly specified in
    relation with source_code body fields. Non empty zip path parameter is special because
    it is mutually exclusive with sourceArchiveUrl and sourceRepository body fields.
    It is also mutually exclusive with non-empty sourceUploadUrl.
    The pre-process modifies sourceUploadUrl body field in special way when zip_path
    is not empty. An extra step is run when execute method is called and sourceUploadUrl
    field value is set in the body with the value returned by generateUploadUrl Cloud
    Function API method.

    :param body: Body passed to the create/update method calls.
    :type body: dict
    :param zip_path: (optional) Path to zip file containing source code of the function. If the path
        is set, the sourceUploadUrl should not be specified in the body or it should
        be empty. Then the zip file will be uploaded using the upload URL generated
        via generateUploadUrl from the Cloud Functions API.
    :type zip_path: str

    """

    upload_function = None  # type: Optional[bool]

    def __init__(self, body: dict, zip_path: Optional[str] = None) -> None:
        self.body = body
        self.zip_path = zip_path

    @staticmethod
    def _is_present_and_empty(dictionary, field) -> bool:
        return field in dictionary and not dictionary[field]

    def _verify_upload_url_and_no_zip_path(self) -> None:
        if self._is_present_and_empty(self.body, GCF_SOURCE_UPLOAD_URL):
            if not self.zip_path:
                raise AirflowException(
                    "Parameter '{url}' is empty in the body and argument '{path}' "
                    "is missing or empty. You need to have non empty '{path}' "
                    "when '{url}' is present and empty.".format(url=GCF_SOURCE_UPLOAD_URL, path=GCF_ZIP_PATH)
                )

    def _verify_upload_url_and_zip_path(self) -> None:
        if GCF_SOURCE_UPLOAD_URL in self.body and self.zip_path:
            if not self.body[GCF_SOURCE_UPLOAD_URL]:
                self.upload_function = True
            else:
                raise AirflowException(
                    "Only one of '{}' in body or '{}' argument "
                    "allowed. Found both.".format(GCF_SOURCE_UPLOAD_URL, GCF_ZIP_PATH)
                )

    def _verify_archive_url_and_zip_path(self) -> None:
        if GCF_SOURCE_ARCHIVE_URL in self.body and self.zip_path:
            raise AirflowException(
                "Only one of '{}' in body or '{}' argument "
                "allowed. Found both.".format(GCF_SOURCE_ARCHIVE_URL, GCF_ZIP_PATH)
            )

    def should_upload_function(self) -> bool:
        """
        Checks if function source should be uploaded.

        :rtype: bool
        """
        if self.upload_function is None:
            raise AirflowException('validate() method has to be invoked before ' 'should_upload_function')
        return self.upload_function

    def preprocess_body(self) -> None:
        """
        Modifies sourceUploadUrl body field in special way when zip_path
        is not empty.
        """
        self._verify_archive_url_and_zip_path()
        self._verify_upload_url_and_zip_path()
        self._verify_upload_url_and_no_zip_path()
        if self.upload_function is None:
            self.upload_function = False


FUNCTION_NAME_PATTERN = '^projects/[^/]+/locations/[^/]+/functions/[^/]+$'
FUNCTION_NAME_COMPILED_PATTERN = re.compile(FUNCTION_NAME_PATTERN)


class CloudFunctionDeleteFunctionOperator(BaseOperator):
    """
    Deletes the specified function from Google Cloud Functions.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFunctionDeleteFunctionOperator`

    :param name: A fully-qualified function name, matching
        the pattern: `^projects/[^/]+/locations/[^/]+/functions/[^/]+$`
    :type name: str
    :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
    :type gcp_conn_id: str
    :param api_version: API version used (for example v1 or v1beta1).
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

    # [START gcf_function_delete_template_fields]
    template_fields = (
        'name',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gcf_function_delete_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        name: str,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.name = name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if not self.name:
            raise AttributeError('Empty parameter: name')
        else:
            pattern = FUNCTION_NAME_COMPILED_PATTERN
            if not pattern.match(self.name):
                raise AttributeError(f'Parameter name must match pattern: {FUNCTION_NAME_PATTERN}')

    def execute(self, context):
        hook = CloudFunctionsHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            return hook.delete_function(self.name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                self.log.info('The function does not exist in this project')
                return None
            else:
                self.log.error('An error occurred. Exiting.')
                raise e


class CloudFunctionInvokeFunctionOperator(BaseOperator):
    """
    Invokes a deployed Cloud Function. To be used for testing
    purposes as very limited traffic is allowed.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFunctionDeployFunctionOperator`

    :param function_id: ID of the function to be called
    :type function_id: str
    :param input_data: Input to be passed to the function
    :type input_data: Dict
    :param location: The location where the function is located.
    :type location: str
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :type project_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :return: None
    """

    template_fields = (
        'function_id',
        'input_data',
        'location',
        'project_id',
        'impersonation_chain',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        function_id: str,
        input_data: Dict,
        location: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.function_id = function_id
        self.input_data = input_data
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Dict):
        hook = CloudFunctionsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info('Calling function %s.', self.function_id)
        result = hook.call_function(
            function_id=self.function_id,
            input_data=self.input_data,
            location=self.location,
            project_id=self.project_id,
        )
        self.log.info('Function called successfully. Execution id %s', result.get('executionId'))
        self.xcom_push(context=context, key='execution_id', value=result.get('executionId'))
        return result
