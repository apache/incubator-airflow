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
"""This module contains Google Compute Engine operators."""

from copy import deepcopy
from typing import Any, Dict, List, Optional, Sequence, Union

from googleapiclient.errors import HttpError
from json_merge_patch import merge

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.utils.field_sanitizer import GcpBodyFieldSanitizer
from airflow.providers.google.cloud.utils.field_validator import GcpBodyFieldValidator
from airflow.utils.decorators import apply_defaults


class ComputeEngineBaseOperator(BaseOperator):
    """Abstract base operator for Google Compute Engine operators to inherit from."""

    @apply_defaults
    def __init__(
        self,
        *,
        zone: str,
        resource_id: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.zone = zone
        self.resource_id = resource_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is missing")
        if not self.zone:
            raise AirflowException("The required parameter 'zone' is missing")
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing")

    def execute(self, context):
        pass


class ComputeEngineStartInstanceOperator(ComputeEngineBaseOperator):
    """
    Starts an instance in Google Compute Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineStartInstanceOperator`

    :param zone: Google Cloud zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
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

    # [START gce_instance_start_template_fields]
    template_fields = (
        'project_id',
        'zone',
        'resource_id',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gce_instance_start_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        zone: str,
        resource_id: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def execute(self, context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.start_instance(zone=self.zone, resource_id=self.resource_id, project_id=self.project_id)


class ComputeEngineStopInstanceOperator(ComputeEngineBaseOperator):
    """
    Stops an instance in Google Compute Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineStopInstanceOperator`

    :param zone: Google Cloud zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
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

    # [START gce_instance_stop_template_fields]
    template_fields = (
        'project_id',
        'zone',
        'resource_id',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gce_instance_stop_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        zone: str,
        resource_id: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def execute(self, context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        hook.stop_instance(zone=self.zone, resource_id=self.resource_id, project_id=self.project_id)


SET_MACHINE_TYPE_VALIDATION_SPECIFICATION = [
    dict(name="machineType", regexp="^.+$"),
]


class ComputeEngineSetMachineTypeOperator(ComputeEngineBaseOperator):
    """
    Changes the machine type for a stopped instance to the machine type specified in
        the request.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineSetMachineTypeOperator`

    :param zone: Google Cloud zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param body: Body required by the Compute Engine setMachineType API, as described in
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType#request-body
    :type body: dict
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
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

    # [START gce_instance_set_machine_type_template_fields]
    template_fields = (
        'project_id',
        'zone',
        'resource_id',
        'body',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gce_instance_set_machine_type_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        zone: str,
        resource_id: str,
        body: dict,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        validate_body: bool = True,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.body = body
        self._field_validator = None  # type: Optional[GcpBodyFieldValidator]
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                SET_MACHINE_TYPE_VALIDATION_SPECIFICATION, api_version=api_version
            )
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def execute(self, context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        return hook.set_machine_type(
            zone=self.zone, resource_id=self.resource_id, body=self.body, project_id=self.project_id
        )


GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION = [
    dict(name="name", regexp="^.+$"),
    dict(name="description", optional=True),
    dict(
        name="properties",
        type='dict',
        optional=True,
        fields=[
            dict(name="description", optional=True),
            dict(name="tags", optional=True, fields=[dict(name="items", optional=True)]),
            dict(name="machineType", optional=True),
            dict(name="canIpForward", optional=True),
            dict(name="networkInterfaces", optional=True),  # not validating deeper
            dict(name="disks", optional=True),  # not validating the array deeper
            dict(
                name="metadata",
                optional=True,
                fields=[
                    dict(name="fingerprint", optional=True),
                    dict(name="items", optional=True),
                    dict(name="kind", optional=True),
                ],
            ),
            dict(name="serviceAccounts", optional=True),  # not validating deeper
            dict(
                name="scheduling",
                optional=True,
                fields=[
                    dict(name="onHostMaintenance", optional=True),
                    dict(name="automaticRestart", optional=True),
                    dict(name="preemptible", optional=True),
                    dict(name="nodeAffinities", optional=True),  # not validating deeper
                ],
            ),
            dict(name="labels", optional=True),
            dict(name="guestAccelerators", optional=True),  # not validating deeper
            dict(name="minCpuPlatform", optional=True),
        ],
    ),
]  # type: List[Dict[str, Any]]

GCE_INSTANCE_TEMPLATE_FIELDS_TO_SANITIZE = [
    "kind",
    "id",
    "name",
    "creationTimestamp",
    "properties.disks.sha256",
    "properties.disks.kind",
    "properties.disks.sourceImageEncryptionKey.sha256",
    "properties.disks.index",
    "properties.disks.licenses",
    "properties.networkInterfaces.kind",
    "properties.networkInterfaces.accessConfigs.kind",
    "properties.networkInterfaces.name",
    "properties.metadata.kind",
    "selfLink",
]


class ComputeEngineCopyInstanceTemplateOperator(ComputeEngineBaseOperator):
    """
    Copies the instance template, applying specified changes.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineCopyInstanceTemplateOperator`

    :param resource_id: Name of the Instance Template
    :type resource_id: str
    :param body_patch: Patch to the body of instanceTemplates object following rfc7386
        PATCH semantics. The body_patch content follows
        https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        Name field is required as we need to rename the template,
        all the other fields are optional. It is important to follow PATCH semantics
        - arrays are replaced fully, so if you need to update an array you should
        provide the whole target array as patch element.
    :type body_patch: dict
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param request_id: Optional, unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again).
        It should be in UUID format as defined in RFC 4122.
    :type request_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
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

    # [START gce_instance_template_copy_operator_template_fields]
    template_fields = (
        'project_id',
        'resource_id',
        'request_id',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gce_instance_template_copy_operator_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        resource_id: str,
        body_patch: dict,
        project_id: Optional[str] = None,
        request_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1',
        validate_body: bool = True,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.body_patch = body_patch
        self.request_id = request_id
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        if 'name' not in self.body_patch:
            raise AirflowException(
                "The body '{}' should contain at least "
                "name for the new operator in the 'name' field".format(body_patch)
            )
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_TEMPLATE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone='global',
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body_patch)

    def execute(self, context) -> dict:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        try:
            # Idempotence check (sort of) - we want to check if the new template
            # is already created and if is, then we assume it was created by previous run
            # of CopyTemplate operator - we do not check if content of the template
            # is as expected. Templates are immutable so we cannot update it anyway
            # and deleting/recreating is not worth the hassle especially
            # that we cannot delete template if it is already used in some Instance
            # Group Manager. We assume success if the template is simply present
            existing_template = hook.get_instance_template(
                resource_id=self.body_patch['name'], project_id=self.project_id
            )
            self.log.info(
                "The %s template already existed. It was likely created by previous run of the operator. "
                "Assuming success.",
                existing_template,
            )
            return existing_template
        except HttpError as e:
            # We actually expect to get 404 / Not Found here as the template should
            # not yet exist
            if not e.resp.status == 404:
                raise e
        old_body = hook.get_instance_template(resource_id=self.resource_id, project_id=self.project_id)
        new_body = deepcopy(old_body)
        self._field_sanitizer.sanitize(new_body)
        new_body = merge(new_body, self.body_patch)
        self.log.info("Calling insert instance template with updated body: %s", new_body)
        hook.insert_instance_template(body=new_body, request_id=self.request_id, project_id=self.project_id)
        return hook.get_instance_template(resource_id=self.body_patch['name'], project_id=self.project_id)


class ComputeEngineInstanceGroupUpdateManagerTemplateOperator(ComputeEngineBaseOperator):
    """
    Patches the Instance Group Manager, replacing source template URL with the
    destination one. API V1 does not have update/patch operations for Instance
    Group Manager, so you must use beta or newer API version. Beta is the default.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineInstanceGroupUpdateManagerTemplateOperator`

    :param resource_id: Name of the Instance Group Manager
    :type resource_id: str
    :param zone: Google Cloud zone where the Instance Group Manager exists.
    :type zone: str
    :param source_template: URL of the template to replace.
    :type source_template: str
    :param destination_template: URL of the target template.
    :type destination_template: str
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param request_id: Optional, unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again).
        It should be in UUID format as defined in RFC 4122.
    :type request_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
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

    # [START gce_igm_update_template_operator_template_fields]
    template_fields = (
        'project_id',
        'resource_id',
        'zone',
        'request_id',
        'source_template',
        'destination_template',
        'gcp_conn_id',
        'api_version',
        'impersonation_chain',
    )
    # [END gce_igm_update_template_operator_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        resource_id: str,
        zone: str,
        source_template: str,
        destination_template: str,
        project_id: Optional[str] = None,
        update_policy: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version='beta',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.zone = zone
        self.source_template = source_template
        self.destination_template = destination_template
        self.request_id = request_id
        self.update_policy = update_policy
        self._change_performed = False
        if api_version == 'v1':
            raise AirflowException(
                "Api version v1 does not have update/patch "
                "operations for Instance Group Managers. Use beta"
                " api version or above"
            )
        super().__init__(
            project_id=project_id,
            zone=self.zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _possibly_replace_template(self, dictionary: dict) -> None:
        if dictionary.get('instanceTemplate') == self.source_template:
            dictionary['instanceTemplate'] = self.destination_template
            self._change_performed = True

    def execute(self, context) -> Optional[bool]:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        old_instance_group_manager = hook.get_instance_group_manager(
            zone=self.zone, resource_id=self.resource_id, project_id=self.project_id
        )
        patch_body = {}
        if 'versions' in old_instance_group_manager:
            patch_body['versions'] = old_instance_group_manager['versions']
        if 'instanceTemplate' in old_instance_group_manager:
            patch_body['instanceTemplate'] = old_instance_group_manager['instanceTemplate']
        if self.update_policy:
            patch_body['updatePolicy'] = self.update_policy
        self._possibly_replace_template(patch_body)
        if 'versions' in patch_body:
            for version in patch_body['versions']:
                self._possibly_replace_template(version)
        if self._change_performed or self.update_policy:
            self.log.info("Calling patch instance template with updated body: %s", patch_body)
            return hook.patch_instance_group_manager(
                zone=self.zone,
                resource_id=self.resource_id,
                body=patch_body,
                request_id=self.request_id,
                project_id=self.project_id,
            )
        else:
            # Idempotence achieved
            return True
