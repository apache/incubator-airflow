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
"""This module contains a Google Cloud Dataproc hook."""

import time
import uuid
import warnings
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import ServerError
from google.api_core.retry import Retry
from google.cloud.dataproc_v1beta2 import (  # pylint: disable=no-name-in-module
    Cluster,
    ClusterControllerClient,
    Job,
    JobControllerClient,
    JobStatus,
    WorkflowTemplate,
    WorkflowTemplateServiceClient,
)
from google.protobuf.duration_pb2 import Duration
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.version import version as airflow_version


class DataProcJobBuilder:
    """A helper class for building Dataproc job."""

    def __init__(
        self,
        project_id: str,
        task_id: str,
        cluster_name: str,
        job_type: str,
        properties: Optional[Dict[str, str]] = None,
    ) -> None:
        name = task_id + "_" + str(uuid.uuid4())[:8]
        self.job_type = job_type
        self.job = {
            "job": {
                "reference": {"project_id": project_id, "job_id": name},
                "placement": {"cluster_name": cluster_name},
                "labels": {'airflow-version': 'v' + airflow_version.replace('.', '-').replace('+', '-')},
                job_type: {},
            }
        }  # type: Dict[str, Any]
        if properties is not None:
            self.job["job"][job_type]["properties"] = properties

    def add_labels(self, labels: dict) -> None:
        """
        Set labels for Dataproc job.

        :param labels: Labels for the job query.
        :type labels: dict
        """
        if labels:
            self.job["job"]["labels"].update(labels)

    def add_variables(self, variables: List[str]) -> None:
        """
        Set variables for Dataproc job.

        :param variables: Variables for the job query.
        :type variables: List[str]
        """
        if variables is not None:
            self.job["job"][self.job_type]["script_variables"] = variables

    def add_args(self, args: List[str]) -> None:
        """
        Set args for Dataproc job.

        :param args: Args for the job query.
        :type args: List[str]
        """
        if args is not None:
            self.job["job"][self.job_type]["args"] = args

    def add_query(self, query: List[str]) -> None:
        """
        Set query uris for Dataproc job.

        :param query: URIs for the job queries.
        :type query: List[str]
        """
        self.job["job"][self.job_type]["query_list"] = {'queries': [query]}

    def add_query_uri(self, query_uri: str) -> None:
        """
        Set query uri for Dataproc job.

        :param query_uri: URI for the job query.
        :type query_uri: str
        """
        self.job["job"][self.job_type]["query_file_uri"] = query_uri

    def add_jar_file_uris(self, jars: List[str]) -> None:
        """
        Set jars uris for Dataproc job.

        :param jars: List of jars URIs
        :type jars: List[str]
        """
        if jars is not None:
            self.job["job"][self.job_type]["jar_file_uris"] = jars

    def add_archive_uris(self, archives: List[str]) -> None:
        """
        Set archives uris for Dataproc job.

        :param archives: List of archives URIs
        :type archives: List[str]
        """
        if archives is not None:
            self.job["job"][self.job_type]["archive_uris"] = archives

    def add_file_uris(self, files: List[str]) -> None:
        """
        Set file uris for Dataproc job.

        :param files: List of files URIs
        :type files: List[str]
        """
        if files is not None:
            self.job["job"][self.job_type]["file_uris"] = files

    def add_python_file_uris(self, pyfiles: List[str]) -> None:
        """
        Set python file uris for Dataproc job.

        :param pyfiles: List of python files URIs
        :type pyfiles: List[str]
        """
        if pyfiles is not None:
            self.job["job"][self.job_type]["python_file_uris"] = pyfiles

    def set_main(self, main_jar: Optional[str], main_class: Optional[str]) -> None:
        """
        Set Dataproc main class.

        :param main_jar: URI for the main file.
        :type main_jar: str
        :param main_class: Name of the main class.
        :type main_class: str
        :raises: Exception
        """
        if main_class is not None and main_jar is not None:
            raise Exception("Set either main_jar or main_class")
        if main_jar:
            self.job["job"][self.job_type]["main_jar_file_uri"] = main_jar
        else:
            self.job["job"][self.job_type]["main_class"] = main_class

    def set_python_main(self, main: str) -> None:
        """
        Set Dataproc main python file uri.

        :param main: URI for the python main file.
        :type main: str
        """
        self.job["job"][self.job_type]["main_python_file_uri"] = main

    def set_job_name(self, name: str) -> None:
        """
        Set Dataproc job name.

        :param name: Job name.
        :type name: str
        """
        self.job["job"]["reference"]["job_id"] = name + "_" + str(uuid.uuid4())[:8]

    def build(self) -> Dict:
        """
        Returns Dataproc job.

        :return: Dataproc job
        :rtype: dict
        """
        return self.job


class DataprocHook(GoogleBaseHook):
    """
    Hook for Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def get_cluster_client(self, location: Optional[str] = None) -> ClusterControllerClient:
        """Returns ClusterControllerClient."""
        client_options = None
        if location and location != 'global':
            client_options = {'api_endpoint': f'{location}-dataproc.googleapis.com:443'}

        return ClusterControllerClient(
            credentials=self._get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def get_template_client(self, location: Optional[str] = None) -> WorkflowTemplateServiceClient:
        """Returns WorkflowTemplateServiceClient."""
        client_options = None
        if location and location != 'global':
            client_options = {'api_endpoint': f'{location}-dataproc.googleapis.com:443'}

        return WorkflowTemplateServiceClient(
            credentials=self._get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def get_job_client(self, location: Optional[str] = None) -> JobControllerClient:
        """Returns JobControllerClient."""
        client_options = None
        if location and location != 'global':
            client_options = {'api_endpoint': f'{location}-dataproc.googleapis.com:443'}

        return JobControllerClient(
            credentials=self._get_credentials(), client_info=self.client_info, client_options=client_options
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        region: str,
        project_id: str,
        cluster_name: str,
        cluster_config: Union[Dict, Cluster],
        labels: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Creates a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :type project_id: str
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :type region: str
        :param cluster_name: Name of the cluster to create
        :type cluster_name: str
        :param labels: Labels that will be assigned to created cluster
        :type labels: Dict[str, str]
        :param cluster_config: Required. The cluster config to create.
            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
        :type cluster_config: Union[Dict, google.cloud.dataproc_v1.types.ClusterConfig]
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``CreateClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :type request_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        labels = labels or {}
        labels.update({'airflow-version': 'v' + airflow_version.replace('.', '-').replace('+', '-')})

        cluster = {
            "project_id": project_id,
            "cluster_name": cluster_name,
            "config": cluster_config,
            "labels": labels,
        }

        client = self.get_cluster_client(location=region)
        result = client.create_cluster(
            request={
                'project_id': project_id,
                'region': region,
                'cluster': cluster,
                'request_id': request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        cluster_uuid: Optional[str] = None,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Deletes a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :type project_id: str
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :type region: str
        :param cluster_name: Required. The cluster name.
        :type cluster_name: str
        :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
            if cluster with specified UUID does not exist.
        :type cluster_uuid: str
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :type request_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_cluster_client(location=region)
        result = client.delete_cluster(
            request={
                'project_id': project_id,
                'region': region,
                'cluster_name': cluster_name,
                'cluster_uuid': cluster_uuid,
                'request_id': request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def diagnose_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Gets cluster diagnostic information. After the operation completes GCS uri to
        diagnose is returned

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :type project_id: str
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :type region: str
        :param cluster_name: Required. The cluster name.
        :type cluster_name: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_cluster_client(location=region)
        operation = client.diagnose_cluster(
            request={'project_id': project_id, 'region': region, 'cluster_name': cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        operation.result()
        gcs_uri = str(operation.operation.response.value)
        return gcs_uri

    @GoogleBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Gets the resource representation for a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :type project_id: str
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :type region: str
        :param cluster_name: Required. The cluster name.
        :type cluster_name: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_cluster_client(location=region)
        result = client.get_cluster(
            request={'project_id': project_id, 'region': region, 'cluster_name': cluster_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_clusters(
        self,
        region: str,
        filter_: str,
        project_id: str,
        page_size: Optional[int] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Lists all regions/{region}/clusters in a project.

        :param project_id: Required. The ID of the Google Cloud project that the cluster belongs to.
        :type project_id: str
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :type region: str
        :param filter_: Optional. A filter constraining the clusters to list. Filters are case-sensitive.
        :type filter_: str
        :param page_size: The maximum number of resources contained in the underlying API response. If page
            streaming is performed per- resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number of resources in a page.
        :type page_size: int
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_cluster_client(location=region)
        result = client.list_clusters(
            request={'project_id': project_id, 'region': region, 'filter': filter_, 'page_size': page_size},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_cluster(  # pylint: disable=too-many-arguments
        self,
        location: str,
        cluster_name: str,
        cluster: Union[Dict, Cluster],
        update_mask: Union[Dict, FieldMask],
        project_id: str,
        graceful_decommission_timeout: Optional[Union[Dict, Duration]] = None,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Updates a cluster in a project.

        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param cluster_name: Required. The cluster name.
        :type cluster_name: str
        :param cluster: Required. The changes to the cluster.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Cluster`
        :type cluster: Union[Dict, google.cloud.dataproc_v1.types.Cluster]
        :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
            example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
            specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify
            the new value, as follows:

            ::

                 { "config":{ "workerConfig":{ "numInstances":"5" } } }

            Similarly, to change the number of preemptible workers in a cluster to 5, the ``update_mask``
            parameter would be ``config.secondary_worker_config.num_instances``, and the ``PATCH`` request
            body would be set as follows:

            ::

                 { "config":{ "secondaryWorkerConfig":{ "numInstances":"5" } } }

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.FieldMask`
        :type update_mask: Union[Dict, google.cloud.dataproc_v1.types.FieldMask]
        :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
            decommissioning allows removing nodes from the cluster without interrupting jobs in progress.
            Timeout specifies how long to wait for jobs in progress to finish before forcefully removing nodes
            (and potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the
            maximum allowed timeout is 1 day.

            Only supported on Dataproc image versions 1.2 and higher.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.Duration`
        :type graceful_decommission_timeout: Union[Dict, google.cloud.dataproc_v1.types.Duration]
        :param request_id: Optional. A unique id used to identify the request. If the server receives two
            ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and
            the first ``google.longrunning.Operation`` created and stored in the backend is returned.
        :type request_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_cluster_client(location=location)
        operation = client.update_cluster(
            request={
                'project_id': project_id,
                'region': location,
                'cluster_name': cluster_name,
                'cluster': cluster,
                'update_mask': update_mask,
                'graceful_decommission_timeout': graceful_decommission_timeout,
                'request_id': request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def create_workflow_template(
        self,
        location: str,
        template: Union[Dict, WorkflowTemplate],
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> WorkflowTemplate:
        """
        Creates new workflow template.

        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param template: The Dataproc workflow template to create. If a dict is provided,
            it must be of the same form as the protobuf message WorkflowTemplate.
        :type template: Union[dict, WorkflowTemplate]
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_template_client(location)
        parent = f'projects/{project_id}/regions/{location}'
        return client.create_workflow_template(
            request={'parent': parent, 'template': template}, retry=retry, timeout=timeout, metadata=metadata
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def instantiate_workflow_template(
        self,
        location: str,
        template_name: str,
        project_id: str,
        version: Optional[int] = None,
        request_id: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Instantiates a template and begins execution.

        :param template_name: Name of template to instantiate.
        :type template_name: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param version: Optional. The version of workflow template to instantiate. If specified,
            the workflow will be instantiated only if the current version of
            the workflow template has the supplied version.
            This option cannot be used to instantiate a previous version of
            workflow template.
        :type version: int
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :type request_id: str
        :param parameters: Optional. Map from parameter names to values that should be used for those
            parameters. Values may not exceed 100 characters.
        :type parameters: Dict[str, str]
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_template_client(location)
        name = f'projects/{project_id}/regions/{location}/workflowTemplates/{template_name}'
        operation = client.instantiate_workflow_template(
            request={'name': name, 'version': version, 'request_id': request_id, 'parameters': parameters},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def instantiate_inline_workflow_template(
        self,
        location: str,
        template: Union[Dict, WorkflowTemplate],
        project_id: str,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ):
        """
        Instantiates a template and begins execution.

        :param template: The workflow template to instantiate. If a dict is provided,
            it must be of the same form as the protobuf message WorkflowTemplate
        :type template: Union[Dict, WorkflowTemplate]
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :type request_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        metadata = metadata or ()
        client = self.get_template_client(location)
        parent = f'projects/{project_id}/regions/{location}'
        operation = client.instantiate_inline_workflow_template(
            request={'parent': parent, 'template': template, 'request_id': request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_job(
        self, job_id: str, location: str, project_id: str, wait_time: int = 10, timeout: Optional[int] = None
    ) -> None:
        """
        Helper method which polls a job to check if it finishes.

        :param job_id: Id of the Dataproc job
        :type job_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param wait_time: Number of seconds between checks
        :type wait_time: int
        :param timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
        :type timeout: int
        """
        state = None
        start = time.monotonic()
        while state not in (JobStatus.State.ERROR, JobStatus.State.DONE, JobStatus.State.CANCELLED):
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: dataproc job {job_id} is not ready after {timeout}s")
            time.sleep(wait_time)
            try:
                job = self.get_job(project_id=project_id, location=location, job_id=job_id)
                state = job.status.state
            except ServerError as err:
                self.log.info("Retrying. Dataproc API returned server error when waiting for job: %s", err)

        if state == JobStatus.State.ERROR:
            raise AirflowException(f'Job failed:\n{job}')
        if state == JobStatus.State.CANCELLED:
            raise AirflowException(f'Job was cancelled:\n{job}')

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        location: str,
        job_id: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Job:
        """
        Gets the resource representation for a job in a project.

        :param job_id: Id of the Dataproc job
        :type job_id: str
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_job_client(location=location)
        job = client.get_job(
            request={'project_id': project_id, 'region': location, 'job_id': job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    def submit_job(
        self,
        location: str,
        job: Union[dict, Job],
        project_id: str,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Job:
        """
        Submits a job to a cluster.

        :param job: The job resource. If a dict is provided,
            it must be of the same form as the protobuf message Job
        :type job: Union[Dict, Job]
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param request_id: Optional. A tag that prevents multiple concurrent workflow instances
            with the same tag from running. This mitigates risk of concurrent
            instances started due to retries.
        :type request_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_job_client(location=location)
        return client.submit_job(
            request={'project_id': project_id, 'region': location, 'job': job, 'request_id': request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def submit(
        self,
        project_id: str,
        job: dict,
        region: str = 'global',
        job_error_states: Optional[Iterable[str]] = None,  # pylint: disable=unused-argument
    ) -> None:
        """
        Submits Google Cloud Dataproc job.

        :param project_id: The id of Google Cloud Dataproc project.
        :type project_id: str
        :param job: The job to be submitted
        :type job: dict
        :param region: The region of Google Dataproc cluster.
        :type region: str
        :param job_error_states: Job states that should be considered error states.
        :type job_error_states: List[str]
        """
        # TODO: Remover one day
        warnings.warn("This method is deprecated. Please use `submit_job`", DeprecationWarning, stacklevel=2)
        job_object = self.submit_job(location=region, project_id=project_id, job=job)
        job_id = job_object.reference.job_id
        self.wait_for_job(job_id=job_id, location=region, project_id=project_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        job_id: str,
        project_id: str,
        location: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Job:
        """
        Starts a job cancellation request.

        :param project_id: Required. The ID of the Google Cloud project that the job belongs to.
        :type project_id: str
        :param location: Required. The Cloud Dataproc region in which to handle the request.
        :type location: str
        :param job_id: Required. The job ID.
        :type job_id: str
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]
        """
        if location is None:
            warnings.warn(
                "Default location value `global` will be deprecated. Please, provide location value.",
                DeprecationWarning,
                stacklevel=2,
            )
            location = 'global'
        client = self.get_job_client(location=location)

        job = client.cancel_job(
            request={'project_id': project_id, 'region': location, 'job_id': job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job
