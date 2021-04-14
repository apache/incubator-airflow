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
import unittest
from unittest import mock

import pytest
from google.cloud.dataproc_v1beta2 import JobStatus  # pylint: disable=no-name-in-module

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook, DataProcJobBuilder
from airflow.version import version

AIRFLOW_VERSION = "v" + version.replace(".", "-").replace("+", "-")

JOB = {"job": "test-job"}
JOB_ID = "test-id"
TASK_ID = "test-task-id"
GCP_LOCATION = "global"
GCP_PROJECT = "test-project"
CLUSTER_CONFIG = {"test": "test"}
LABELS = {"test": "test"}
CLUSTER_NAME = "cluster-name"
CLUSTER = {
    "cluster_name": CLUSTER_NAME,
    "config": CLUSTER_CONFIG,
    "labels": LABELS,
    "project_id": GCP_PROJECT,
}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATAPROC_STRING = "airflow.providers.google.cloud.hooks.dataproc.{}"


def mock_init(*args, **kwargs):
    pass


class TestDataprocHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_init):
            self.hook = DataprocHook(gcp_conn_id="test")

    @mock.patch(DATAPROC_STRING.format("DataprocHook._get_credentials"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.client_info"), new_callable=mock.PropertyMock)
    @mock.patch(DATAPROC_STRING.format("ClusterControllerClient"))
    def test_get_cluster_client(self, mock_client, mock_client_info, mock_get_credentials):
        self.hook.get_cluster_client(location=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook._get_credentials"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.client_info"), new_callable=mock.PropertyMock)
    @mock.patch(DATAPROC_STRING.format("ClusterControllerClient"))
    def test_get_cluster_client_region(self, mock_client, mock_client_info, mock_get_credentials):
        self.hook.get_cluster_client(location='region1')
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value,
            client_options={'api_endpoint': 'region1-dataproc.googleapis.com:443'},
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook._get_credentials"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.client_info"), new_callable=mock.PropertyMock)
    @mock.patch(DATAPROC_STRING.format("WorkflowTemplateServiceClient"))
    def test_get_template_client_global(self, mock_client, mock_client_info, mock_get_credentials):
        _ = self.hook.get_template_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook._get_credentials"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.client_info"), new_callable=mock.PropertyMock)
    @mock.patch(DATAPROC_STRING.format("WorkflowTemplateServiceClient"))
    def test_get_template_client_region(self, mock_client, mock_client_info, mock_get_credentials):
        _ = self.hook.get_template_client(location='region1')
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value,
            client_options={'api_endpoint': 'region1-dataproc.googleapis.com:443'},
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook._get_credentials"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.client_info"), new_callable=mock.PropertyMock)
    @mock.patch(DATAPROC_STRING.format("JobControllerClient"))
    def test_get_job_client(self, mock_client, mock_client_info, mock_get_credentials):
        self.hook.get_job_client(location=GCP_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value,
            client_options=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook._get_credentials"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.client_info"), new_callable=mock.PropertyMock)
    @mock.patch(DATAPROC_STRING.format("JobControllerClient"))
    def test_get_job_client_region(self, mock_client, mock_client_info, mock_get_credentials):
        self.hook.get_job_client(location='region1')
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value,
            client_options={'api_endpoint': 'region1-dataproc.googleapis.com:443'},
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_create_cluster(self, mock_client):
        self.hook.create_cluster(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            cluster_name=CLUSTER_NAME,
            cluster_config=CLUSTER_CONFIG,
            labels=LABELS,
        )
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.create_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster=CLUSTER,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_delete_cluster(self, mock_client):
        self.hook.delete_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.delete_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
                cluster_uuid=None,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_diagnose_cluster(self, mock_client):
        self.hook.diagnose_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.diagnose_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )
        mock_client.return_value.diagnose_cluster.return_value.result.assert_called_once_with()

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_get_cluster(self, mock_client):
        self.hook.get_cluster(project_id=GCP_PROJECT, region=GCP_LOCATION, cluster_name=CLUSTER_NAME)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.get_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster_name=CLUSTER_NAME,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_list_clusters(self, mock_client):
        filter_ = "filter"

        self.hook.list_clusters(project_id=GCP_PROJECT, region=GCP_LOCATION, filter_=filter_)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.list_clusters.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                filter=filter_,
                page_size=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_cluster_client"))
    def test_update_cluster(self, mock_client):
        update_mask = "update-mask"
        self.hook.update_cluster(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster=CLUSTER,
            cluster_name=CLUSTER_NAME,
            update_mask=update_mask,
        )
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.update_cluster.assert_called_once_with(
            request=dict(
                project_id=GCP_PROJECT,
                region=GCP_LOCATION,
                cluster=CLUSTER,
                cluster_name=CLUSTER_NAME,
                update_mask=update_mask,
                graceful_decommission_timeout=None,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_create_workflow_template(self, mock_client):
        template = {"test": "test"}
        parent = f'projects/{GCP_PROJECT}/regions/{GCP_LOCATION}'
        self.hook.create_workflow_template(location=GCP_LOCATION, template=template, project_id=GCP_PROJECT)
        mock_client.return_value.create_workflow_template.assert_called_once_with(
            request=dict(parent=parent, template=template), retry=None, timeout=None, metadata=()
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_instantiate_workflow_template(self, mock_client):
        template_name = "template_name"
        name = f'projects/{GCP_PROJECT}/regions/{GCP_LOCATION}/workflowTemplates/{template_name}'
        self.hook.instantiate_workflow_template(
            location=GCP_LOCATION, template_name=template_name, project_id=GCP_PROJECT
        )
        mock_client.return_value.instantiate_workflow_template.assert_called_once_with(
            request=dict(name=name, version=None, parameters=None, request_id=None),
            retry=None,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_template_client"))
    def test_instantiate_inline_workflow_template(self, mock_client):
        template = {"test": "test"}
        parent = f'projects/{GCP_PROJECT}/regions/{GCP_LOCATION}'
        self.hook.instantiate_inline_workflow_template(
            location=GCP_LOCATION, template=template, project_id=GCP_PROJECT
        )
        mock_client.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            request=dict(parent=parent, template=template, request_id=None),
            retry=None,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job"))
    def test_wait_for_job(self, mock_get_job):
        mock_get_job.side_effect = [
            mock.MagicMock(status=mock.MagicMock(state=JobStatus.State.RUNNING)),
            mock.MagicMock(status=mock.MagicMock(state=JobStatus.State.ERROR)),
        ]
        with pytest.raises(AirflowException):
            self.hook.wait_for_job(job_id=JOB_ID, location=GCP_LOCATION, project_id=GCP_PROJECT, wait_time=0)
        calls = [
            mock.call(location=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT),
            mock.call(location=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT),
        ]
        mock_get_job.has_calls(calls)

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_get_job(self, mock_client):
        self.hook.get_job(location=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.get_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_submit_job(self, mock_client):
        self.hook.submit_job(location=GCP_LOCATION, job=JOB, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.submit_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job=JOB,
                project_id=GCP_PROJECT,
                request_id=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.wait_for_job"))
    @mock.patch(DATAPROC_STRING.format("DataprocHook.submit_job"))
    def test_submit(self, mock_submit_job, mock_wait_for_job):
        mock_submit_job.return_value.reference.job_id = JOB_ID
        with pytest.warns(DeprecationWarning):
            self.hook.submit(project_id=GCP_PROJECT, job=JOB, region=GCP_LOCATION)
        mock_submit_job.assert_called_once_with(location=GCP_LOCATION, project_id=GCP_PROJECT, job=JOB)
        mock_wait_for_job.assert_called_once_with(
            location=GCP_LOCATION, project_id=GCP_PROJECT, job_id=JOB_ID
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_cancel_job(self, mock_client):
        self.hook.cancel_job(location=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(location=GCP_LOCATION)
        mock_client.return_value.cancel_job.assert_called_once_with(
            request=dict(
                region=GCP_LOCATION,
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_STRING.format("DataprocHook.get_job_client"))
    def test_cancel_job_deprecation_warning(self, mock_client):
        with pytest.warns(DeprecationWarning):
            self.hook.cancel_job(job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(location='global')
        mock_client.return_value.cancel_job.assert_called_once_with(
            request=dict(
                region='global',
                job_id=JOB_ID,
                project_id=GCP_PROJECT,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestDataProcJobBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.job_type = "test"
        self.builder = DataProcJobBuilder(
            project_id=GCP_PROJECT,
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            job_type=self.job_type,
            properties={"test": "test"},
        )

    @mock.patch(DATAPROC_STRING.format("uuid.uuid4"))
    def test_init(self, mock_uuid):
        mock_uuid.return_value = "uuid"
        properties = {"test": "test"}
        job = {
            "job": {
                "labels": {"airflow-version": AIRFLOW_VERSION},
                "placement": {"cluster_name": CLUSTER_NAME},
                "reference": {"job_id": TASK_ID + "_uuid", "project_id": GCP_PROJECT},
                "test": {"properties": properties},
            }
        }
        builder = DataProcJobBuilder(
            project_id=GCP_PROJECT,
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            job_type="test",
            properties=properties,
        )

        assert job == builder.job

    def test_add_labels(self):
        labels = {"key": "value"}
        self.builder.add_labels(labels)
        assert "key" in self.builder.job["job"]["labels"]
        assert "value" == self.builder.job["job"]["labels"]["key"]

    def test_add_variables(self):
        variables = ["variable"]
        self.builder.add_variables(variables)
        assert variables == self.builder.job["job"][self.job_type]["script_variables"]

    def test_add_args(self):
        args = ["args"]
        self.builder.add_args(args)
        assert args == self.builder.job["job"][self.job_type]["args"]

    def test_add_query(self):
        query = ["query"]
        self.builder.add_query(query)
        assert {"queries": [query]} == self.builder.job["job"][self.job_type]["query_list"]

    def test_add_query_uri(self):
        query_uri = "query_uri"
        self.builder.add_query_uri(query_uri)
        assert query_uri == self.builder.job["job"][self.job_type]["query_file_uri"]

    def test_add_jar_file_uris(self):
        jar_file_uris = ["jar_file_uris"]
        self.builder.add_jar_file_uris(jar_file_uris)
        assert jar_file_uris == self.builder.job["job"][self.job_type]["jar_file_uris"]

    def test_add_archive_uris(self):
        archive_uris = ["archive_uris"]
        self.builder.add_archive_uris(archive_uris)
        assert archive_uris == self.builder.job["job"][self.job_type]["archive_uris"]

    def test_add_file_uris(self):
        file_uris = ["file_uris"]
        self.builder.add_file_uris(file_uris)
        assert file_uris == self.builder.job["job"][self.job_type]["file_uris"]

    def test_add_python_file_uris(self):
        python_file_uris = ["python_file_uris"]
        self.builder.add_python_file_uris(python_file_uris)
        assert python_file_uris == self.builder.job["job"][self.job_type]["python_file_uris"]

    def test_set_main_error(self):
        with pytest.raises(Exception):
            self.builder.set_main("test", "test")

    def test_set_main_class(self):
        main = "main"
        self.builder.set_main(main_class=main, main_jar=None)
        assert main == self.builder.job["job"][self.job_type]["main_class"]

    def test_set_main_jar(self):
        main = "main"
        self.builder.set_main(main_class=None, main_jar=main)
        assert main == self.builder.job["job"][self.job_type]["main_jar_file_uri"]

    def test_set_python_main(self):
        main = "main"
        self.builder.set_python_main(main)
        assert main == self.builder.job["job"][self.job_type]["main_python_file_uri"]

    @mock.patch(DATAPROC_STRING.format("uuid.uuid4"))
    def test_set_job_name(self, mock_uuid):
        uuid = "test_uuid"
        mock_uuid.return_value = uuid
        name = "name"
        self.builder.set_job_name(name)
        name += "_" + uuid[:8]
        assert name == self.builder.job["job"]["reference"]["job_id"]

    def test_build(self):
        assert self.builder.job == self.builder.build()
