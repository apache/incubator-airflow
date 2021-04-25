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

from unittest import mock

from airflow import DAG
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionCreateInstanceOperator,
    CloudDataFusionCreatePipelineOperator,
    CloudDataFusionDeleteInstanceOperator,
    CloudDataFusionDeletePipelineOperator,
    CloudDataFusionGetInstanceOperator,
    CloudDataFusionListPipelinesOperator,
    CloudDataFusionRestartInstanceOperator,
    CloudDataFusionStartPipelineOperator,
    CloudDataFusionStopPipelineOperator,
    CloudDataFusionUpdateInstanceOperator,
)

HOOK_STR = "airflow.providers.google.cloud.operators.datafusion.DataFusionHook"

LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
PROJECT_ID = "test_project_id"
PIPELINE_NAME = "shrubberyPipeline"
PIPELINE = {"test": "pipeline"}
INSTANCE_URL = "http://datafusion.instance.com"
NAMESPACE = "TEST_NAMESPACE"
RUNTIME_ARGS = {"arg1": "a", "arg2": "b"}


class TestCloudDataFusionUpdateInstanceOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        update_maks = "instance.name"
        op = CloudDataFusionUpdateInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            update_mask=update_maks,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})

        mock_hook.return_value.patch_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            update_mask=update_maks,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionRestartInstanceOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        op = CloudDataFusionRestartInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})

        mock_hook.return_value.restart_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionCreateInstanceOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        op = CloudDataFusionCreateInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})

        mock_hook.return_value.create_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionDeleteInstanceOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        op = CloudDataFusionDeleteInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})

        mock_hook.return_value.delete_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionGetInstanceOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        op = CloudDataFusionGetInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})

        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )


class TestCloudDataFusionCreatePipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}
        op = CloudDataFusionCreatePipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            pipeline=PIPELINE,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.create_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
        )


class TestCloudDataFusionDeletePipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}
        op = CloudDataFusionDeletePipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            version_id="1.12",
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.delete_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            version_id="1.12",
        )


class TestCloudDataFusionStartPipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}

        op = CloudDataFusionStartPipelineOperator(
            task_id="test_task",
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
        )
        op.dag = mock.MagicMock(spec=DAG, task_dict={}, dag_id="test")

        op.execute({})
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.start_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            runtime_args=RUNTIME_ARGS,
        )


class TestCloudDataFusionStopPipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}
        op = CloudDataFusionStopPipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.stop_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL, pipeline_name=PIPELINE_NAME, namespace=NAMESPACE
        )


class TestCloudDataFusionListPipelinesOperator:
    @mock.patch(HOOK_STR)
    def test_execute(self, mock_hook):
        artifact_version = "artifact_version"
        artifact_name = "artifact_name"
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}
        op = CloudDataFusionListPipelinesOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            artifact_version=artifact_version,
            artifact_name=artifact_name,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute({})
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.list_pipelines.assert_called_once_with(
            instance_url=INSTANCE_URL,
            namespace=NAMESPACE,
            artifact_version=artifact_version,
            artifact_name=artifact_name,
        )
