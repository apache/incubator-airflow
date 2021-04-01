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

# pylint: disable=too-many-lines

import ast
import unittest
from copy import deepcopy
from unittest import mock

import httplib2
import pytest
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils import timezone

EMPTY_CONTENT = b''

GCP_PROJECT_ID = 'project-id'
GCE_ZONE = 'zone'
RESOURCE_ID = 'resource-id'
GCE_SHORT_MACHINE_TYPE_NAME = 'n1-machine-type'
SET_MACHINE_TYPE_BODY = {'machineType': f'zones/{GCE_ZONE}/machineTypes/{GCE_SHORT_MACHINE_TYPE_NAME}'}

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestGceInstanceStart(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_instance_start(self, mock_hook):
        mock_hook.return_value.start_instance.return_value = True
        op = ComputeEngineStartInstanceOperator(
            project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id=RESOURCE_ID, task_id='id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.start_instance.assert_called_once_with(
            zone=GCE_ZONE, resource_id=RESOURCE_ID, project_id=GCP_PROJECT_ID
        )
        assert result

    # Setting all the operator's input parameters as template dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_instance_start_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = ComputeEngineStartInstanceOperator(
            project_id='{{ dag.dag_id }}',
            zone='{{ dag.dag_id }}',
            resource_id='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'project_id')
        assert dag_id == getattr(op, 'zone')
        assert dag_id == getattr(op, 'resource_id')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_start_should_throw_ex_when_missing_project_id(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineStartInstanceOperator(
                project_id="", zone=GCE_ZONE, resource_id=RESOURCE_ID, task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'project_id' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_start_should_not_throw_ex_when_project_id_none(self, _):
        op = ComputeEngineStartInstanceOperator(zone=GCE_ZONE, resource_id=RESOURCE_ID, task_id='id')
        op.execute(None)

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_start_should_throw_ex_when_missing_zone(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineStartInstanceOperator(
                project_id=GCP_PROJECT_ID, zone="", resource_id=RESOURCE_ID, task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'zone' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_start_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineStartInstanceOperator(
                project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id="", task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'resource_id' is missing" in str(err)
        mock_hook.assert_not_called()


class TestGceInstanceStop(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_instance_stop(self, mock_hook):
        op = ComputeEngineStopInstanceOperator(
            project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id=RESOURCE_ID, task_id='id'
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.stop_instance.assert_called_once_with(
            zone=GCE_ZONE, resource_id=RESOURCE_ID, project_id=GCP_PROJECT_ID
        )

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_instance_stop_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = ComputeEngineStopInstanceOperator(
            project_id='{{ dag.dag_id }}',
            zone='{{ dag.dag_id }}',
            resource_id='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'project_id')
        assert dag_id == getattr(op, 'zone')
        assert dag_id == getattr(op, 'resource_id')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_stop_should_throw_ex_when_missing_project_id(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineStopInstanceOperator(
                project_id="", zone=GCE_ZONE, resource_id=RESOURCE_ID, task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'project_id' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_stop_should_not_throw_ex_when_project_id_none(self, mock_hook):
        op = ComputeEngineStopInstanceOperator(zone=GCE_ZONE, resource_id=RESOURCE_ID, task_id='id')
        op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.stop_instance.assert_called_once_with(
            zone=GCE_ZONE, resource_id=RESOURCE_ID, project_id=None
        )

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_stop_should_throw_ex_when_missing_zone(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineStopInstanceOperator(
                project_id=GCP_PROJECT_ID, zone="", resource_id=RESOURCE_ID, task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'zone' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_stop_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineStopInstanceOperator(
                project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id="", task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'resource_id' is missing" in str(err)
        mock_hook.assert_not_called()


class TestGceInstanceSetMachineType(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type(self, mock_hook):
        mock_hook.return_value.set_machine_type.return_value = True
        op = ComputeEngineSetMachineTypeOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=RESOURCE_ID,
            body=SET_MACHINE_TYPE_BODY,
            task_id='id',
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.set_machine_type.assert_called_once_with(
            zone=GCE_ZONE, resource_id=RESOURCE_ID, body=SET_MACHINE_TYPE_BODY, project_id=GCP_PROJECT_ID
        )

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = ComputeEngineSetMachineTypeOperator(
            project_id='{{ dag.dag_id }}',
            zone='{{ dag.dag_id }}',
            resource_id='{{ dag.dag_id }}',
            body={},
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'project_id')
        assert dag_id == getattr(op, 'zone')
        assert dag_id == getattr(op, 'resource_id')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type_should_throw_ex_when_missing_project_id(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineSetMachineTypeOperator(
                project_id="",
                zone=GCE_ZONE,
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id',
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'project_id' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type_should_not_throw_ex_when_project_id_none(self, mock_hook):
        op = ComputeEngineSetMachineTypeOperator(
            zone=GCE_ZONE, resource_id=RESOURCE_ID, body=SET_MACHINE_TYPE_BODY, task_id='id'
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.set_machine_type.assert_called_once_with(
            zone=GCE_ZONE, resource_id=RESOURCE_ID, body=SET_MACHINE_TYPE_BODY, project_id=None
        )

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type_should_throw_ex_when_missing_zone(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineSetMachineTypeOperator(
                project_id=GCP_PROJECT_ID,
                zone="",
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id',
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'zone' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type_should_throw_ex_when_missing_resource_id(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineSetMachineTypeOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                resource_id="",
                body=SET_MACHINE_TYPE_BODY,
                task_id='id',
            )
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'resource_id' is missing" in str(err)
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_set_machine_type_should_throw_ex_when_missing_machine_type(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineSetMachineTypeOperator(
                project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id=RESOURCE_ID, body={}, task_id='id'
            )
            op.execute(None)
        err = ctx.value
        assert "The required body field 'machineType' is missing. Please add it." in str(err)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )

    MOCK_OP_RESPONSE = (
        "{'kind': 'compute#operation', 'id': '8529919847974922736', "
        "'name': "
        "'operation-1538578207537-577542784f769-7999ab71-94f9ec1d', "
        "'zone': 'https://www.googleapis.com/compute/v1/projects/example"
        "-project/zones/europe-west3-b', 'operationType': "
        "'setMachineType', 'targetLink': "
        "'https://www.googleapis.com/compute/v1/projects/example-project"
        "/zones/europe-west3-b/instances/pa-1', 'targetId': "
        "'2480086944131075860', 'status': 'DONE', 'user': "
        "'service-account@example-project.iam.gserviceaccount.com', "
        "'progress': 100, 'insertTime': '2018-10-03T07:50:07.951-07:00', "
        "'startTime': '2018-10-03T07:50:08.324-07:00', 'endTime': "
        "'2018-10-03T07:50:08.484-07:00', 'error': {'errors': [{'code': "
        "'UNSUPPORTED_OPERATION', 'message': \"Machine type with name "
        "'machine-type-1' does not exist in zone 'europe-west3-b'.\"}]}, "
        "'httpErrorStatusCode': 400, 'httpErrorMessage': 'BAD REQUEST', "
        "'selfLink': "
        "'https://www.googleapis.com/compute/v1/projects/example-project"
        "/zones/europe-west3-b/operations/operation-1538578207537"
        "-577542784f769-7999ab71-94f9ec1d'} "
    )

    @mock.patch(
        'airflow.providers.google.cloud.operators.compute.ComputeEngineHook._check_zone_operation_status'
    )
    @mock.patch(
        'airflow.providers.google.cloud.operators.compute.ComputeEngineHook._execute_set_machine_type'
    )
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook.get_conn')
    def test_set_machine_type_should_handle_and_trim_gce_error(
        self, get_conn, _execute_set_machine_type, _check_zone_operation_status
    ):
        get_conn.return_value = {}
        _execute_set_machine_type.return_value = {"name": "test-operation"}
        _check_zone_operation_status.return_value = ast.literal_eval(self.MOCK_OP_RESPONSE)
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineSetMachineTypeOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                resource_id=RESOURCE_ID,
                body=SET_MACHINE_TYPE_BODY,
                task_id='id',
            )
            op.execute(None)
        err = ctx.value
        _check_zone_operation_status.assert_called_once_with(
            {}, "test-operation", GCP_PROJECT_ID, GCE_ZONE, mock.ANY
        )
        _execute_set_machine_type.assert_called_once_with(
            GCE_ZONE, RESOURCE_ID, SET_MACHINE_TYPE_BODY, GCP_PROJECT_ID
        )
        # Checking the full message was sometimes failing due to different order
        # of keys in the serialized JSON
        assert "400 BAD REQUEST: {" in str(err)  # checking the square bracket trim
        assert "UNSUPPORTED_OPERATION" in str(err)


GCE_INSTANCE_TEMPLATE_NAME = "instance-template-test"
GCE_INSTANCE_TEMPLATE_NEW_NAME = "instance-template-test-new"
GCE_INSTANCE_TEMPLATE_REQUEST_ID = "e12d5b48-4826-4ba9-ada6-0cff1e0b36a6"

GCE_INSTANCE_TEMPLATE_BODY_GET = {
    "kind": "compute#instanceTemplate",
    "id": "6950321349997439715",
    "creationTimestamp": "2018-10-15T06:20:12.777-07:00",
    "name": GCE_INSTANCE_TEMPLATE_NAME,
    "description": "",
    "properties": {
        "machineType": "n1-standard-1",
        "networkInterfaces": [
            {
                "kind": "compute#networkInterface",
                "network": "https://www.googleapis.com/compute/v1/"
                "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "kind": "compute#accessConfig",
                        "type": "ONE_TO_ONE_NAT",
                    }
                ],
            },
            {
                "network": "https://www.googleapis.com/compute/v1/"
                "projects/project/global/networks/default",
                "accessConfigs": [{"kind": "compute#accessConfig", "networkTier": "PREMIUM"}],
            },
        ],
        "disks": [
            {
                "kind": "compute#attachedDisk",
                "type": "PERSISTENT",
                "licenses": [
                    "A String",
                ],
            }
        ],
        "metadata": {"kind": "compute#metadata", "fingerprint": "GDPUYxlwHe4="},
    },
    "selfLink": "https://www.googleapis.com/compute/v1/projects/project"
    "/global/instanceTemplates/instance-template-test",
}

GCE_INSTANCE_TEMPLATE_BODY_INSERT = {
    "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
    "description": "",
    "properties": {
        "machineType": "n1-standard-1",
        "networkInterfaces": [
            {
                "network": "https://www.googleapis.com/compute/v1/"
                "projects/project/global/networks/default",
                "accessConfigs": [
                    {
                        "type": "ONE_TO_ONE_NAT",
                    }
                ],
            },
            {
                "network": "https://www.googleapis.com/compute/v1/"
                "projects/project/global/networks/default",
                "accessConfigs": [{"networkTier": "PREMIUM"}],
            },
        ],
        "disks": [
            {
                "type": "PERSISTENT",
            }
        ],
        "metadata": {"fingerprint": "GDPUYxlwHe4="},
    },
}

GCE_INSTANCE_TEMPLATE_BODY_GET_NEW = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_GET)
GCE_INSTANCE_TEMPLATE_BODY_GET_NEW['name'] = GCE_INSTANCE_TEMPLATE_NEW_NAME


class TestGceInstanceTemplateCopy(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME},
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID, body=GCE_INSTANCE_TEMPLATE_BODY_INSERT, request_id=None
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME},
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=None, body=GCE_INSTANCE_TEMPLATE_BODY_INSERT, request_id=None
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_idempotent_copy_template_when_already_copied(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [GCE_INSTANCE_TEMPLATE_BODY_GET_NEW]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME},
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.insert_instance_template.assert_not_called()
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template_with_request_id(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME},
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=GCE_INSTANCE_TEMPLATE_BODY_INSERT,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template_with_description_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
            task_id='id',
            body_patch={"name": GCE_INSTANCE_TEMPLATE_NEW_NAME, "description": "New description"},
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )

        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["description"] = "New description"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_insert,
            request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_copy_with_some_validation_warnings(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "some_wrong_field": "test",
                "properties": {"some_other_wrong_field": "test"},
            },
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["some_wrong_field"] = "test"
        body_insert["properties"]["some_other_wrong_field"] = "test"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_insert,
            request_id=None,
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template_with_updated_nested_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "machineType": "n1-standard-2",
                },
            },
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"]["machineType"] = "n1-standard-2"
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID, body=body_insert, request_id=None
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template_with_smaller_array_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "machineType": "n1-standard-1",
                    "networkInterfaces": [
                        {
                            "network": "https://www.googleapis.com/compute/v1/"
                            "projects/project/global/networks/default",
                            "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "natIP": "8.8.8.8"}],
                        }
                    ],
                },
            },
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"]["networkInterfaces"] = [
            {
                "network": "https://www.googleapis.com/compute/v1/"
                "projects/project/global/networks/default",
                "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "natIP": "8.8.8.8"}],
            }
        ]
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID, body=body_insert, request_id=None
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_copy_template_with_bigger_array_fields(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        op = ComputeEngineCopyInstanceTemplateOperator(
            project_id=GCP_PROJECT_ID,
            resource_id=GCE_INSTANCE_TEMPLATE_NAME,
            task_id='id',
            body_patch={
                "name": GCE_INSTANCE_TEMPLATE_NEW_NAME,
                "properties": {
                    "disks": [
                        {
                            "kind": "compute#attachedDisk",
                            "type": "SCRATCH",
                            "licenses": [
                                "Updated String",
                            ],
                        },
                        {
                            "kind": "compute#attachedDisk",
                            "type": "PERSISTENT",
                            "licenses": [
                                "Another String",
                            ],
                        },
                    ],
                },
            },
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )

        body_insert = deepcopy(GCE_INSTANCE_TEMPLATE_BODY_INSERT)
        body_insert["properties"]["disks"] = [
            {
                "kind": "compute#attachedDisk",
                "type": "SCRATCH",
                "licenses": [
                    "Updated String",
                ],
            },
            {
                "kind": "compute#attachedDisk",
                "type": "PERSISTENT",
                "licenses": [
                    "Another String",
                ],
            },
        ]
        mock_hook.return_value.insert_instance_template.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            body=body_insert,
            request_id=None,
        )
        assert GCE_INSTANCE_TEMPLATE_BODY_GET_NEW == result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_missing_name(self, mock_hook):
        mock_hook.return_value.get_instance_template.side_effect = [
            HttpError(resp=httplib2.Response({'status': 404}), content=EMPTY_CONTENT),
            GCE_INSTANCE_TEMPLATE_BODY_GET,
            GCE_INSTANCE_TEMPLATE_BODY_GET_NEW,
        ]
        with pytest.raises(AirflowException) as ctx:
            op = ComputeEngineCopyInstanceTemplateOperator(
                project_id=GCP_PROJECT_ID,
                resource_id=GCE_INSTANCE_TEMPLATE_NAME,
                request_id=GCE_INSTANCE_TEMPLATE_REQUEST_ID,
                task_id='id',
                body_patch={"description": "New description"},
            )
            op.execute(None)
        err = ctx.value
        assert "should contain at least name for the new operator in the 'name' field" in str(err)
        mock_hook.assert_not_called()


GCE_INSTANCE_GROUP_MANAGER_NAME = "instance-group-test"
GCE_INSTANCE_TEMPLATE_SOURCE_URL = (
    "https://www.googleapis.com/compute/beta/projects/project"
    "/global/instanceTemplates/instance-template-test"
)

GCE_INSTANCE_TEMPLATE_OTHER_URL = (
    "https://www.googleapis.com/compute/beta/projects/project"
    "/global/instanceTemplates/instance-template-other"
)

GCE_INSTANCE_TEMPLATE_NON_EXISTING_URL = (
    "https://www.googleapis.com/compute/beta/projects/project"
    "/global/instanceTemplates/instance-template-non-existing"
)

GCE_INSTANCE_TEMPLATE_DESTINATION_URL = (
    "https://www.googleapis.com/compute/beta/projects/project"
    "/global/instanceTemplates/instance-template-new"
)

GCE_INSTANCE_GROUP_MANAGER_GET = {
    "kind": "compute#instanceGroupManager",
    "id": "2822359583810032488",
    "creationTimestamp": "2018-10-17T05:39:35.793-07:00",
    "name": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "zone": "https://www.googleapis.com/compute/beta/projects/project/zones/zone",
    "instanceTemplate": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
    "versions": [
        {"name": "v1", "instanceTemplate": GCE_INSTANCE_TEMPLATE_SOURCE_URL, "targetSize": {"calculated": 1}},
        {
            "name": "v2",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_OTHER_URL,
        },
    ],
    "instanceGroup": GCE_INSTANCE_TEMPLATE_SOURCE_URL,
    "baseInstanceName": GCE_INSTANCE_GROUP_MANAGER_NAME,
    "fingerprint": "BKWB_igCNbQ=",
    "currentActions": {
        "none": 1,
        "creating": 0,
        "creatingWithoutRetries": 0,
        "verifying": 0,
        "recreating": 0,
        "deleting": 0,
        "abandoning": 0,
        "restarting": 0,
        "refreshing": 0,
    },
    "pendingActions": {"creating": 0, "deleting": 0, "recreating": 0, "restarting": 0},
    "targetSize": 1,
    "selfLink": "https://www.googleapis.com/compute/beta/projects/project/zones/"
    "zone/instanceGroupManagers/" + GCE_INSTANCE_GROUP_MANAGER_NAME,
    "autoHealingPolicies": [{"initialDelaySec": 300}],
    "serviceAccount": "198907790164@cloudservices.gserviceaccount.com",
}

GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH = {
    "instanceTemplate": GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
    "versions": [
        {
            "name": "v1",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            "targetSize": {"calculated": 1},
        },
        {
            "name": "v2",
            "instanceTemplate": GCE_INSTANCE_TEMPLATE_OTHER_URL,
        },
    ],
}

GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID = "e12d5b48-4826-4ba9-ada6-0cff1e0b36a6"

GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {"fixed": 1},
    "maxUnavailable": {"percent": 10},
    "minReadySec": 1800,
}


class TestGceInstanceGroupManagerUpdate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_instance_group_update(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = deepcopy(
            GCE_INSTANCE_GROUP_MANAGER_GET
        )
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=None,
        )
        assert result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_instance_group_update_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = deepcopy(
            GCE_INSTANCE_GROUP_MANAGER_GET
        )
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=None,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=None,
        )
        assert result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_instance_group_update_no_instance_template_field(self, mock_hook):
        instance_group_manager_no_template = deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        del instance_group_manager_no_template['instanceTemplate']
        mock_hook.return_value.get_instance_group_manager.return_value = instance_group_manager_no_template
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        expected_patch_no_instance_template = deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        del expected_patch_no_instance_template['instanceTemplate']
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_no_instance_template,
            request_id=None,
        )
        assert result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_instance_group_update_no_versions_field(self, mock_hook):
        instance_group_manager_no_versions = deepcopy(GCE_INSTANCE_GROUP_MANAGER_GET)
        del instance_group_manager_no_versions['versions']
        mock_hook.return_value.get_instance_group_manager.return_value = instance_group_manager_no_versions
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        expected_patch_no_versions = deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        del expected_patch_no_versions['versions']
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_no_versions,
            request_id=None,
        )
        assert result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_instance_group_update_with_update_policy(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = deepcopy(
            GCE_INSTANCE_GROUP_MANAGER_GET
        )
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            update_policy=GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY,
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        expected_patch_with_update_policy = deepcopy(GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH)
        expected_patch_with_update_policy['updatePolicy'] = GCE_INSTANCE_GROUP_MANAGER_UPDATE_POLICY
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=expected_patch_with_update_policy,
            request_id=None,
        )
        assert result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_successful_instance_group_update_with_request_id(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = deepcopy(
            GCE_INSTANCE_GROUP_MANAGER_GET
        )
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
            request_id=GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            body=GCE_INSTANCE_GROUP_MANAGER_EXPECTED_PATCH,
            request_id=GCE_INSTANCE_GROUP_MANAGER_REQUEST_ID,
        )
        assert result

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_try_to_use_api_v1(self, _):
        with pytest.raises(AirflowException) as ctx:
            ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
                project_id=GCP_PROJECT_ID,
                zone=GCE_ZONE,
                resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
                task_id='id',
                api_version='v1',
                source_template=GCE_INSTANCE_TEMPLATE_SOURCE_URL,
                destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
            )
        err = ctx.value
        assert "Use beta api version or above" in str(err)

    @mock.patch('airflow.providers.google.cloud.operators.compute.ComputeEngineHook')
    def test_try_to_use_non_existing_template(self, mock_hook):
        mock_hook.return_value.get_instance_group_manager.return_value = deepcopy(
            GCE_INSTANCE_GROUP_MANAGER_GET
        )
        op = ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            project_id=GCP_PROJECT_ID,
            zone=GCE_ZONE,
            resource_id=GCE_INSTANCE_GROUP_MANAGER_NAME,
            task_id='id',
            source_template=GCE_INSTANCE_TEMPLATE_NON_EXISTING_URL,
            destination_template=GCE_INSTANCE_TEMPLATE_DESTINATION_URL,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='beta',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance_group_manager.assert_not_called()
        assert result
