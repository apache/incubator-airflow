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
import itertools
import unittest
from copy import deepcopy
from datetime import date, time
from typing import Dict
from unittest import mock

import pytest
from botocore.credentials import Credentials
from freezegun import freeze_time
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    ACCESS_KEY_ID,
    AWS_ACCESS_KEY,
    AWS_S3_DATA_SOURCE,
    BUCKET_NAME,
    FILTER_JOB_NAMES,
    GCS_DATA_SINK,
    GCS_DATA_SOURCE,
    HTTP_DATA_SOURCE,
    LIST_URL,
    NAME,
    SCHEDULE,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    SECRET_ACCESS_KEY,
    START_TIME_OF_DAY,
    STATUS,
    TRANSFER_SPEC,
)
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCancelOperationOperator,
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
    CloudDataTransferServiceGCSToGCSOperator,
    CloudDataTransferServiceGetOperationOperator,
    CloudDataTransferServiceListOperationsOperator,
    CloudDataTransferServicePauseOperationOperator,
    CloudDataTransferServiceResumeOperationOperator,
    CloudDataTransferServiceS3ToGCSOperator,
    CloudDataTransferServiceUpdateJobOperator,
    TransferJobPreprocessor,
    TransferJobValidator,
)
from airflow.utils import timezone

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None

GCP_PROJECT_ID = 'project-id'
TASK_ID = 'task-id'
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

JOB_NAME = "job-name"
OPERATION_NAME = "operation-name"
AWS_BUCKET_NAME = "aws-bucket-name"
GCS_BUCKET_NAME = "gcp-bucket-name"
DESCRIPTION = "description"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

TEST_FILTER = {FILTER_JOB_NAMES: [JOB_NAME]}

TEST_AWS_ACCESS_KEY_ID = "test-key-1"
TEST_AWS_ACCESS_SECRET = "test-secret-1"
TEST_AWS_ACCESS_KEY = {ACCESS_KEY_ID: TEST_AWS_ACCESS_KEY_ID, SECRET_ACCESS_KEY: TEST_AWS_ACCESS_SECRET}

NATIVE_DATE = date(2018, 10, 15)
DICT_DATE = {'day': 15, 'month': 10, 'year': 2018}
NATIVE_TIME = time(hour=11, minute=42, second=43)
DICT_TIME = {'hours': 11, 'minutes': 42, 'seconds': 43}
SCHEDULE_NATIVE = {
    SCHEDULE_START_DATE: NATIVE_DATE,
    SCHEDULE_END_DATE: NATIVE_DATE,
    START_TIME_OF_DAY: NATIVE_TIME,
}

SCHEDULE_DICT = {
    SCHEDULE_START_DATE: {'day': 15, 'month': 10, 'year': 2018},
    SCHEDULE_END_DATE: {'day': 15, 'month': 10, 'year': 2018},
    START_TIME_OF_DAY: {'hours': 11, 'minutes': 42, 'seconds': 43},
}

SOURCE_AWS = {AWS_S3_DATA_SOURCE: {BUCKET_NAME: AWS_BUCKET_NAME}}
SOURCE_GCS = {GCS_DATA_SOURCE: {BUCKET_NAME: GCS_BUCKET_NAME}}
SOURCE_HTTP = {HTTP_DATA_SOURCE: {LIST_URL: "http://example.com"}}

VALID_TRANSFER_JOB_BASE = {
    NAME: JOB_NAME,
    DESCRIPTION: DESCRIPTION,
    STATUS: 'ENABLED',
    SCHEDULE: SCHEDULE_DICT,
    TRANSFER_SPEC: {GCS_DATA_SINK: {BUCKET_NAME: GCS_BUCKET_NAME}},
}  # type: Dict
VALID_TRANSFER_JOB_GCS = deepcopy(VALID_TRANSFER_JOB_BASE)
VALID_TRANSFER_JOB_GCS[TRANSFER_SPEC].update(deepcopy(SOURCE_GCS))
VALID_TRANSFER_JOB_AWS = deepcopy(VALID_TRANSFER_JOB_BASE)
VALID_TRANSFER_JOB_AWS[TRANSFER_SPEC].update(deepcopy(SOURCE_AWS))

VALID_TRANSFER_JOB_GCS = {
    NAME: JOB_NAME,
    DESCRIPTION: DESCRIPTION,
    STATUS: 'ENABLED',
    SCHEDULE: SCHEDULE_NATIVE,
    TRANSFER_SPEC: {
        GCS_DATA_SOURCE: {BUCKET_NAME: GCS_BUCKET_NAME},
        GCS_DATA_SINK: {BUCKET_NAME: GCS_BUCKET_NAME},
    },
}

VALID_TRANSFER_JOB_RAW = {
    DESCRIPTION: DESCRIPTION,
    STATUS: 'ENABLED',
    SCHEDULE: SCHEDULE_DICT,
    TRANSFER_SPEC: {GCS_DATA_SINK: {BUCKET_NAME: GCS_BUCKET_NAME}},
}  # type: Dict

VALID_TRANSFER_JOB_GCS_RAW = deepcopy(VALID_TRANSFER_JOB_RAW)
VALID_TRANSFER_JOB_GCS_RAW[TRANSFER_SPEC].update(SOURCE_GCS)
VALID_TRANSFER_JOB_AWS_RAW = deepcopy(VALID_TRANSFER_JOB_RAW)
VALID_TRANSFER_JOB_AWS_RAW[TRANSFER_SPEC].update(deepcopy(SOURCE_AWS))
VALID_TRANSFER_JOB_AWS_RAW[TRANSFER_SPEC][AWS_S3_DATA_SOURCE][AWS_ACCESS_KEY] = TEST_AWS_ACCESS_KEY


VALID_OPERATION = {NAME: "operation-name"}


class TestTransferJobPreprocessor(unittest.TestCase):
    def test_should_do_nothing_on_empty(self):
        body = {}
        TransferJobPreprocessor(body=body).process_body()
        assert body == {}

    @unittest.skipIf(boto3 is None, "Skipping test because boto3 is not available")
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_should_inject_aws_credentials(self, mock_hook):
        mock_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        body = {TRANSFER_SPEC: deepcopy(SOURCE_AWS)}
        body = TransferJobPreprocessor(body=body).process_body()
        assert body[TRANSFER_SPEC][AWS_S3_DATA_SOURCE][AWS_ACCESS_KEY] == TEST_AWS_ACCESS_KEY

    @parameterized.expand([(SCHEDULE_START_DATE,), (SCHEDULE_END_DATE,)])
    def test_should_format_date_from_python_to_dict(self, field_attr):
        body = {SCHEDULE: {field_attr: NATIVE_DATE}}
        TransferJobPreprocessor(body=body).process_body()
        assert body[SCHEDULE][field_attr] == DICT_DATE

    def test_should_format_time_from_python_to_dict(self):
        body = {SCHEDULE: {START_TIME_OF_DAY: NATIVE_TIME}}
        TransferJobPreprocessor(body=body).process_body()
        assert body[SCHEDULE][START_TIME_OF_DAY] == DICT_TIME

    @parameterized.expand([(SCHEDULE_START_DATE,), (SCHEDULE_END_DATE,)])
    def test_should_not_change_date_for_dict(self, field_attr):
        body = {SCHEDULE: {field_attr: DICT_DATE}}
        TransferJobPreprocessor(body=body).process_body()
        assert body[SCHEDULE][field_attr] == DICT_DATE

    def test_should_not_change_time_for_dict(self):
        body = {SCHEDULE: {START_TIME_OF_DAY: DICT_TIME}}
        TransferJobPreprocessor(body=body).process_body()
        assert body[SCHEDULE][START_TIME_OF_DAY] == DICT_TIME

    @freeze_time("2018-10-15")
    def test_should_set_default_schedule(self):
        body = {}
        TransferJobPreprocessor(body=body, default_schedule=True).process_body()
        assert body == {
            SCHEDULE: {
                SCHEDULE_END_DATE: {'day': 15, 'month': 10, 'year': 2018},
                SCHEDULE_START_DATE: {'day': 15, 'month': 10, 'year': 2018},
            }
        }


class TestTransferJobValidator(unittest.TestCase):
    def test_should_raise_exception_when_encounters_aws_credentials(self):
        body = {"transferSpec": {"awsS3DataSource": {"awsAccessKey": TEST_AWS_ACCESS_KEY}}}
        with pytest.raises(AirflowException) as ctx:
            TransferJobValidator(body=body).validate_body()
        err = ctx.value
        assert (
            "AWS credentials detected inside the body parameter (awsAccessKey). This is not allowed, please "
            "use Airflow connections to store credentials." in str(err)
        )

    def test_should_raise_exception_when_body_empty(self):
        body = None
        with pytest.raises(AirflowException) as ctx:
            TransferJobValidator(body=body).validate_body()
        err = ctx.value
        assert "The required parameter 'body' is empty or None" in str(err)

    @parameterized.expand(
        [
            (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_GCS.items(), SOURCE_HTTP.items())),),
            (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_GCS.items())),),
            (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_HTTP.items())),),
            (dict(itertools.chain(SOURCE_GCS.items(), SOURCE_HTTP.items())),),
        ]
    )
    def test_verify_data_source(self, transfer_spec):
        body = {TRANSFER_SPEC: transfer_spec}

        with pytest.raises(AirflowException) as ctx:
            TransferJobValidator(body=body).validate_body()
        err = ctx.value
        assert (
            "More than one data source detected. Please choose exactly one data source from: "
            "gcsDataSource, awsS3DataSource and httpDataSource." in str(err)
        )

    @parameterized.expand([(VALID_TRANSFER_JOB_GCS,), (VALID_TRANSFER_JOB_AWS,)])
    def test_verify_success(self, body):
        try:
            TransferJobValidator(body=body).validate_body()
            validated = True
        except AirflowException:
            validated = False

        assert validated


class TestGcpStorageTransferJobCreateOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_job_create_gcs(self, mock_hook):
        mock_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB_GCS_RAW
        body = deepcopy(VALID_TRANSFER_JOB_GCS)
        del body['name']
        op = CloudDataTransferServiceCreateJobOperator(
            body=body,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(None)

        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_transfer_job.assert_called_once_with(body=VALID_TRANSFER_JOB_GCS_RAW)

        assert result == VALID_TRANSFER_JOB_GCS_RAW

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_job_create_aws(self, aws_hook, mock_hook):
        mock_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB_AWS_RAW
        aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )
        body = deepcopy(VALID_TRANSFER_JOB_AWS)
        del body['name']
        op = CloudDataTransferServiceCreateJobOperator(
            body=body,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(None)

        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_transfer_job.assert_called_once_with(body=VALID_TRANSFER_JOB_AWS_RAW)

        assert result == VALID_TRANSFER_JOB_AWS_RAW

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_job_create_multiple(self, aws_hook, gcp_hook):
        aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )
        gcp_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB_AWS_RAW
        body = deepcopy(VALID_TRANSFER_JOB_AWS)

        op = CloudDataTransferServiceCreateJobOperator(body=body, task_id=TASK_ID)
        result = op.execute(None)
        assert result == VALID_TRANSFER_JOB_AWS_RAW

        op = CloudDataTransferServiceCreateJobOperator(body=body, task_id=TASK_ID)
        result = op.execute(None)
        assert result == VALID_TRANSFER_JOB_AWS_RAW

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        # pylint: disable=attribute-defined-outside-init
        self.dag = DAG(dag_id, default_args={'start_date': DEFAULT_DATE})
        op = CloudDataTransferServiceCreateJobOperator(
            body={"description": "{{ dag.dag_id }}"},
            gcp_conn_id='{{ dag.dag_id }}',
            aws_conn_id='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'body')[DESCRIPTION]
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'aws_conn_id')


class TestGcpStorageTransferJobUpdateOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_job_update(self, mock_hook):
        mock_hook.return_value.update_transfer_job.return_value = VALID_TRANSFER_JOB_GCS
        body = {'transferJob': {'description': 'example-name'}, 'updateTransferJobFieldMask': DESCRIPTION}

        op = CloudDataTransferServiceUpdateJobOperator(
            job_name=JOB_NAME,
            body=body,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(None)

        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_transfer_job.assert_called_once_with(job_name=JOB_NAME, body=body)
        assert result == VALID_TRANSFER_JOB_GCS

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceUpdateJobOperator(
            job_name='{{ dag.dag_id }}',
            body={'transferJob': {"name": "{{ dag.dag_id }}"}},
            task_id='task-id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'body')['transferJob']['name']
        assert dag_id == getattr(op, 'job_name')


class TestGcpStorageTransferJobDeleteOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_job_delete(self, mock_hook):
        op = CloudDataTransferServiceDeleteJobOperator(
            job_name=JOB_NAME,
            project_id=GCP_PROJECT_ID,
            task_id='task-id',
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_transfer_job.assert_called_once_with(
            job_name=JOB_NAME, project_id=GCP_PROJECT_ID
        )

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_job_delete_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceDeleteJobOperator(
            job_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'job_name')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_job_delete_should_throw_ex_when_name_none(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = CloudDataTransferServiceDeleteJobOperator(job_name="", task_id='task-id')
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'job_name' is empty or None" in str(err)
        mock_hook.assert_not_called()


class TestGpcStorageTransferOperationsGetOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_get(self, mock_hook):
        mock_hook.return_value.get_transfer_operation.return_value = VALID_OPERATION
        op = CloudDataTransferServiceGetOperationOperator(
            operation_name=OPERATION_NAME,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_transfer_operation.assert_called_once_with(operation_name=OPERATION_NAME)
        assert result == VALID_OPERATION

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_get_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceGetOperationOperator(
            operation_name='{{ dag.dag_id }}', task_id='task-id', dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'operation_name')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_get_should_throw_ex_when_operation_name_none(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = CloudDataTransferServiceGetOperationOperator(operation_name="", task_id=TASK_ID)
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'operation_name' is empty or None" in str(err)
        mock_hook.assert_not_called()


class TestGcpStorageTransferOperationListOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_list(self, mock_hook):
        mock_hook.return_value.list_transfer_operations.return_value = [VALID_TRANSFER_JOB_GCS]
        op = CloudDataTransferServiceListOperationsOperator(
            request_filter=TEST_FILTER,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_transfer_operations.assert_called_once_with(request_filter=TEST_FILTER)
        assert result == [VALID_TRANSFER_JOB_GCS]

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceListOperationsOperator(
            request_filter={"job_names": ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()

        # pylint: disable=unsubscriptable-object
        assert dag_id == getattr(op, 'filter')['job_names'][0]
        # pylint: enable=unsubscriptable-object

        assert dag_id == getattr(op, 'gcp_conn_id')


class TestGcpStorageTransferOperationsPauseOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_pause(self, mock_hook):
        op = CloudDataTransferServicePauseOperationOperator(
            operation_name=OPERATION_NAME,
            task_id='task-id',
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.pause_transfer_operation.assert_called_once_with(operation_name=OPERATION_NAME)

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_pause_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServicePauseOperationOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'operation_name')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_pause_should_throw_ex_when_name_none(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = CloudDataTransferServicePauseOperationOperator(operation_name="", task_id='task-id')
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'operation_name' is empty or None" in str(err)
        mock_hook.assert_not_called()


class TestGcpStorageTransferOperationsResumeOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_resume(self, mock_hook):
        op = CloudDataTransferServiceResumeOperationOperator(
            operation_name=OPERATION_NAME,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.resume_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        assert result is None

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_resume_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceResumeOperationOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'operation_name')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_resume_should_throw_ex_when_name_none(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = CloudDataTransferServiceResumeOperationOperator(operation_name="", task_id=TASK_ID)
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'operation_name' is empty or None" in str(err)
        mock_hook.assert_not_called()


class TestGcpStorageTransferOperationsCancelOperator(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_cancel(self, mock_hook):
        op = CloudDataTransferServiceCancelOperationOperator(
            operation_name=OPERATION_NAME,
            task_id=TASK_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(
            api_version='v1',
            gcp_conn_id='google_cloud_default',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.cancel_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        assert result is None

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_cancel_with_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceCancelOperationOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'operation_name')
        assert dag_id == getattr(op, 'gcp_conn_id')
        assert dag_id == getattr(op, 'api_version')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_operation_cancel_should_throw_ex_when_name_none(self, mock_hook):
        with pytest.raises(AirflowException) as ctx:
            op = CloudDataTransferServiceCancelOperationOperator(operation_name="", task_id=TASK_ID)
            op.execute(None)
        err = ctx.value
        assert "The required parameter 'operation_name' is empty or None" in str(err)
        mock_hook.assert_not_called()


class TestS3ToGoogleCloudStorageTransferOperator(unittest.TestCase):
    def test_constructor(self):
        operator = CloudDataTransferServiceS3ToGCSOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        assert operator.task_id == TASK_ID
        assert operator.s3_bucket == AWS_BUCKET_NAME
        assert operator.gcs_bucket == GCS_BUCKET_NAME
        assert operator.project_id == GCP_PROJECT_ID
        assert operator.description == DESCRIPTION
        assert operator.schedule == SCHEDULE_DICT

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceS3ToGCSOperator(
            s3_bucket='{{ dag.dag_id }}',
            gcs_bucket='{{ dag.dag_id }}',
            description='{{ dag.dag_id }}',
            object_conditions={'exclude_prefixes': ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 's3_bucket')
        assert dag_id == getattr(op, 'gcs_bucket')
        assert dag_id == getattr(op, 'description')

        # pylint: disable=unsubscriptable-object
        assert dag_id == getattr(op, 'object_conditions')['exclude_prefixes'][0]
        # pylint: enable=unsubscriptable-object

        assert dag_id == getattr(op, 'gcp_conn_id')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_execute(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        operator = CloudDataTransferServiceS3ToGCSOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        assert mock_transfer_hook.return_value.wait_for_transfer_job.called
        assert not mock_transfer_hook.return_value.delete_transfer_job.called

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_execute_skip_wait(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        operator = CloudDataTransferServiceS3ToGCSOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
            wait=False,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        assert not mock_transfer_hook.return_value.wait_for_transfer_job.called
        assert not mock_transfer_hook.return_value.delete_transfer_job.called

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_execute_delete_job_after_completion(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        operator = CloudDataTransferServiceS3ToGCSOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
            wait=True,
            delete_job_after_completion=True,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        assert mock_transfer_hook.return_value.wait_for_transfer_job.called
        assert mock_transfer_hook.return_value.delete_transfer_job.called

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    @mock.patch('airflow.providers.google.cloud.operators.cloud_storage_transfer_service.AwsBaseHook')
    def test_execute_should_throw_ex_when_delete_job_without_wait(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        with pytest.raises(AirflowException) as ctx:

            operator = CloudDataTransferServiceS3ToGCSOperator(
                task_id=TASK_ID,
                s3_bucket=AWS_BUCKET_NAME,
                gcs_bucket=GCS_BUCKET_NAME,
                description=DESCRIPTION,
                schedule=SCHEDULE_DICT,
                wait=False,
                delete_job_after_completion=True,
            )

            operator.execute(None)

        err = ctx.value
        assert "If 'delete_job_after_completion' is True, then 'wait' must also be True." in str(err)
        mock_aws_hook.assert_not_called()
        mock_transfer_hook.assert_not_called()


class TestGoogleCloudStorageToGoogleCloudStorageTransferOperator(unittest.TestCase):
    def test_constructor(self):
        operator = CloudDataTransferServiceGCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        assert operator.task_id == TASK_ID
        assert operator.source_bucket == GCS_BUCKET_NAME
        assert operator.destination_bucket == GCS_BUCKET_NAME
        assert operator.project_id == GCP_PROJECT_ID
        assert operator.description == DESCRIPTION
        assert operator.schedule == SCHEDULE_DICT

    # Setting all the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)  # pylint: disable=attribute-defined-outside-init
        op = CloudDataTransferServiceGCSToGCSOperator(
            source_bucket='{{ dag.dag_id }}',
            destination_bucket='{{ dag.dag_id }}',
            description='{{ dag.dag_id }}',
            object_conditions={'exclude_prefixes': ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        assert dag_id == getattr(op, 'source_bucket')
        assert dag_id == getattr(op, 'destination_bucket')
        assert dag_id == getattr(op, 'description')

        # pylint: disable=unsubscriptable-object
        assert dag_id == getattr(op, 'object_conditions')['exclude_prefixes'][0]
        # pylint: enable=unsubscriptable-object

        assert dag_id == getattr(op, 'gcp_conn_id')

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_execute(self, mock_transfer_hook):
        operator = CloudDataTransferServiceGCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        assert mock_transfer_hook.return_value.wait_for_transfer_job.called
        assert not mock_transfer_hook.return_value.delete_transfer_job.called

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_execute_skip_wait(self, mock_transfer_hook):
        operator = CloudDataTransferServiceGCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            wait=False,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        assert not mock_transfer_hook.return_value.wait_for_transfer_job.called
        assert not mock_transfer_hook.return_value.delete_transfer_job.called

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_execute_delete_job_after_completion(self, mock_transfer_hook):

        operator = CloudDataTransferServiceGCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
            wait=True,
            delete_job_after_completion=True,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        assert mock_transfer_hook.return_value.wait_for_transfer_job.called
        assert mock_transfer_hook.return_value.delete_transfer_job.called

    @mock.patch(
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceHook'
    )
    def test_execute_should_throw_ex_when_delete_job_without_wait(self, mock_transfer_hook):

        with pytest.raises(AirflowException) as ctx:

            operator = CloudDataTransferServiceS3ToGCSOperator(
                task_id=TASK_ID,
                s3_bucket=AWS_BUCKET_NAME,
                gcs_bucket=GCS_BUCKET_NAME,
                description=DESCRIPTION,
                schedule=SCHEDULE_DICT,
                wait=False,
                delete_job_after_completion=True,
            )

            operator.execute(None)

        err = ctx.value
        assert "If 'delete_job_after_completion' is True, then 'wait' must also be True." in str(err)
        mock_transfer_hook.assert_not_called()
