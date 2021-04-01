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
from datetime import datetime, timedelta, timezone
from unittest import TestCase, mock

import pendulum
import pytest

from airflow.exceptions import AirflowSensorTimeout
from airflow.models.dag import DAG, AirflowException
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
    ts_function,
)

TEST_BUCKET = "TEST_BUCKET"

TEST_OBJECT = "TEST_OBJECT"

TEST_DELEGATE_TO = "TEST_DELEGATE_TO"

TEST_GCP_CONN_ID = 'TEST_GCP_CONN_ID'

TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

TEST_PREFIX = "TEST_PREFIX"

TEST_DAG_ID = 'unit_tests_gcs_sensor'

DEFAULT_DATE = datetime(2015, 1, 1)

MOCK_DATE_ARRAY = [datetime(2019, 2, 24, 12, 0, 0) - i * timedelta(seconds=10) for i in range(25)]


def next_time_side_effect():
    """
    This each time this is called mock a time 10 seconds later
    than the previous call.
    """
    return MOCK_DATE_ARRAY.pop()


mock_time = mock.Mock(side_effect=next_time_side_effect)


class TestGoogleCloudStorageObjectSensor(TestCase):
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GCSObjectExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.exists.return_value = True

        result = task.poke(mock.MagicMock())

        assert result is True
        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.exists.assert_called_once_with(TEST_BUCKET, TEST_OBJECT)


class TestTsFunction(TestCase):
    def test_should_support_datetime(self):
        context = {
            'dag': DAG(dag_id=TEST_DAG_ID, schedule_interval=timedelta(days=5)),
            'execution_date': datetime(2019, 2, 14, 0, 0),
        }
        result = ts_function(context)
        assert datetime(2019, 2, 19, 0, 0, tzinfo=timezone.utc) == result

    def test_should_support_cron(self):
        dag = DAG(dag_id=TEST_DAG_ID, start_date=datetime(2019, 2, 19, 0, 0), schedule_interval='@weekly')

        context = {
            'dag': dag,
            'execution_date': datetime(2019, 2, 19),
        }
        result = ts_function(context)
        assert pendulum.instance(datetime(2019, 2, 24)).isoformat() == result.isoformat()


class TestGoogleCloudStorageObjectUpdatedSensor(TestCase):
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GCSObjectUpdateSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.is_updated_after.return_value = True
        result = task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.is_updated_after.assert_called_once_with(TEST_BUCKET, TEST_OBJECT, mock.ANY)
        assert result is True


class TestGoogleCloudStoragePrefixSensor(TestCase):
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_pass_arguments_to_hook(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list.return_value = ["NOT_EMPTY_LIST"]
        result = task.poke(mock.MagicMock)

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)
        assert result is True

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_return_false_on_empty_list(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )
        mock_hook.return_value.list.return_value = []
        result = task.poke(mock.MagicMock)

        assert result is False

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.GCSHook')
    def test_execute(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poke_interval=0,
        )
        generated_messages = [f'test-prefix/obj{i}' for i in range(5)]
        mock_hook.return_value.list.return_value = generated_messages

        response = task.execute(None)

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)
        assert response == generated_messages

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.GCSHook')
    def test_execute_timeout(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id", bucket=TEST_BUCKET, prefix=TEST_PREFIX, poke_interval=0, timeout=1
        )
        mock_hook.return_value.list.return_value = []
        with pytest.raises(AirflowSensorTimeout):
            task.execute(mock.MagicMock)
            mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)


class TestGCSUploadSessionCompleteSensor(TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.dag = dag

        self.sensor = GCSUploadSessionCompleteSensor(
            task_id='sensor_1',
            bucket='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=False,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            dag=self.dag,
        )

        self.last_mocked_date = datetime(2019, 4, 24, 0, 0, 0)

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_get_gcs_hook(self, mock_hook):
        self.sensor._get_gcs_hook()
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        assert mock_hook.return_value == self.sensor.hook

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.get_time', mock_time)
    def test_files_deleted_between_pokes_throw_error(self):
        self.sensor.is_bucket_updated({'a', 'b'})
        with pytest.raises(AirflowException):
            self.sensor.is_bucket_updated({'a'})

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.get_time', mock_time)
    def test_files_deleted_between_pokes_allow_delete(self):
        self.sensor = GCSUploadSessionCompleteSensor(
            task_id='sensor_2',
            bucket='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=True,
            dag=self.dag,
        )
        self.sensor.is_bucket_updated({'a', 'b'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a'})
        assert len(self.sensor.previous_objects) == 1
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a', 'c'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a', 'd'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a', 'd'})
        assert self.sensor.inactivity_seconds == 10
        assert self.sensor.is_bucket_updated({'a', 'd'})

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.get_time', mock_time)
    def test_incoming_data(self):
        self.sensor.is_bucket_updated({'a'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a', 'b'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a', 'b', 'c'})
        assert self.sensor.inactivity_seconds == 0

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.get_time', mock_time)
    def test_no_new_data(self):
        self.sensor.is_bucket_updated({'a'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a'})
        assert self.sensor.inactivity_seconds == 10

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.get_time', mock_time)
    def test_no_new_data_success_criteria(self):
        self.sensor.is_bucket_updated({'a'})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({'a'})
        assert self.sensor.inactivity_seconds == 10
        assert self.sensor.is_bucket_updated({'a'})

    @mock.patch('airflow.providers.google.cloud.sensors.gcs.get_time', mock_time)
    def test_not_enough_objects(self):
        self.sensor.is_bucket_updated(set())
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated(set())
        assert self.sensor.inactivity_seconds == 10
        assert not self.sensor.is_bucket_updated(set())
