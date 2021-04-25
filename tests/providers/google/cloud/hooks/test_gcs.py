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
import copy
import io
import os
import re
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

import dateutil
import pytest
from google.cloud import exceptions, storage

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks import gcs
from airflow.providers.google.cloud.hooks.gcs import _fallback_object_url_to_object_name_and_bucket_name
from airflow.utils import timezone
from airflow.version import version
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = 'airflow.providers.google.common.hooks.base_google.{}'
GCS_STRING = 'airflow.providers.google.cloud.hooks.gcs.{}'

EMPTY_CONTENT = b''
PROJECT_ID_TEST = 'project-id'


class TestGCSHookHelperFunctions(unittest.TestCase):
    def test_parse_gcs_url(self):
        """
        Test GCS url parsing
        """

        assert gcs._parse_gcs_url('gs://bucket/path/to/blob') == ('bucket', 'path/to/blob')

        # invalid URI
        with pytest.raises(AirflowException):
            gcs._parse_gcs_url('gs:/bucket/path/to/blob')
        with pytest.raises(AirflowException):
            gcs._parse_gcs_url('http://google.com/aaa')

        # trailing slash
        assert gcs._parse_gcs_url('gs://bucket/path/to/blob/') == ('bucket', 'path/to/blob/')

        # bucket only
        assert gcs._parse_gcs_url('gs://bucket/') == ('bucket', '')


class TestFallbackObjectUrlToObjectNameAndBucketName(unittest.TestCase):
    def setUp(self) -> None:
        self.assertion_on_body = mock.MagicMock()

        @_fallback_object_url_to_object_name_and_bucket_name()
        def test_method(_, bucket_name=None, object_name=None, object_url=None):
            assert object_name == "OBJECT_NAME"
            assert bucket_name == "BUCKET_NAME"
            assert object_url is None
            self.assertion_on_body()

        self.test_method = test_method

    def test_should_url(self):
        self.test_method(None, object_url="gs://BUCKET_NAME/OBJECT_NAME")
        self.assertion_on_body.assert_called_once()

    def test_should_support_bucket_and_object(self):
        self.test_method(None, bucket_name="BUCKET_NAME", object_name="OBJECT_NAME")
        self.assertion_on_body.assert_called_once()

    def test_should_raise_exception_on_missing(self):
        with pytest.raises(
            TypeError,
            match=re.escape(
                "test_method() missing 2 required positional arguments: 'bucket_name' and 'object_name'"
            ),
        ):
            self.test_method(None)
        self.assertion_on_body.assert_not_called()

    def test_should_raise_exception_on_mutually_exclusive(self):
        with pytest.raises(AirflowException, match=re.escape("The mutually exclusive parameters.")):
            self.test_method(
                None,
                bucket_name="BUCKET_NAME",
                object_name="OBJECT_NAME",
                object_url="gs://BUCKET_NAME/OBJECT_NAME",
            )
        self.assertion_on_body.assert_not_called()


class TestGCSHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            GCS_STRING.format('GoogleBaseHook.__init__'),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gcs_hook = gcs.GCSHook(gcp_conn_id='test')

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.client_info',
        new_callable=mock.PropertyMock,
        return_value="CLIENT_INFO",
    )
    @mock.patch(
        BASE_STRING.format("GoogleBaseHook._get_credentials_and_project_id"),
        return_value=("CREDENTIALS", "PROJECT_ID"),
    )
    @mock.patch(GCS_STRING.format('GoogleBaseHook.get_connection'))
    @mock.patch('google.cloud.storage.Client')
    def test_storage_client_creation(
        self, mock_client, mock_get_connection, mock_get_creds_and_project_id, mock_client_info
    ):
        hook = gcs.GCSHook()
        result = hook.get_conn()
        # test that Storage Client is called with required arguments
        mock_client.assert_called_once_with(
            client_info="CLIENT_INFO", credentials="CREDENTIALS", project="PROJECT_ID"
        )
        assert mock_client.return_value == result

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_exists(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        bucket_mock = mock_service.return_value.bucket
        blob_object = bucket_mock.return_value.blob
        exists_method = blob_object.return_value.exists
        exists_method.return_value = True

        # When
        response = self.gcs_hook.exists(bucket_name=test_bucket, object_name=test_object)

        # Then
        assert response
        bucket_mock.assert_called_once_with(test_bucket)
        blob_object.assert_called_once_with(blob_name=test_object)
        exists_method.assert_called_once_with()

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_exists_nonexisting_object(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        bucket_mock = mock_service.return_value.bucket
        blob_object = bucket_mock.return_value.blob
        exists_method = blob_object.return_value.exists
        exists_method.return_value = False

        # When
        response = self.gcs_hook.exists(bucket_name=test_bucket, object_name=test_object)

        # Then
        assert not response

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_is_updated_after(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2019, 8, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_updated_after(
            bucket_name=test_bucket, object_name=test_object, ts=datetime(2018, 1, 1, 1, 1, 1)
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_is_updated_before(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2019, 8, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_updated_before(
            bucket_name=test_bucket, object_name=test_object, ts=datetime(2020, 1, 1, 1, 1, 1)
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_is_updated_between(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2019, 8, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_updated_between(
            bucket_name=test_bucket,
            object_name=test_object,
            min_ts=datetime(2018, 1, 1, 1, 1, 1),
            max_ts=datetime(2020, 1, 1, 1, 1, 1),
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_is_older_than_with_true_cond(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2020, 1, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_older_than(
            bucket_name=test_bucket, object_name=test_object, seconds=86400  # 24hr
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_is_older_than_with_false_cond(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Given
        # fmt: off
        mock_service.return_value.bucket.return_value.get_blob \
            .return_value.updated = timezone.utcnow() + timedelta(days=2)
        # fmt: on
        # When
        response = self.gcs_hook.is_older_than(
            bucket_name=test_bucket, object_name=test_object, seconds=86400  # 24hr
        )
        # Then
        assert not response

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_copy(self, mock_service, mock_bucket):
        source_bucket = 'test-source-bucket'
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        destination_bucket_instance = mock_bucket
        source_blob = mock_bucket.blob(source_object)
        destination_blob = storage.Blob(bucket=destination_bucket_instance, name=destination_object)

        # Given
        bucket_mock = mock_service.return_value.bucket
        bucket_mock.return_value = mock_bucket
        copy_method = bucket_mock.return_value.copy_blob
        copy_method.return_value = destination_blob

        # When
        response = self.gcs_hook.copy(  # pylint: disable=assignment-from-no-return
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        # Then
        assert response is None
        copy_method.assert_called_once_with(
            blob=source_blob, destination_bucket=destination_bucket_instance, new_name=destination_object
        )

    def test_copy_fail_same_source_and_destination(self):
        source_bucket = 'test-source-bucket'
        source_object = 'test-source-object'
        destination_bucket = 'test-source-bucket'
        destination_object = 'test-source-object'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

        assert str(ctx.value) == (
            'Either source/destination bucket or source/destination object must be different, '
            'not both the same: bucket={}, object={}'
        ).format(
            source_bucket,
            source_object,
        )

    def test_copy_empty_source_bucket(self):
        source_bucket = None
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

        assert str(ctx.value) == 'source_bucket and source_object cannot be empty.'

    def test_copy_empty_source_object(self):
        source_bucket = 'test-source-object'
        source_object = None
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

        assert str(ctx.value) == 'source_bucket and source_object cannot be empty.'

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_rewrite(self, mock_service, mock_bucket):
        source_bucket = 'test-source-bucket'
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        source_blob = mock_bucket.blob(source_object)

        # Given
        bucket_mock = mock_service.return_value.bucket
        bucket_mock.return_value = mock_bucket
        get_blob_method = bucket_mock.return_value.blob
        rewrite_method = get_blob_method.return_value.rewrite
        rewrite_method.side_effect = [(None, mock.ANY, mock.ANY), (mock.ANY, mock.ANY, mock.ANY)]

        # When
        response = self.gcs_hook.rewrite(  # pylint: disable=assignment-from-no-return
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        # Then
        assert response is None
        rewrite_method.assert_called_once_with(source=source_blob)

    def test_rewrite_empty_source_bucket(self):
        source_bucket = None
        source_object = 'test-source-object'
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.rewrite(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

        assert str(ctx.value) == 'source_bucket and source_object cannot be empty.'

    def test_rewrite_empty_source_object(self):
        source_bucket = 'test-source-object'
        source_object = None
        destination_bucket = 'test-dest-bucket'
        destination_object = 'test-dest-object'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.rewrite(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

        assert str(ctx.value) == 'source_bucket and source_object cannot be empty.'

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_delete(self, mock_service, mock_bucket):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        blob_to_be_deleted = storage.Blob(name=test_object, bucket=mock_bucket)

        get_bucket_method = mock_service.return_value.get_bucket
        get_blob_method = get_bucket_method.return_value.get_blob
        delete_method = get_blob_method.return_value.delete
        delete_method.return_value = blob_to_be_deleted

        response = self.gcs_hook.delete(  # pylint: disable=assignment-from-no-return
            bucket_name=test_bucket, object_name=test_object
        )
        assert response is None

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_delete_nonexisting_object(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        bucket_method = mock_service.return_value.bucket
        blob = bucket_method.return_value.blob
        delete_method = blob.return_value.delete
        delete_method.side_effect = exceptions.NotFound(message="Not Found")

        with pytest.raises(exceptions.NotFound):
            self.gcs_hook.delete(bucket_name=test_bucket, object_name=test_object)

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_delete_bucket(self, mock_service):
        test_bucket = "test bucket"

        self.gcs_hook.delete_bucket(bucket_name=test_bucket)

        mock_service.return_value.bucket.assert_called_once_with(test_bucket)
        mock_service.return_value.bucket.return_value.delete.assert_called_once()

    @mock.patch(
        GCS_STRING.format('GCSHook.get_conn'),
        **{'return_value.bucket.return_value.delete.side_effect': exceptions.NotFound(message="Not Found")},
    )
    def test_delete_nonexisting_bucket(self, mock_service):
        test_bucket = "test bucket"

        self.gcs_hook.delete_bucket(bucket_name=test_bucket)

        mock_service.return_value.bucket.assert_called_once_with(test_bucket)
        mock_service.return_value.bucket.return_value.delete.assert_called_once()

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_object_get_size(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        returned_file_size = 1200

        bucket_method = mock_service.return_value.bucket
        get_blob_method = bucket_method.return_value.get_blob
        get_blob_method.return_value.size = returned_file_size

        response = self.gcs_hook.get_size(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_size

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_object_get_crc32c(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        returned_file_crc32c = "xgdNfQ=="

        bucket_method = mock_service.return_value.bucket
        get_blob_method = bucket_method.return_value.get_blob
        get_blob_method.return_value.crc32c = returned_file_crc32c

        response = self.gcs_hook.get_crc32c(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_crc32c

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_object_get_md5hash(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        returned_file_md5hash = "leYUJBUWrRtks1UeUFONJQ=="

        bucket_method = mock_service.return_value.bucket
        get_blob_method = bucket_method.return_value.get_blob
        get_blob_method.return_value.md5_hash = returned_file_md5hash

        response = self.gcs_hook.get_md5hash(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_md5hash

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_create_bucket(self, mock_service, mock_bucket):
        test_bucket = 'test_bucket'
        test_project = 'test-project'
        test_location = 'EU'
        test_labels = {'env': 'prod'}
        test_storage_class = 'MULTI_REGIONAL'

        labels_with_version = copy.deepcopy(test_labels)
        labels_with_version['airflow-version'] = 'v' + version.replace('.', '-').replace('+', '-')

        mock_service.return_value.bucket.return_value.create.return_value = None
        mock_bucket.return_value.storage_class = test_storage_class
        mock_bucket.return_value.labels = labels_with_version

        sample_bucket = mock_service().bucket(bucket_name=test_bucket)

        response = self.gcs_hook.create_bucket(
            bucket_name=test_bucket,
            storage_class=test_storage_class,
            location=test_location,
            labels=test_labels,
            project_id=test_project,
        )

        assert response == sample_bucket.id

        assert sample_bucket.storage_class == test_storage_class
        assert sample_bucket.labels == test_labels

        mock_service.return_value.bucket.return_value.create.assert_called_once_with(
            project=test_project, location=test_location
        )

    @mock.patch('google.cloud.storage.Bucket')
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_create_bucket_with_resource(self, mock_service, mock_bucket):
        test_bucket = 'test_bucket'
        test_project = 'test-project'
        test_location = 'EU'
        test_labels = {'env': 'prod'}
        test_storage_class = 'MULTI_REGIONAL'
        test_versioning_enabled = {"enabled": True}

        mock_service.return_value.bucket.return_value.create.return_value = None
        mock_bucket.return_value.storage_class = test_storage_class
        mock_bucket.return_value.labels = test_labels
        mock_bucket.return_value.versioning_enabled = True

        sample_bucket = mock_service().bucket(bucket_name=test_bucket)

        # sample_bucket = storage.Bucket(client=mock_service, name=test_bucket)
        # Assert for resource other than None.
        response = self.gcs_hook.create_bucket(
            bucket_name=test_bucket,
            resource={"versioning": test_versioning_enabled},
            storage_class=test_storage_class,
            location=test_location,
            labels=test_labels,
            project_id=test_project,
        )
        assert response == sample_bucket.id

        mock_service.return_value.bucket.return_value._patch_property.assert_called_once_with(
            name='versioning', value=test_versioning_enabled
        )

        mock_service.return_value.bucket.return_value.create.assert_called_once_with(
            project=test_project, location=test_location
        )

    @mock.patch('google.cloud.storage.Bucket.blob')
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_compose(self, mock_service, mock_blob):
        test_bucket = 'test_bucket'
        test_source_objects = ['test_object_1', 'test_object_2', 'test_object_3']
        test_destination_object = 'test_object_composed'

        mock_service.return_value.bucket.return_value.blob.return_value = mock_blob(blob_name=mock.ANY)
        method = mock_service.return_value.bucket.return_value.blob.return_value.compose

        self.gcs_hook.compose(
            bucket_name=test_bucket,
            source_objects=test_source_objects,
            destination_object=test_destination_object,
        )

        method.assert_called_once_with(
            sources=[mock_blob(blob_name=source_object) for source_object in test_source_objects]
        )

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_compose_with_empty_source_objects(self, mock_service):  # pylint: disable=unused-argument
        test_bucket = 'test_bucket'
        test_source_objects = []
        test_destination_object = 'test_object_composed'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object,
            )

        assert str(ctx.value) == 'source_objects cannot be empty.'

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_compose_without_bucket(self, mock_service):  # pylint: disable=unused-argument
        test_bucket = None
        test_source_objects = ['test_object_1', 'test_object_2', 'test_object_3']
        test_destination_object = 'test_object_composed'

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object,
            )

        assert str(ctx.value) == 'bucket_name and destination_object cannot be empty.'

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_compose_without_destination_object(self, mock_service):  # pylint: disable=unused-argument
        test_bucket = 'test_bucket'
        test_source_objects = ['test_object_1', 'test_object_2', 'test_object_3']
        test_destination_object = None

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object,
            )

        assert str(ctx.value) == 'bucket_name and destination_object cannot be empty.'

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_download_as_string(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        test_object_bytes = io.BytesIO(b"input")

        download_method = mock_service.return_value.bucket.return_value.blob.return_value.download_as_string
        download_method.return_value = test_object_bytes

        response = self.gcs_hook.download(bucket_name=test_bucket, object_name=test_object, filename=None)

        assert response == test_object_bytes
        download_method.assert_called_once_with()

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_download_to_file(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        test_object_bytes = io.BytesIO(b"input")
        test_file = 'test_file'

        download_filename_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_to_filename
        )
        download_filename_method.return_value = None

        download_as_a_string_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_as_string
        )
        download_as_a_string_method.return_value = test_object_bytes
        response = self.gcs_hook.download(
            bucket_name=test_bucket, object_name=test_object, filename=test_file
        )

        assert response == test_file
        download_filename_method.assert_called_once_with(test_file, timeout=60)

    @mock.patch(GCS_STRING.format('NamedTemporaryFile'))
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_provide_file(self, mock_service, mock_temp_file):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        test_object_bytes = io.BytesIO(b"input")
        test_file = 'test_file'

        download_filename_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_to_filename
        )
        download_filename_method.return_value = None

        download_as_a_string_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_as_string
        )
        download_as_a_string_method.return_value = test_object_bytes
        mock_temp_file.return_value.__enter__.return_value = mock.MagicMock()
        mock_temp_file.return_value.__enter__.return_value.name = test_file

        with self.gcs_hook.provide_file(bucket_name=test_bucket, object_name=test_object) as response:

            assert test_file == response.name
        download_filename_method.assert_called_once_with(test_file, timeout=60)
        mock_temp_file.assert_has_calls(
            [
                mock.call(suffix='test_object'),
                mock.call().__enter__(),
                mock.call().__enter__().flush(),
                mock.call().__exit__(None, None, None),
            ]
        )

    @mock.patch(GCS_STRING.format('NamedTemporaryFile'))
    @mock.patch(GCS_STRING.format('GCSHook.upload'))
    def test_provide_file_upload(self, mock_upload, mock_temp_file):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        test_file = 'test_file'

        mock_temp_file.return_value.__enter__.return_value = mock.MagicMock()
        mock_temp_file.return_value.__enter__.return_value.name = test_file

        with self.gcs_hook.provide_file_and_upload(
            bucket_name=test_bucket, object_name=test_object
        ) as fhandle:
            assert fhandle.name == test_file
            fhandle.write()

        mock_upload.assert_called_once_with(
            bucket_name=test_bucket, object_name=test_object, filename=test_file
        )
        mock_temp_file.assert_has_calls(
            [
                mock.call(suffix='test_object'),
                mock.call().__enter__(),
                mock.call().__enter__().write(),
                mock.call().__enter__().flush(),
                mock.call().__exit__(None, None, None),
            ]
        )

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_list_by_timespans(self, mock_service):
        test_bucket = 'test_bucket'

        # Given
        blob1 = mock.Mock()
        blob1.name = "in-interval-1"
        blob1.updated = datetime(2019, 8, 28, 14, 7, 20, 0, dateutil.tz.tzutc())
        blob2 = mock.Mock()
        blob2.name = "in-interval-2"
        blob2.updated = datetime(2019, 8, 28, 14, 37, 20, 0, dateutil.tz.tzutc())
        blob3 = mock.Mock()
        blob3.name = "outside-interval"
        blob3.updated = datetime(2019, 8, 29, 14, 7, 20, 0, dateutil.tz.tzutc())

        mock_service.return_value.bucket.return_value.list_blobs.return_value.__iter__.return_value = [
            blob1,
            blob2,
            blob3,
        ]
        mock_service.return_value.bucket.return_value.list_blobs.return_value.prefixes = None
        mock_service.return_value.bucket.return_value.list_blobs.return_value.next_page_token = None

        # When
        response = self.gcs_hook.list_by_timespan(
            bucket_name=test_bucket,
            timespan_start=datetime(2019, 8, 28, 14, 0, 0, 0, dateutil.tz.tzutc()),
            timespan_end=datetime(2019, 8, 28, 15, 0, 0, 0, dateutil.tz.tzutc()),
        )

        # Then
        assert len(response) == 2 and all('in-interval' in b for b in response)


class TestGCSHookUpload(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleBaseHook.__init__')):
            self.gcs_hook = gcs.GCSHook(gcp_conn_id='test')

        # generate a 384KiB test file (larger than the minimum 256KiB multipart chunk size)
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"x" * 393216)
        self.testfile.flush()
        self.testdata_bytes = b"x" * 393216
        self.testdata_str = "x" * 393216

    def tearDown(self):
        os.unlink(self.testfile.name)

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_file(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_filename

        self.gcs_hook.upload(test_bucket, test_object, filename=self.testfile.name)

        upload_method.assert_called_once_with(
            filename=self.testfile.name, content_type='application/octet-stream', timeout=60
        )

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_file_gzip(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        self.gcs_hook.upload(test_bucket, test_object, filename=self.testfile.name, gzip=True)
        assert not os.path.exists(self.testfile.name + '.gz')

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_data_str(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=self.testdata_str)

        upload_method.assert_called_once_with(self.testdata_str, content_type='text/plain', timeout=60)

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_data_bytes(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=self.testdata_bytes)

        upload_method.assert_called_once_with(self.testdata_bytes, content_type='text/plain', timeout=60)

    @mock.patch(GCS_STRING.format('BytesIO'))
    @mock.patch(GCS_STRING.format('gz.GzipFile'))
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_data_str_gzip(self, mock_service, mock_gzip, mock_bytes_io):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        encoding = 'utf-8'

        gzip_ctx = mock_gzip.return_value.__enter__.return_value
        data = mock_bytes_io.return_value.getvalue.return_value
        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=self.testdata_str, gzip=True)

        byte_str = bytes(self.testdata_str, encoding)
        mock_gzip.assert_called_once_with(fileobj=mock_bytes_io.return_value, mode="w")
        gzip_ctx.write.assert_called_once_with(byte_str)
        upload_method.assert_called_once_with(data, content_type='text/plain', timeout=60)

    @mock.patch(GCS_STRING.format('BytesIO'))
    @mock.patch(GCS_STRING.format('gz.GzipFile'))
    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_data_bytes_gzip(self, mock_service, mock_gzip, mock_bytes_io):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        gzip_ctx = mock_gzip.return_value.__enter__.return_value
        data = mock_bytes_io.return_value.getvalue.return_value
        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=self.testdata_bytes, gzip=True)

        mock_gzip.assert_called_once_with(fileobj=mock_bytes_io.return_value, mode="w")
        gzip_ctx.write.assert_called_once_with(self.testdata_bytes)
        upload_method.assert_called_once_with(data, content_type='text/plain', timeout=60)

    @mock.patch(GCS_STRING.format('GCSHook.get_conn'))
    def test_upload_exceptions(self, mock_service):
        test_bucket = 'test_bucket'
        test_object = 'test_object'
        both_params_except = (
            "'filename' and 'data' parameter provided. Please "
            "specify a single parameter, either 'filename' for "
            "local file uploads or 'data' for file content uploads."
        )
        no_params_except = "'filename' and 'data' parameter missing. One is required to upload to gcs."

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.upload(test_bucket, test_object)
        assert no_params_except == str(ctx.value)

        with pytest.raises(ValueError) as ctx:
            self.gcs_hook.upload(
                test_bucket, test_object, filename=self.testfile.name, data=self.testdata_str
            )
        assert both_params_except == str(ctx.value)


class TestSyncGcsHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            GCS_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gcs_hook = gcs.GCSHook(gcp_conn_id="test")

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_do_nothing_when_buckets_is_empty(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET")
        mock_get_conn.return_value.bucket.assert_has_calls(
            [mock.call("SOURCE_BUCKET"), mock.call("DEST_BUCKET")]
        )
        source_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix=None)
        destination_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix=None)
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_append_slash_to_object_if_missing(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            source_object="SOURCE_OBJECT",
            destination_object="DESTINATION_OBJECT",
        )
        source_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix="SOURCE_OBJECT/")
        destination_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix="DESTINATION_OBJECT/")

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET")
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_has_calls(
            [
                mock.call(
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                ),
                mock.call(
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                ),
            ],
            any_order=True,
        )

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files_non_recursive(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("AAA/FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", recursive=False)
        source_bucket.list_blobs.assert_called_once_with(delimiter='/', prefix=None)
        destination_bucket.list_blobs.assert_called_once_with(delimiter='/', prefix=None)

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files_to_subdirectory(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", destination_object="DEST_OBJ/"
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_B",
                ),
            ],
            any_order=True,
        )

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files_from_subdirectory(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1"),
            self._create_blob("SRC_OBJ/FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", source_object="SRC_OBJ/"
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                ),
            ],
            any_order=True,
        )

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C2"),
            self._create_blob("FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", allow_overwrite=True
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                ),
                mock.call(
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_files_to_subdirectory(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("DEST_OBJ/FILE_A", "C2"),
            self._create_blob("DEST_OBJ/FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            destination_object="DEST_OBJ/",
            allow_overwrite=True,
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_files_from_subdirectory(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1"),
            self._create_blob("SRC_OBJ/FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C2"),
            self._create_blob("FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            source_object="SRC_OBJ/",
            allow_overwrite=True,
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_delete_extra_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", destination_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C1", destination_bucket),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", delete_extra_files=True
        )
        mock_delete.assert_has_calls(
            [mock.call("DEST_BUCKET", "SRC_OBJ/FILE_B"), mock.call("DEST_BUCKET", "SRC_OBJ/FILE_A")],
            any_order=True,
        )
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_not_delete_extra_files_when_delete_extra_files_is_disabled(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", destination_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C1", destination_bucket),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", delete_extra_files=False
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_not_overwrite_when_overwrite_is_disabled(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        # mock_get_conn.return_value =
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", source_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C2", source_bucket),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", destination_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C2", destination_bucket),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            delete_extra_files=False,
            allow_overwrite=False,
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    def _create_blob(self, name: str, crc32: str, bucket=None):
        blob = mock.MagicMock(name=f"BLOB:{name}")
        blob.name = name
        blob.crc32 = crc32
        blob.bucket = bucket
        return blob

    def _create_bucket(self, name: str):
        bucket = mock.MagicMock(name=f"BUCKET:{name}")
        bucket.name = name
        return bucket
