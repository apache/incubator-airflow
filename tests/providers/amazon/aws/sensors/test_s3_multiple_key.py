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

import unittest
from datetime import datetime
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.sensors.s3_multiple_key import S3MultipleKeySensor


class TestS3MultipleKeySensor(unittest.TestCase):
    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3MultipleKeySensor(
            task_id='s3_key_sensor', bucket_keys=["file_1_in_bucket", "file_2_in_bucket"]
        )
        with pytest.raises(AirflowException):
            op.poke(None)

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3MultipleKeySensor(
            task_id='s3_key_sensor',
            bucket_keys=["s3://test_bucket/file", "s3://test_bucket/file_2"],
            bucket_name='test_bucket',
        )
        with pytest.raises(AirflowException):
            op.poke(None)

    @parameterized.expand(
        [
            [['s3://bucket/key'], None, ['s3://bucket/key'], 'bucket'],
            [
                ['s3://bucket/key_1', 's3://bucket/key_2'],
                None,
                ['s3://bucket/key_1', 's3://bucket/key_2'],
                'bucket',
            ],
            [['key_1'], 'bucket', ['key_1'], 'bucket'],
            [['key_1, key_2'], 'bucket', ['key_1, key_2'], 'bucket'],
        ]
    )
    @mock.patch('airflow.providers.amazon.aws.sensors.s3_multiple_key.S3Hook')
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket, mock_hook):
        mock_hook.return_value.check_for_key.return_value = False

        op = S3MultipleKeySensor(
            task_id='s3_key_sensor',
            bucket_keys=key,
            bucket_name=bucket,
        )

        op.poke(None)

        assert op.bucket_keys == parsed_key
        assert op.bucket_name == parsed_bucket

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_multiple_key.S3Hook')
    def test_parse_bucket_key_from_jinja(self, mock_hook):
        mock_hook.return_value.check_for_key.return_value = False

        Variable.set("test_bucket_key_1", "s3://bucket/key_1")
        Variable.set("test_bucket_key_2", "s3://bucket/key_2")

        execution_date = datetime(2020, 1, 1)

        dag = DAG("test_s3_key", start_date=execution_date)
        op = S3MultipleKeySensor(
            task_id='s3_key_sensor',
            bucket_keys=['{{ var.value.test_bucket_key_1 }}', '{{ var.value.test_bucket_key_2 }}'],
            bucket_name=None,
            dag=dag,
        )

        ti = TaskInstance(task=op, execution_date=execution_date)
        context = ti.get_template_context()
        ti.render_templates(context)

        op.poke(None)

        assert op.bucket_keys == ["s3://bucket/key_1", "s3://bucket/key_2"]
        assert op.bucket_name == "bucket"

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_multiple_key.S3Hook')
    def test_poke_for_nonexistent_keys(self, mock_hook):
        op = S3MultipleKeySensor(
            task_id='s3_key_sensor', bucket_keys=['s3://test_bucket/file_1', 's3://test_bucket/file_2']
        )

        mock_check_for_key = mock_hook.return_value.check_for_key
        mock_check_for_key.return_value = False
        assert not op.poke(None)
        mock_check_for_key.assert_called_once_with('file_1', op.bucket_name)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_multiple_key.S3Hook')
    def test_poke_for_found_keys(self, mock_hook):
        op = S3MultipleKeySensor(task_id='s3_key_sensor', bucket_keys=['s3://test_bucket/file'])

        mock_check_for_key = mock_hook.return_value.check_for_key
        mock_check_for_key.return_value = True
        assert op.poke(None)
        mock_check_for_key.assert_called_once_with('file', op.bucket_name)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_multiple_key.S3Hook')
    def test_poke_wildcard_for_nonexistent_key(self, mock_hook):
        op = S3MultipleKeySensor(
            task_id='s3_key_sensor', bucket_keys=['s3://test_bucket/file'], wildcard_match=True
        )

        mock_check_for_wildcard_key = mock_hook.return_value.check_for_wildcard_key
        mock_check_for_wildcard_key.return_value = False
        assert not op.poke(None)
        mock_check_for_wildcard_key.assert_called_once_with('file', op.bucket_name)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_multiple_key.S3Hook')
    def test_poke_wildcard_for_found_key(self, mock_hook):
        op = S3MultipleKeySensor(
            task_id='s3_key_sensor', bucket_keys=['s3://test_bucket/file'], wildcard_match=True
        )

        mock_check_for_wildcard_key = mock_hook.return_value.check_for_wildcard_key
        mock_check_for_wildcard_key.return_value = True
        assert op.poke(None)
        mock_check_for_wildcard_key.assert_called_once_with('file', op.bucket_name)
