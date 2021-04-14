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
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor, S3KeySizeSensor


class TestS3KeySensor(unittest.TestCase):
    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key="file_in_bucket")
        with pytest.raises(AirflowException):
            op.poke(None)

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3KeySensor(
            task_id='s3_key_sensor', bucket_key="s3://test_bucket/file", bucket_name='test_bucket'
        )
        with pytest.raises(AirflowException):
            op.poke(None)

    @parameterized.expand(
        [
            ['s3://bucket/key', None, 'key', 'bucket'],
            ['key', 'bucket', 'key', 'bucket'],
        ]
    )
    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook')
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket, mock_hook):
        mock_hook.return_value.check_for_key.return_value = False

        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key=key,
            bucket_name=bucket,
        )

        op.poke(None)

        assert op.bucket_key == parsed_key
        assert op.bucket_name == parsed_bucket

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook')
    def test_parse_bucket_key_from_jinja(self, mock_hook):
        mock_hook.return_value.check_for_key.return_value = False

        Variable.set("test_bucket_key", "s3://bucket/key")

        execution_date = datetime(2020, 1, 1)

        dag = DAG("test_s3_key", start_date=execution_date)
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key='{{ var.value.test_bucket_key }}',
            bucket_name=None,
            dag=dag,
        )

        ti = TaskInstance(task=op, execution_date=execution_date)
        context = ti.get_template_context()
        ti.render_templates(context)

        op.poke(None)

        assert op.bucket_key == "key"
        assert op.bucket_name == "bucket"

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook')
    def test_poke(self, mock_hook):
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file')

        mock_check_for_key = mock_hook.return_value.check_for_key
        mock_check_for_key.return_value = False
        assert not op.poke(None)
        mock_check_for_key.assert_called_once_with(op.bucket_key, op.bucket_name)

        mock_hook.return_value.check_for_key.return_value = True
        assert op.poke(None)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook')
    def test_poke_wildcard(self, mock_hook):
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file', wildcard_match=True)

        mock_check_for_wildcard_key = mock_hook.return_value.check_for_wildcard_key
        mock_check_for_wildcard_key.return_value = False
        assert not op.poke(None)
        mock_check_for_wildcard_key.assert_called_once_with(op.bucket_key, op.bucket_name)

        mock_check_for_wildcard_key.return_value = True
        assert op.poke(None)


class TestS3KeySizeSensor(unittest.TestCase):
    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook.check_for_key', return_value=False)
    def test_poke_check_for_key_false(self, mock_check_for_key):
        op = S3KeySizeSensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file')
        assert not op.poke(None)
        mock_check_for_key.assert_called_once_with(op.bucket_key, op.bucket_name)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3KeySizeSensor.get_files', return_value=[])
    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook.check_for_key', return_value=True)
    def test_poke_get_files_false(self, mock_check_for_key, mock_get_files):
        op = S3KeySizeSensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file')
        assert not op.poke(None)
        mock_check_for_key.assert_called_once_with(op.bucket_key, op.bucket_name)
        mock_get_files.assert_called_once_with(s3_hook=op.get_hook())

    @parameterized.expand(
        [
            [{"Contents": [{"Size": 0}, {"Size": 0}]}, False],
            [{"Contents": [{"Size": 0}]}, False],
            [{"Contents": []}, False],
            [{"Contents": [{"Size": 10}]}, True],
            [{"Contents": [{"Size": 10}, {"Size": 0}]}, False],
            [{"Contents": [{"Size": 10}, {"Size": 10}]}, True],
        ]
    )
    @mock.patch('airflow.providers.amazon.aws.sensors.s3_key.S3Hook')
    def test_poke(self, paginate_return_value, poke_return_value, mock_hook):
        op = S3KeySizeSensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file')

        mock_check_for_key = mock_hook.return_value.check_for_key
        mock_hook.return_value.check_for_key.return_value = True
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = []
        mock_conn = mock.Mock()
        # pylint: disable=no-member
        mock_conn.return_value.get_paginator.return_value = mock_paginator
        mock_hook.return_value.get_conn = mock_conn
        mock_paginator.paginate.return_value = [paginate_return_value]
        assert op.poke(None) is poke_return_value
        mock_check_for_key.assert_called_once_with(op.bucket_key, op.bucket_name)
