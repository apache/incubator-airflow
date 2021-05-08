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
from unittest import mock

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.transfers.mongo_to_s3 import MongoToS3Operator
from airflow.utils import timezone

TASK_ID = 'test_mongo_to_s3_operator'
MONGO_CONN_ID = 'default_mongo'
AWS_CONN_ID = 'default_s3'
MONGO_COLLECTION = 'example_collection'
MONGO_QUERY = {"$lt": "{{ ts + 'Z' }}"}
S3_BUCKET = 'example_bucket'
S3_KEY = 'example_key'
COMPRESSION = None

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
MOCK_MONGO_RETURN = [
    {'example_return_key_1': 'example_return_value_1'},
    {'example_return_key_2': 'example_return_value_2'},
]


class TestMongoToS3Operator(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        self.dag = DAG('test_dag_id', default_args=args)

        self.mock_operator = MongoToS3Operator(
            task_id=TASK_ID,
            mongo_conn_id=MONGO_CONN_ID,
            aws_conn_id=AWS_CONN_ID,
            mongo_collection=MONGO_COLLECTION,
            mongo_query=MONGO_QUERY,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
            dag=self.dag,
            compression=COMPRESSION,
        )

    def test_init(self):
        assert self.mock_operator.task_id == TASK_ID
        assert self.mock_operator.mongo_conn_id == MONGO_CONN_ID
        assert self.mock_operator.aws_conn_id == AWS_CONN_ID
        assert self.mock_operator.mongo_collection == MONGO_COLLECTION
        assert self.mock_operator.mongo_query == MONGO_QUERY
        assert self.mock_operator.s3_bucket == S3_BUCKET
        assert self.mock_operator.s3_key == S3_KEY
        assert self.mock_operator.compression == COMPRESSION

    def test_template_field_overrides(self):
        assert self.mock_operator.template_fields == (
            's3_bucket',
            's3_key',
            'mongo_query',
            'mongo_collection',
        )

    def test_render_template(self):
        ti = TaskInstance(self.mock_operator, DEFAULT_DATE)
        ti.render_templates()

        expected_rendered_template = {'$lt': '2017-01-01T00:00:00+00:00Z'}

        assert expected_rendered_template == getattr(self.mock_operator, 'mongo_query')

    @mock.patch('airflow.providers.amazon.aws.transfers.mongo_to_s3.MongoHook')
    @mock.patch('airflow.providers.amazon.aws.transfers.mongo_to_s3.S3Hook')
    def test_execute(self, mock_s3_hook, mock_mongo_hook):
        operator = self.mock_operator

        mock_mongo_hook.return_value.find.return_value = iter(MOCK_MONGO_RETURN)
        mock_s3_hook.return_value.load_string.return_value = True

        operator.execute(None)

        mock_mongo_hook.return_value.find.assert_called_once_with(
            mongo_collection=MONGO_COLLECTION, query=MONGO_QUERY, mongo_db=None
        )

        op_stringify = self.mock_operator._stringify
        op_transform = self.mock_operator.transform

        s3_doc_str = op_stringify(op_transform(MOCK_MONGO_RETURN))

        mock_s3_hook.return_value.load_string.assert_called_once_with(
            string_data=s3_doc_str, key=S3_KEY, bucket_name=S3_BUCKET, replace=False, compression=COMPRESSION
        )

    @mock.patch('airflow.providers.amazon.aws.transfers.mongo_to_s3.MongoHook')
    @mock.patch('airflow.providers.amazon.aws.transfers.mongo_to_s3.S3Hook')
    def test_execute_compress(self, mock_s3_hook, mock_mongo_hook):
        operator = self.mock_operator
        self.mock_operator.compression = 'gzip'
        mock_mongo_hook.return_value.find.return_value = iter(MOCK_MONGO_RETURN)
        mock_s3_hook.return_value.load_string.return_value = True

        operator.execute(None)

        mock_mongo_hook.return_value.find.assert_called_once_with(
            mongo_collection=MONGO_COLLECTION, query=MONGO_QUERY, mongo_db=None
        )

        op_stringify = self.mock_operator._stringify
        op_transform = self.mock_operator.transform

        s3_doc_str = op_stringify(op_transform(MOCK_MONGO_RETURN))

        mock_s3_hook.return_value.load_string.assert_called_once_with(
            string_data=s3_doc_str, key=S3_KEY, bucket_name=S3_BUCKET, replace=False, compression='gzip'
        )
