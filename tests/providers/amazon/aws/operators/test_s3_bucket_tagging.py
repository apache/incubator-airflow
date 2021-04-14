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
import os
import unittest
from unittest import mock

from moto import mock_s3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket_tagging import (
    S3DeleteBucketTaggingOperator,
    S3GetBucketTaggingOperator,
    S3PutBucketTaggingOperator,
)

BUCKET_NAME = os.environ.get("BUCKET_NAME", "test-airflow-bucket")
TAG_SET = [{'Key': 'Color', 'Value': 'Green'}]
TASK_ID = os.environ.get("TASK_ID", "test-s3-operator")


class TestS3GetBucketTaggingOperator(unittest.TestCase):
    def setUp(self):
        self.get_bucket_tagging_operator = S3GetBucketTaggingOperator(
            task_id=TASK_ID,
            bucket_name=BUCKET_NAME,
        )

    @mock_s3
    @mock.patch.object(S3Hook, "get_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, get_bucket_tagging):
        mock_check_for_bucket.return_value = True
        # execute s3 get bucket tagging operator
        self.get_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        get_bucket_tagging.assert_called_once_with(BUCKET_NAME)

    @mock_s3
    @mock.patch.object(S3Hook, "get_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, get_bucket_tagging):
        mock_check_for_bucket.return_value = False
        # execute s3 get bucket tagging operator
        self.get_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        get_bucket_tagging.assert_not_called()


class TestS3PutBucketTaggingOperator(unittest.TestCase):
    def setUp(self):
        self.put_bucket_tagging_operator = S3PutBucketTaggingOperator(
            task_id=TASK_ID,
            tag_set=TAG_SET,
            bucket_name=BUCKET_NAME,
        )

    @mock_s3
    @mock.patch.object(S3Hook, "put_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, put_bucket_tagging):
        mock_check_for_bucket.return_value = True
        # execute s3 put bucket tagging operator
        self.put_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        put_bucket_tagging.assert_called_once_with(
            key=None, value=None, tag_set=TAG_SET, bucket_name=BUCKET_NAME
        )

    @mock_s3
    @mock.patch.object(S3Hook, "put_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, put_bucket_tagging):
        mock_check_for_bucket.return_value = False
        # execute s3 put bucket tagging operator
        self.put_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        put_bucket_tagging.assert_not_called()


class TestS3DeleteBucketTaggingOperator(unittest.TestCase):
    def setUp(self):
        self.delete_bucket_tagging_operator = S3DeleteBucketTaggingOperator(
            task_id=TASK_ID,
            bucket_name=BUCKET_NAME,
        )

    @mock_s3
    @mock.patch.object(S3Hook, "delete_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, delete_bucket_tagging):
        mock_check_for_bucket.return_value = True
        # execute s3 delete bucket tagging operator
        self.delete_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        delete_bucket_tagging.assert_called_once_with(BUCKET_NAME)

    @mock_s3
    @mock.patch.object(S3Hook, "delete_bucket_tagging")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, delete_bucket_tagging):
        mock_check_for_bucket.return_value = False
        # execute s3 delete bucket tagging operator
        self.delete_bucket_tagging_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        delete_bucket_tagging.assert_not_called()
