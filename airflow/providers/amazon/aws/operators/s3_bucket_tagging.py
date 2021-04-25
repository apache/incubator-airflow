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
"""This module contains AWS S3 operators."""
from typing import Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET_DOES_NOT_EXIST_MSG = "Bucket with name: %s doesn't exist"


class S3GetBucketTaggingOperator(BaseOperator):
    """
    This operator gets tagging from an S3 bucket

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3GetBucketTaggingOperator`

    :param bucket_name: This is bucket name you want to reference
    :type bucket_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    """

    template_fields = ("bucket_name",)

    def __init__(self, bucket_name: str, aws_conn_id: Optional[str] = "aws_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        if s3_hook.check_for_bucket(self.bucket_name):
            self.log.info("Getting tags for bucket %s", self.bucket_name)
            return s3_hook.get_bucket_tagging(self.bucket_name)
        else:
            self.log.warning(BUCKET_DOES_NOT_EXIST_MSG, self.bucket_name)
            return None


class S3PutBucketTaggingOperator(BaseOperator):
    """
    This operator puts tagging for an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3PutBucketTaggingOperator`

    :param bucket_name: The name of the bucket to add tags to.
    :type bucket_name: str
    :param key: The key portion of the key/value pair for a tag to be added.
        If a key is provided, a value must be provided as well.
    :type key: str
    :param value: The value portion of the key/value pair for a tag to be added.
        If a value is provided, a key must be provided as well.
    :param tag_set: A List of key/value pairs.
    :type tag_set: List[Dict[str, str]]
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    """

    template_fields = ("bucket_name",)

    def __init__(
        self,
        bucket_name: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        tag_set: Optional[List[Dict[str, str]]] = None,
        aws_conn_id: Optional[str] = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.value = value
        self.tag_set = tag_set
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        if s3_hook.check_for_bucket(self.bucket_name):
            self.log.info("Putting tags for bucket %s", self.bucket_name)
            return s3_hook.put_bucket_tagging(
                key=self.key, value=self.value, tag_set=self.tag_set, bucket_name=self.bucket_name
            )
        else:
            self.log.warning(BUCKET_DOES_NOT_EXIST_MSG, self.bucket_name)
            return None


class S3DeleteBucketTaggingOperator(BaseOperator):
    """
    This operator deletes tagging from an S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3DeleteBucketTaggingOperator`

    :param bucket_name: This is the name of the bucket to delete tags from.
    :type bucket_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    """

    template_fields = ("bucket_name",)

    def __init__(self, bucket_name: str, aws_conn_id: Optional[str] = "aws_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        if s3_hook.check_for_bucket(self.bucket_name):
            self.log.info("Deleting tags for bucket %s", self.bucket_name)
            return s3_hook.delete_bucket_tagging(self.bucket_name)
        else:
            self.log.warning(BUCKET_DOES_NOT_EXIST_MSG, self.bucket_name)
            return None
