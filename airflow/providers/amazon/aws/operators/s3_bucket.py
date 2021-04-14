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
from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class S3CreateBucketOperator(BaseOperator):
    """
    This operator creates an S3 bucket

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3CreateBucketOperator`

    :param bucket_name: This is bucket name you want to create
    :type bucket_name: str
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    :param region_name: AWS region_name. If not specified fetched from connection.
    :type region_name: Optional[str]
    """

    template_fields = ("bucket_name",)

    @apply_defaults
    def __init__(
        self,
        *,
        bucket_name: str,
        aws_conn_id: Optional[str] = "aws_default",
        region_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        if not s3_hook.check_for_bucket(self.bucket_name):
            s3_hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)
            self.log.info("Created bucket with name: %s", self.bucket_name)
        else:
            self.log.info("Bucket with name: %s already exists", self.bucket_name)


class S3DeleteBucketOperator(BaseOperator):
    """
    This operator deletes an S3 bucket

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3DeleteBucketOperator`

    :param bucket_name: This is bucket name you want to delete
    :type bucket_name: str
    :param force_delete: Forcibly delete all objects in the bucket before deleting the bucket
    :type force_delete: bool
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    """

    template_fields = ("bucket_name",)

    def __init__(
        self,
        bucket_name: str,
        force_delete: bool = False,
        aws_conn_id: Optional[str] = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.force_delete = force_delete
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        if s3_hook.check_for_bucket(self.bucket_name):
            s3_hook.delete_bucket(bucket_name=self.bucket_name, force_delete=self.force_delete)
            self.log.info("Deleted bucket with name: %s", self.bucket_name)
        else:
            self.log.info("Bucket with name: %s doesn't exist", self.bucket_name)
