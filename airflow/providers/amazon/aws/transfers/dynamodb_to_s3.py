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
#

"""
This module contains operators to replicate records from
DynamoDB table to S3.
"""
import json
from copy import copy
from os.path import getsize
from tempfile import NamedTemporaryFile
from typing import IO, Any, Callable, Dict, Optional
from uuid import uuid4

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dynamodb import AwsDynamoDBHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


def _convert_item_to_json_bytes(item: Dict[str, Any]) -> bytes:
    return (json.dumps(item) + '\n').encode('utf-8')


def _upload_file_to_s3(file_obj: IO, bucket_name: str, s3_key_prefix: str) -> None:
    s3_client = S3Hook().get_conn()
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class DynamoDBToS3Operator(BaseOperator):
    """
    Replicates records from a DynamoDB table to S3.
    It scans a DynamoDB table and write the received records to a file
    on the local filesystem. It flushes the file to S3 once the file size
    exceeds the file size limit specified by the user.

    Users can also specify a filtering criteria using dynamodb_scan_kwargs
    to only replicate records that satisfy the criteria.

    To parallelize the replication, users can create multiple tasks of DynamoDBToS3Operator.
    For instance to replicate with parallelism of 2, create two tasks like:

    .. code-block:: python

       op1 = DynamoDBToS3Operator(
           task_id='replicator-1',
           dynamodb_table_name='hello',
           dynamodb_scan_kwargs={
               'TotalSegments': 2,
               'Segment': 0,
           },
           ...
       )

       op2 = DynamoDBToS3Operator(
           task_id='replicator-2',
           dynamodb_table_name='hello',
           dynamodb_scan_kwargs={
               'TotalSegments': 2,
               'Segment': 1,
           },
           ...
       )

    :param dynamodb_table_name: Dynamodb table to replicate data from
    :type dynamodb_table_name: str
    :param s3_bucket_name: S3 bucket to replicate data to
    :type s3_bucket_name: str
    :param file_size: Flush file to s3 if file size >= file_size
    :type file_size: int
    :param dynamodb_scan_kwargs: kwargs pass to <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan>  # noqa: E501 pylint: disable=line-too-long
    :type dynamodb_scan_kwargs: Optional[Dict[str, Any]]
    :param s3_key_prefix: Prefix of s3 object key
    :type s3_key_prefix: Optional[str]
    :param process_func: How we transforms a dynamodb item to bytes. By default we dump the json
    :type process_func: Callable[[Dict[str, Any]], bytes]
    """

    @apply_defaults
    def __init__(
        self,
        *,
        dynamodb_table_name: str,
        s3_bucket_name: str,
        file_size: int,
        dynamodb_scan_kwargs: Optional[Dict[str, Any]] = None,
        s3_key_prefix: str = '',
        process_func: Callable[[Dict[str, Any]], bytes] = _convert_item_to_json_bytes,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_size = file_size
        self.process_func = process_func
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb_scan_kwargs = dynamodb_scan_kwargs
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix

    def execute(self, context) -> None:
        table = AwsDynamoDBHook().get_conn().Table(self.dynamodb_table_name)
        scan_kwargs = copy(self.dynamodb_scan_kwargs) if self.dynamodb_scan_kwargs else {}
        err = None
        f = NamedTemporaryFile()
        try:
            f = self._scan_dynamodb_and_upload_to_s3(f, scan_kwargs, table)
        except Exception as e:
            err = e
            raise e
        finally:
            if err is None:
                _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
            f.close()

    def _scan_dynamodb_and_upload_to_s3(self, temp_file: IO, scan_kwargs: dict, table: Any) -> IO:
        while True:
            response = table.scan(**scan_kwargs)
            items = response['Items']
            for item in items:
                temp_file.write(self.process_func(item))

            if 'LastEvaluatedKey' not in response:
                # no more items to scan
                break

            last_evaluated_key = response['LastEvaluatedKey']
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

            # Upload the file to S3 if reach file size limit
            if getsize(temp_file.name) >= self.file_size:
                _upload_file_to_s3(temp_file, self.s3_bucket_name, self.s3_key_prefix)
                temp_file.close()
                temp_file = NamedTemporaryFile()
        return temp_file
