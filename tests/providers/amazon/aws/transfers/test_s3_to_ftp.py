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
from unittest.mock import Mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator

TASK_ID = 'test_s3_to_ftp'
BUCKET = 'test-s3-bucket'
S3_KEY = 'test/test_1_file.csv'
FTP_PATH = '/tmp/remote_path.txt'
AWS_CONN_ID = 'aws_default'
FTP_CONN_ID = 'ftp_default'


class TestS3ToFTPOperator(unittest.TestCase):
    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.store_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.NamedTemporaryFile")
    @mock.patch('airflow.providers.amazon.aws.transfers.s3_to_ftp.S3Hook')
    def test_execute(self, mock_s3_hook, mock_local_tmp_file, mock_ftp_hook_store_file):
        operator = S3ToFTPOperator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        operator.execute(None)

        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        key = operator.s3_key
        bucket = operator.s3_bucket

        s3_client = mock_s3_hook.get_conn()

        s3_client.download_file(bucket, key, mock_local_tmp_file.name)
        s3_client.download_file.assert_called_once_with(bucket, key, mock_local_tmp_file.name)

        mock_ftp_hook_store_file.assert_called_once_with(operator.ftp_path, mock_local_tmp_file_value.name)
