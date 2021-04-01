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

from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator

TASK_ID = 'test_ftp_to_s3'
BUCKET = 'test-s3-bucket'
S3_KEY = 'test/test_1_file.csv'
FTP_PATH = '/tmp/remote_path.txt'
AWS_CONN_ID = 'aws_default'
FTP_CONN_ID = 'ftp_default'


class TestFTPToS3Operator(unittest.TestCase):
    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.ftp_to_s3.NamedTemporaryFile")
    def test_execute(self, mock_local_tmp_file, mock_s3_hook_load_file, mock_ftp_hook_retrieve_file):
        operator = FTPToS3Operator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        operator.execute(None)

        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        mock_ftp_hook_retrieve_file.assert_called_once_with(
            local_full_path_or_buffer=mock_local_tmp_file_value.name, remote_full_path=operator.ftp_path
        )

        mock_s3_hook_load_file.assert_called_once_with(
            filename=mock_local_tmp_file_value.name,
            key=operator.s3_key,
            bucket_name=operator.s3_bucket,
            acl_policy=None,
            encrypt=False,
            gzip=False,
            replace=False,
        )
