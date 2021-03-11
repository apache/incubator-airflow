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

from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.utils.decorators import apply_defaults


class S3ToFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from S3 to a FTP server.
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
        where the file is downloaded.
    :type s3_bucket: str
    :param s3_key: The targeted s3 key. This is the specified file path for
        downloading the file from S3.
    :type s3_key: str
    :param ftp_path: The ftp remote path where the file will be stored, inclusive of filename.
    :type ftp_path: str
    :param ftp_conn_id: The ftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    :type ftp_conn_id: str
    :param aws_conn_id: The AWS connection id. The name or identifier for
        establishing a connection to S3.
    :type aws_conn_id: str

    """

    template_fields = ('s3_bucket', 's3_key', 'ftp_path')

    @apply_defaults
    def __init__(
        self,
        *,
        s3_bucket,
        s3_key,
        ftp_path,
        ftp_conn_id='ftp_default',
        aws_conn_id='aws_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ftp_path = ftp_path
        self.aws_conn_id = aws_conn_id
        self.ftp_conn_id = ftp_conn_id

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

        s3_client = s3_hook.get_conn()

        with NamedTemporaryFile() as local_tmp_file:
            s3_client.download_file(self.s3_bucket, self.s3_key, local_tmp_file.name)
            ftp_hook.store_file(self.ftp_path, local_tmp_file.name)
