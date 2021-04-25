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


class FTPToS3Operator(BaseOperator):
    """
    This operator enables the transferring of files from FTP server to S3.

    :param s3_bucket: The targeted s3 bucket in which upload the file to
    :type s3_bucket: str
    :param s3_key: The targeted s3 key. This is the specified file path for
        uploading the file to S3.
    :type s3_key: str
    :param ftp_path: The ftp remote path, including the file.
    :type ftp_path: str
    :param ftp_conn_id: The ftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    :type ftp_conn_id: str
    :param aws_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :type aws_conn_id: str
    :param replace: A flag to decide whether or not to overwrite the key
        if it already exists. If replace is False and the key exists, an
        error will be raised.
    :type replace: bool
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :type encrypt: bool
    :param gzip: If True, the file will be compressed locally
    :type gzip: bool
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :type acl_policy: str
    """

    template_fields = (
        's3_bucket',
        's3_key',
        'ftp_path',
    )

    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_key,
        ftp_path,
        ftp_conn_id='ftp_default',
        aws_conn_id='aws_default',
        replace=False,
        encrypt=False,
        gzip=False,
        acl_policy=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ftp_path = ftp_path
        self.aws_conn_id = aws_conn_id
        self.ftp_conn_id = ftp_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

        with NamedTemporaryFile() as local_tmp_file:
            ftp_hook.retrieve_file(
                remote_full_path=self.ftp_path, local_full_path_or_buffer=local_tmp_file.name
            )

            s3_hook.load_file(
                filename=local_tmp_file.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )
