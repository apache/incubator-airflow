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
from typing import List, Optional, Union
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class S3MultipleKeySensor(BaseSensorOperator):
    """
    Waits for the keys (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url different buckets for each key can be used, and lease leave
         the bucket_name as `None`.
    :type bucket_key: str
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :type bucket_name: str
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :type wildcard_match: bool
    :param aws_conn_id: a reference to the s3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    """

    template_fields = ('bucket_keys', 'bucket_name')

    @apply_defaults
    def __init__(
        self,
        *,
        bucket_keys: List[str],
        bucket_name: Optional[str] = None,
        wildcard_match: bool = False,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.bucket_keys = bucket_keys
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def poke(self, context):
        for bucket_key in self.bucket_keys:
            if self.bucket_name is None:
                parsed_url = urlparse(bucket_key)
                if parsed_url.netloc == '':
                    raise AirflowException(
                        'If key is a relative path from root, please provide a bucket_name'
                    )
                self.bucket_name = parsed_url.netloc
                bucket_key = parsed_url.path.lstrip('/')
            else:
                parsed_url = urlparse(bucket_key)
                if parsed_url.scheme != '' or parsed_url.netloc != '':
                    raise AirflowException(
                        'If bucket_name is provided, bucket_key'
                        + ' should be relative path from root'
                        + ' level, rather than a full s3:// url'
                    )

            self.log.info('Poking for key : s3://%s/%s', self.bucket_name, bucket_key)
            if self.wildcard_match:
                if not self.get_hook().check_for_wildcard_key(bucket_key, self.bucket_name):
                    return False
            if not self.get_hook().check_for_key(bucket_key, self.bucket_name):
                return False
        return True

    def get_hook(self) -> S3Hook:
        """Create and return an S3Hook"""
        if self.hook:
            return self.hook

        self.hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self.hook
