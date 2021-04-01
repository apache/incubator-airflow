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
import re
from typing import Callable, List, Optional, Union
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class S3KeySensor(BaseSensorOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
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

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(
        self,
        *,
        bucket_key: str,
        bucket_name: Optional[str] = None,
        wildcard_match: bool = False,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def poke(self, context):

        if self.bucket_name is None:
            parsed_url = urlparse(self.bucket_key)
            if parsed_url.netloc == '':
                raise AirflowException('If key is a relative path from root, please provide a bucket_name')
            self.bucket_name = parsed_url.netloc
            self.bucket_key = parsed_url.path.lstrip('/')
        else:
            parsed_url = urlparse(self.bucket_key)
            if parsed_url.scheme != '' or parsed_url.netloc != '':
                raise AirflowException(
                    'If bucket_name is provided, bucket_key'
                    + ' should be relative path from root'
                    + ' level, rather than a full s3:// url'
                )

        self.log.info('Poking for key : s3://%s/%s', self.bucket_name, self.bucket_key)
        if self.wildcard_match:
            return self.get_hook().check_for_wildcard_key(self.bucket_key, self.bucket_name)
        return self.get_hook().check_for_key(self.bucket_key, self.bucket_name)

    def get_hook(self) -> S3Hook:
        """Create and return an S3Hook"""
        if self.hook:
            return self.hook

        self.hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self.hook


class S3KeySizeSensor(S3KeySensor):
    """
    Waits for a key (a file-like instance on S3) to be present and be more than
    some size in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
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
    :type check_fn: Optional[Callable[..., bool]]
    :param check_fn: Function that receives the list of the S3 objects,
        and returns the boolean:
        - ``True``: a certain criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(self, data: List) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in data if isinstance(f, dict))
    :type check_fn: Optional[Callable[..., bool]]
    """

    @apply_defaults
    def __init__(
        self,
        *,
        check_fn: Optional[Callable[..., bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.check_fn_user = check_fn

    def poke(self, context):
        if super().poke(context=context) is False:
            return False

        s3_objects = self.get_files(s3_hook=self.get_hook())
        if not s3_objects:
            return False
        check_fn = self.check_fn if self.check_fn_user is None else self.check_fn_user
        return check_fn(s3_objects)

    def get_files(self, s3_hook: S3Hook, delimiter: Optional[str] = '/') -> List:
        """Gets a list of files in the bucket"""
        prefix = self.bucket_key
        config = {
            'PageSize': None,
            'MaxItems': None,
        }
        if self.wildcard_match:
            prefix = re.split(r'[*]', self.bucket_key, 1)[0]

        paginator = s3_hook.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
        )
        keys = []
        for page in response:
            if 'Contents' in page:
                _temp = [k for k in page['Contents'] if isinstance(k.get('Size', None), (int, float))]
                keys = keys + _temp
        return keys

    def check_fn(self, data: List, object_min_size: Optional[Union[int, float]] = 0) -> bool:
        """Default function for checking that S3 Objects have size more than 0

        :param data: List of the objects in S3 bucket.
        :type data: list
        :param object_min_size: Checks if the objects sizes are greater then this value.
        :type object_min_size: int
        """
        return all(f.get('Size', 0) > object_min_size for f in data if isinstance(f, dict))
