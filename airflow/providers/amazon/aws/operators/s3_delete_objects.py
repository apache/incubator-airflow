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
from typing import Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3DeleteObjectsOperator(BaseOperator):
    """
    To enable users to delete single object or multiple objects from
    a bucket using a single HTTP request.

    Users may specify up to 1000 keys to delete.

    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :type bucket: str
    :param keys: The key(s) to delete from S3 bucket. (templated)

        When ``keys`` is a string, it's supposed to be the key name of
        the single object to delete.

        When ``keys`` is a list, it's supposed to be the list of the
        keys to delete.

        You may specify up to 1000 keys.
    :type keys: str or list
    :param prefix: Prefix of objects to delete. (templated)
        All objects matching this prefix in the bucket will be deleted.
    :type prefix: str
    :param aws_conn_id: Connection id of the S3 connection to use
    :type aws_conn_id: str
        (Adding this param to template_fields so that it can be overriden using jinja template from Dags
         This feature can be useful if user wants to update/override the aws_conn_id for some kind of Dag Isolation etc)
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.

        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    """

    template_fields = ('keys', 'bucket', 'prefix' , 'aws_conn_id')

    def __init__(
        self,
        *,
        bucket: str,
        keys: Optional[Union[str, list]] = None,
        prefix: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):

        if not bool(keys) ^ bool(prefix):
            raise ValueError("Either keys or prefix should be set.")

        super().__init__(**kwargs)
        self.bucket = bucket
        self.keys = keys
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        keys = self.keys or s3_hook.list_keys(bucket_name=self.bucket, prefix=self.prefix)
        if keys:
            s3_hook.delete_objects(bucket=self.bucket, keys=keys)
