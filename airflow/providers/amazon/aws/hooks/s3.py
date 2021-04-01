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

# pylint: disable=invalid-name
"""Interact with AWS S3, using the boto3 library."""
import fnmatch
import gzip as gz
import io
import re
import shutil
from functools import wraps
from inspect import signature
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, cast
from urllib.parse import urlparse

from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import chunks

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def provide_bucket_name(func: T) -> T:
    """
    Function decorator that provides a bucket name taken from the connection
    in case no bucket name has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)

        if 'bucket_name' not in bound_args.arguments:
            self = args[0]
            if self.aws_conn_id:
                connection = self.get_connection(self.aws_conn_id)
                if connection.schema:
                    bound_args.arguments['bucket_name'] = connection.schema

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


def unify_bucket_name_and_key(func: T) -> T:
    """
    Function decorator that unifies bucket name and key taken from the key
    in case no bucket name and at least a key has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)

        def get_key_name() -> Optional[str]:
            if 'wildcard_key' in bound_args.arguments:
                return 'wildcard_key'
            if 'key' in bound_args.arguments:
                return 'key'
            raise ValueError('Missing key parameter!')

        key_name = get_key_name()
        if key_name and 'bucket_name' not in bound_args.arguments:
            bound_args.arguments['bucket_name'], bound_args.arguments[key_name] = S3Hook.parse_s3_url(
                bound_args.arguments[key_name]
            )

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class S3Hook(AwsBaseHook):
    """
    Interact with AWS S3, using the boto3 library.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    conn_type = 's3'
    hook_name = 'S3'

    def __init__(self, *args, **kwargs) -> None:
        kwargs['client_type'] = 's3'

        self.extra_args = {}
        if 'extra_args' in kwargs:
            self.extra_args = kwargs['extra_args']
            if not isinstance(self.extra_args, dict):
                raise ValueError(f"extra_args '{self.extra_args!r}' must be of type {dict}")
            del kwargs['extra_args']

        self.transfer_config = TransferConfig()
        if 'transfer_config_args' in kwargs:
            transport_config_args = kwargs['transfer_config_args']
            if not isinstance(transport_config_args, dict):
                raise ValueError(f"transfer_config_args '{transport_config_args!r} must be of type {dict}")
            self.transfer_config = TransferConfig(**transport_config_args)
            del kwargs['transfer_config_args']

        super().__init__(*args, **kwargs)

    @staticmethod
    def parse_s3_url(s3url: str) -> Tuple[str, str]:
        """
        Parses the S3 Url into a bucket name and key.

        :param s3url: The S3 Url to parse.
        :rtype s3url: str
        :return: the parsed bucket name and key
        :rtype: tuple of str
        """
        parsed_url = urlparse(s3url)

        if not parsed_url.netloc:
            raise AirflowException(f'Please provide a bucket_name instead of "{s3url}"')

        bucket_name = parsed_url.netloc
        key = parsed_url.path.strip('/')

        return bucket_name, key

    @provide_bucket_name
    def check_for_bucket(self, bucket_name: Optional[str] = None) -> bool:
        """
        Check if bucket_name exists.

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: True if it exists and False if not.
        :rtype: bool
        """
        try:
            self.get_conn().head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            return False

    @provide_bucket_name
    def get_bucket(self, bucket_name: Optional[str] = None) -> str:
        """
        Returns a boto3.S3.Bucket object

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: the bucket object to the bucket name.
        :rtype: boto3.S3.Bucket
        """
        s3_resource = self.get_resource_type('s3')
        return s3_resource.Bucket(bucket_name)

    @provide_bucket_name
    def create_bucket(self, bucket_name: Optional[str] = None, region_name: Optional[str] = None) -> None:
        """
        Creates an Amazon S3 bucket.

        :param bucket_name: The name of the bucket
        :type bucket_name: str
        :param region_name: The name of the aws region in which to create the bucket.
        :type region_name: str
        """
        if not region_name:
            region_name = self.get_conn().meta.region_name
        if region_name == 'us-east-1':
            self.get_conn().create_bucket(Bucket=bucket_name)
        else:
            self.get_conn().create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region_name}
            )

    @provide_bucket_name
    def check_for_prefix(self, prefix: str, delimiter: str, bucket_name: Optional[str] = None) -> bool:
        """
        Checks that a prefix exists in a bucket

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :return: False if the prefix does not exist in the bucket and True if it does.
        :rtype: bool
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(fr'(\w+[{delimiter}])$', prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return prefix in plist

    @provide_bucket_name
    def list_prefixes(
        self,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> list:
        """
        Lists prefixes in a bucket under prefix

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        :return: a list of matched prefixes
        :rtype: list
        """
        prefix = prefix or ''
        delimiter = delimiter or ''
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
        )

        prefixes = []
        for page in response:
            if 'CommonPrefixes' in page:
                for common_prefix in page['CommonPrefixes']:
                    prefixes.append(common_prefix['Prefix'])

        return prefixes

    @provide_bucket_name
    def list_keys(
        self,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> list:
        """
        Lists keys in a bucket under prefix and not containing delimiter

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        :return: a list of matched keys
        :rtype: list
        """
        prefix = prefix or ''
        delimiter = delimiter or ''
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
        )

        keys = []
        for page in response:
            if 'Contents' in page:
                for k in page['Contents']:
                    keys.append(k['Key'])

        return keys

    @provide_bucket_name
    @unify_bucket_name_and_key
    def check_for_key(self, key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Checks if a key exists in a bucket

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: True if the key exists and False if not.
        :rtype: bool
        """
        try:
            self.get_conn().head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            else:
                raise e

    @provide_bucket_name
    @unify_bucket_name_and_key
    def get_key(self, key: str, bucket_name: Optional[str] = None) -> S3Transfer:
        """
        Returns a boto3.s3.Object

        :param key: the path to the key
        :type key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: the key object from the bucket
        :rtype: boto3.s3.Object
        """
        obj = self.get_resource_type('s3').Object(bucket_name, key)
        obj.load()
        return obj

    @provide_bucket_name
    @unify_bucket_name_and_key
    def read_key(self, key: str, bucket_name: Optional[str] = None) -> str:
        """
        Reads a key from S3

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: the content of the key
        :rtype: str
        """
        obj = self.get_key(key, bucket_name)
        return obj.get()['Body'].read().decode('utf-8')

    @provide_bucket_name
    @unify_bucket_name_and_key
    def select_key(
        self,
        key: str,
        bucket_name: Optional[str] = None,
        expression: Optional[str] = None,
        expression_type: Optional[str] = None,
        input_serialization: Optional[Dict[str, Any]] = None,
        output_serialization: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Reads a key with S3 Select.

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :param expression: S3 Select expression
        :type expression: str
        :param expression_type: S3 Select expression type
        :type expression_type: str
        :param input_serialization: S3 Select input data serialization format
        :type input_serialization: dict
        :param output_serialization: S3 Select output data serialization format
        :type output_serialization: dict
        :return: retrieved subset of original data by S3 Select
        :rtype: str

        .. seealso::
            For more details about S3 Select parameters:
            http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.select_object_content
        """
        expression = expression or 'SELECT * FROM S3Object'
        expression_type = expression_type or 'SQL'

        if input_serialization is None:
            input_serialization = {'CSV': {}}
        if output_serialization is None:
            output_serialization = {'CSV': {}}

        response = self.get_conn().select_object_content(
            Bucket=bucket_name,
            Key=key,
            Expression=expression,
            ExpressionType=expression_type,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization,
        )

        return ''.join(
            event['Records']['Payload'].decode('utf-8') for event in response['Payload'] if 'Records' in event
        )

    @provide_bucket_name
    @unify_bucket_name_and_key
    def check_for_wildcard_key(
        self, wildcard_key: str, bucket_name: Optional[str] = None, delimiter: str = ''
    ) -> bool:
        """
        Checks that a key matching a wildcard expression exists in a bucket

        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param delimiter: the delimiter marks key hierarchy
        :type delimiter: str
        :return: True if a key exists and False if not.
        :rtype: bool
        """
        return (
            self.get_wildcard_key(wildcard_key=wildcard_key, bucket_name=bucket_name, delimiter=delimiter)
            is not None
        )

    @provide_bucket_name
    @unify_bucket_name_and_key
    def get_wildcard_key(
        self, wildcard_key: str, bucket_name: Optional[str] = None, delimiter: str = ''
    ) -> S3Transfer:
        """
        Returns a boto3.s3.Object object matching the wildcard expression

        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param delimiter: the delimiter marks key hierarchy
        :type delimiter: str
        :return: the key object from the bucket or None if none has been found.
        :rtype: boto3.s3.Object
        """
        prefix = re.split(r'[*]', wildcard_key, 1)[0]
        key_list = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        key_matches = [k for k in key_list if fnmatch.fnmatch(k, wildcard_key)]
        if key_matches:
            return self.get_key(key_matches[0], bucket_name)
        return None

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_file(
        self,
        filename: str,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Loads a local file to S3

        :param filename: name of the file to load.
        :type filename: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
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
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = self.extra_args
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if gzip:
            with open(filename, 'rb') as f_in:
                filename_gz = f_in.name + '.gz'
                with gz.open(filename_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz
        if acl_policy:
            extra_args['ACL'] = acl_policy

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args, Config=self.transfer_config)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_string(
        self,
        string_data: str,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        encoding: Optional[str] = None,
        acl_policy: Optional[str] = None,
        compression: Optional[str] = None,
    ) -> None:
        """
        Loads a string to S3

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        :param string_data: str to set as content for the key.
        :type string_data: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        :param encoding: The string to byte encoding
        :type encoding: str
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        :type acl_policy: str
        :param compression: Type of compression to use, currently only gzip is supported.
        :type compression: str
        """
        encoding = encoding or 'utf-8'

        bytes_data = string_data.encode(encoding)

        # Compress string
        available_compressions = ['gzip']
        if compression is not None and compression not in available_compressions:
            raise NotImplementedError(
                "Received {} compression type. String "
                "can currently be compressed in {} "
                "only.".format(compression, available_compressions)
            )
        if compression == 'gzip':
            bytes_data = gz.compress(bytes_data)

        file_obj = io.BytesIO(bytes_data)

        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt, acl_policy)
        file_obj.close()

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_bytes(
        self,
        bytes_data: bytes,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Loads bytes to S3

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        :param bytes_data: bytes to set as content for the key.
        :type bytes_data: bytes
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        :type acl_policy: str
        """
        file_obj = io.BytesIO(bytes_data)
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt, acl_policy)
        file_obj.close()

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_file_obj(
        self,
        file_obj: BytesIO,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Loads a file object to S3

        :param file_obj: The file-like object to set as the content for the S3 key.
        :type file_obj: file-like object
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag that indicates whether to overwrite the key
            if it already exists.
        :type replace: bool
        :param encrypt: If True, S3 encrypts the file on the server,
            and the file is stored in encrypted form at rest in S3.
        :type encrypt: bool
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        :type acl_policy: str
        """
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt, acl_policy)

    def _upload_file_obj(
        self,
        file_obj: BytesIO,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: Optional[str] = None,
    ) -> None:
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = self.extra_args
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if acl_policy:
            extra_args['ACL'] = acl_policy

        client = self.get_conn()
        client.upload_fileobj(
            file_obj,
            bucket_name,
            key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )

    def copy_object(
        self,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: Optional[str] = None,
        dest_bucket_name: Optional[str] = None,
        source_version_id: Optional[str] = None,
        acl_policy: Optional[str] = None,
    ) -> None:
        """
        Creates a copy of an object that is already stored in S3.

        Note: the S3 connection used here needs to have access to both
        source and destination bucket/key.

        :param source_bucket_key: The key of the source object.

            It can be either full s3:// style url or relative path from root level.

            When it's specified as a full s3:// url, please omit source_bucket_name.
        :type source_bucket_key: str
        :param dest_bucket_key: The key of the object to copy to.

            The convention to specify `dest_bucket_key` is the same
            as `source_bucket_key`.
        :type dest_bucket_key: str
        :param source_bucket_name: Name of the S3 bucket where the source object is in.

            It should be omitted when `source_bucket_key` is provided as a full s3:// url.
        :type source_bucket_name: str
        :param dest_bucket_name: Name of the S3 bucket to where the object is copied.

            It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
        :type dest_bucket_name: str
        :param source_version_id: Version ID of the source object (OPTIONAL)
        :type source_version_id: str
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be copied which is private by default.
        :type acl_policy: str
        """
        acl_policy = acl_policy or 'private'

        if dest_bucket_name is None:
            dest_bucket_name, dest_bucket_key = self.parse_s3_url(dest_bucket_key)
        else:
            parsed_url = urlparse(dest_bucket_key)
            if parsed_url.scheme != '' or parsed_url.netloc != '':
                raise AirflowException(
                    'If dest_bucket_name is provided, '
                    + 'dest_bucket_key should be relative path '
                    + 'from root level, rather than a full s3:// url'
                )

        if source_bucket_name is None:
            source_bucket_name, source_bucket_key = self.parse_s3_url(source_bucket_key)
        else:
            parsed_url = urlparse(source_bucket_key)
            if parsed_url.scheme != '' or parsed_url.netloc != '':
                raise AirflowException(
                    'If source_bucket_name is provided, '
                    + 'source_bucket_key should be relative path '
                    + 'from root level, rather than a full s3:// url'
                )

        copy_source = {'Bucket': source_bucket_name, 'Key': source_bucket_key, 'VersionId': source_version_id}
        response = self.get_conn().copy_object(
            Bucket=dest_bucket_name, Key=dest_bucket_key, CopySource=copy_source, ACL=acl_policy
        )
        return response

    @provide_bucket_name
    def delete_bucket(self, bucket_name: str, force_delete: bool = False) -> None:
        """
        To delete s3 bucket, delete all s3 bucket objects and then delete the bucket.

        :param bucket_name: Bucket name
        :type bucket_name: str
        :param force_delete: Enable this to delete bucket even if not empty
        :type force_delete: bool
        :return: None
        :rtype: None
        """
        if force_delete:
            bucket_keys = self.list_keys(bucket_name=bucket_name)
            if bucket_keys:
                self.delete_objects(bucket=bucket_name, keys=bucket_keys)
        self.conn.delete_bucket(Bucket=bucket_name)

    def delete_objects(self, bucket: str, keys: Union[str, list]) -> None:
        """
        Delete keys from the bucket.

        :param bucket: Name of the bucket in which you are going to delete object(s)
        :type bucket: str
        :param keys: The key(s) to delete from S3 bucket.

            When ``keys`` is a string, it's supposed to be the key name of
            the single object to delete.

            When ``keys`` is a list, it's supposed to be the list of the
            keys to delete.
        :type keys: str or list
        """
        if isinstance(keys, str):
            keys = [keys]

        s3 = self.get_conn()

        # We can only send a maximum of 1000 keys per request.
        # For details see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        for chunk in chunks(keys, chunk_size=1000):
            response = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
            deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
            self.log.info("Deleted: %s", deleted_keys)
            if "Errors" in response:
                errors_keys = [x['Key'] for x in response.get("Errors", [])]
                raise AirflowException(f"Errors when deleting: {errors_keys}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def download_file(
        self, key: str, bucket_name: Optional[str] = None, local_path: Optional[str] = None
    ) -> str:
        """
        Downloads a file from the S3 location to the local file system.

        :param key: The key path in S3.
        :type key: str
        :param bucket_name: The specific bucket to use.
        :type bucket_name: Optional[str]
        :param local_path: The local path to the downloaded file. If no path is provided it will use the
            system's temporary directory.
        :type local_path: Optional[str]
        :return: the file name.
        :rtype: str
        """
        self.log.info('Downloading source S3 file from Bucket %s with path %s', bucket_name, key)

        if not self.check_for_key(key, bucket_name):
            raise AirflowException(f'The source file in Bucket {bucket_name} with path {key} does not exist')

        s3_obj = self.get_key(key, bucket_name)

        with NamedTemporaryFile(dir=local_path, prefix='airflow_tmp_', delete=False) as local_tmp_file:
            s3_obj.download_fileobj(local_tmp_file)

        return local_tmp_file.name

    def generate_presigned_url(
        self,
        client_method: str,
        params: Optional[dict] = None,
        expires_in: int = 3600,
        http_method: Optional[str] = None,
    ) -> Optional[str]:
        """
        Generate a presigned url given a client, its method, and arguments

        :param client_method: The client method to presign for.
        :type client_method: str
        :param params: The parameters normally passed to ClientMethod.
        :type params: dict
        :param expires_in: The number of seconds the presigned url is valid for.
            By default it expires in an hour (3600 seconds).
        :type expires_in: int
        :param http_method: The http method to use on the generated url.
            By default, the http method is whatever is used in the method's model.
        :type http_method: str
        :return: The presigned url.
        :rtype: str
        """
        s3_client = self.get_conn()
        try:
            return s3_client.generate_presigned_url(
                ClientMethod=client_method, Params=params, ExpiresIn=expires_in, HttpMethod=http_method
            )

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            return None

    @provide_bucket_name
    def get_bucket_tagging(self, bucket_name: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        Gets a List of tags from a bucket.

        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :return: A List containing the key/value pairs for the tags
        :rtype: Optional[List[Dict[str, str]]]
        """
        try:
            s3_client = self.get_conn()
            result = s3_client.get_bucket_tagging(Bucket=bucket_name)['TagSet']
            self.log.info("S3 Bucket Tag Info: %s", result)
            return result
        except ClientError as e:
            self.log.error(e)
            raise e

    @provide_bucket_name
    def put_bucket_tagging(
        self,
        tag_set: Optional[List[Dict[str, str]]] = None,
        key: Optional[str] = None,
        value: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> None:
        """
        Overwrites the existing TagSet with provided tags.  Must provide either a TagSet or a key/value pair.

        :param tag_set: A List containing the key/value pairs for the tags.
        :type tag_set: List[Dict[str, str]]
        :param key: The Key for the new TagSet entry.
        :type key: str
        :param value: The Value for the new TagSet entry.
        :type value: str
        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :return: None
        :rtype: None
        """
        self.log.info("S3 Bucket Tag Info:\tKey: %s\tValue: %s\tSet: %s", key, value, tag_set)
        if not tag_set:
            tag_set = []
        if key and value:
            tag_set.append({'Key': key, 'Value': value})
        elif not tag_set or (key or value):
            message = 'put_bucket_tagging() requires either a predefined TagSet or a key/value pair.'
            self.log.error(message)
            raise ValueError(message)

        try:
            s3_client = self.get_conn()
            s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': tag_set})
        except ClientError as e:
            self.log.error(e)
            raise e

    @provide_bucket_name
    def delete_bucket_tagging(self, bucket_name: Optional[str] = None) -> None:
        """
        Deletes all tags from a bucket.

        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :return: None
        :rtype: None
        """
        s3_client = self.get_conn()
        s3_client.delete_bucket_tagging(Bucket=bucket_name)
