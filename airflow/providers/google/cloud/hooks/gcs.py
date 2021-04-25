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
"""This module contains a Google Cloud Storage hook."""
import functools
import gzip as gz
import os
import shutil
import time
import warnings
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from io import BytesIO
from os import path
from tempfile import NamedTemporaryFile
from typing import Callable, List, Optional, Sequence, Set, Tuple, TypeVar, Union, cast
from urllib.parse import urlparse

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils import timezone
from airflow.version import version

RT = TypeVar('RT')  # pylint: disable=invalid-name
T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name

# Use default timeout from google-cloud-storage
DEFAULT_TIMEOUT = 60


def _fallback_object_url_to_object_name_and_bucket_name(
    object_url_keyword_arg_name='object_url',
    bucket_name_keyword_arg_name='bucket_name',
    object_name_keyword_arg_name='object_name',
) -> Callable[[T], T]:
    """
    Decorator factory that convert object URL parameter to object name and bucket name parameter.

    :param object_url_keyword_arg_name: Name of the object URL parameter
    :type object_url_keyword_arg_name: str
    :param bucket_name_keyword_arg_name: Name of the bucket name parameter
    :type bucket_name_keyword_arg_name: str
    :param object_name_keyword_arg_name: Name of the object name parameter
    :type object_name_keyword_arg_name: str
    :return: Decorator
    """

    def _wrapper(func: T):
        @functools.wraps(func)
        def _inner_wrapper(self: "GCSHook", *args, **kwargs) -> RT:
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )

            object_url = kwargs.get(object_url_keyword_arg_name)
            bucket_name = kwargs.get(bucket_name_keyword_arg_name)
            object_name = kwargs.get(object_name_keyword_arg_name)

            if object_url and bucket_name and object_name:
                raise AirflowException(
                    "The mutually exclusive parameters. `object_url`, `bucket_name` together "
                    "with `object_name` parameters are present. "
                    "Please provide `object_url` or `bucket_name` and `object_name`."
                )
            if object_url:
                bucket_name, object_name = _parse_gcs_url(object_url)
                kwargs[bucket_name_keyword_arg_name] = bucket_name
                kwargs[object_name_keyword_arg_name] = object_name
                del kwargs[object_url_keyword_arg_name]

            if not object_name or not bucket_name:
                raise TypeError(
                    f"{func.__name__}() missing 2 required positional arguments: "
                    f"'{bucket_name_keyword_arg_name}' and '{object_name_keyword_arg_name}' "
                    f"or {object_url_keyword_arg_name}"
                )
            if not object_name:
                raise TypeError(
                    f"{func.__name__}() missing 1 required positional argument: "
                    f"'{object_name_keyword_arg_name}'"
                )
            if not bucket_name:
                raise TypeError(
                    f"{func.__name__}() missing 1 required positional argument: "
                    f"'{bucket_name_keyword_arg_name}'"
                )

            return func(self, *args, **kwargs)

        return cast(T, _inner_wrapper)

    return _wrapper


class GCSHook(GoogleBaseHook):
    """
    Interact with Google Cloud Storage. This hook uses the Google Cloud
    connection.
    """

    _conn = None  # type: Optional[storage.Client]

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        google_cloud_storage_conn_id: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        # To preserve backward compatibility
        # TODO: remove one day
        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            gcp_conn_id = google_cloud_storage_conn_id
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> storage.Client:
        """Returns a Google Cloud Storage service object."""
        if not self._conn:
            self._conn = storage.Client(
                credentials=self._get_credentials(), client_info=self.client_info, project=self.project_id
            )

        return self._conn

    def copy(
        self,
        source_bucket: str,
        source_object: str,
        destination_bucket: Optional[str] = None,
        destination_object: Optional[str] = None,
    ) -> None:
        """
        Copies an object from a bucket to another, with renaming if requested.

        destination_bucket or destination_object can be omitted, in which case
        source bucket/object is used, but not both.

        :param source_bucket: The bucket of the object to copy from.
        :type source_bucket: str
        :param source_object: The object to copy.
        :type source_object: str
        :param destination_bucket: The destination of the object to copied to.
            Can be omitted; then the same bucket is used.
        :type destination_bucket: str
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        :type destination_object: str
        """
        destination_bucket = destination_bucket or source_bucket
        destination_object = destination_object or source_object

        if source_bucket == destination_bucket and source_object == destination_object:

            raise ValueError(
                'Either source/destination bucket or source/destination object '
                'must be different, not both the same: bucket=%s, object=%s' % (source_bucket, source_object)
            )
        if not source_bucket or not source_object:
            raise ValueError('source_bucket and source_object cannot be empty.')

        client = self.get_conn()
        source_bucket = client.bucket(source_bucket)
        source_object = source_bucket.blob(source_object)  # type: ignore[attr-defined]
        destination_bucket = client.bucket(destination_bucket)
        destination_object = source_bucket.copy_blob(  # type: ignore[attr-defined]
            blob=source_object, destination_bucket=destination_bucket, new_name=destination_object
        )

        self.log.info(
            'Object %s in bucket %s copied to object %s in bucket %s',
            source_object.name,  # type: ignore[attr-defined]
            source_bucket.name,  # type: ignore[attr-defined]
            destination_object.name,  # type: ignore[union-attr]
            destination_bucket.name,  # type: ignore[union-attr]
        )

    def rewrite(
        self,
        source_bucket: str,
        source_object: str,
        destination_bucket: str,
        destination_object: Optional[str] = None,
    ) -> None:
        """
        Has the same functionality as copy, except that will work on files
        over 5 TB, as well as when copying between locations and/or storage
        classes.

        destination_object can be omitted, in which case source_object is used.

        :param source_bucket: The bucket of the object to copy from.
        :type source_bucket: str
        :param source_object: The object to copy.
        :type source_object: str
        :param destination_bucket: The destination of the object to copied to.
        :type destination_bucket: str
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        :type destination_object: str
        """
        destination_object = destination_object or source_object
        if source_bucket == destination_bucket and source_object == destination_object:
            raise ValueError(
                'Either source/destination bucket or source/destination object '
                'must be different, not both the same: bucket=%s, object=%s' % (source_bucket, source_object)
            )
        if not source_bucket or not source_object:
            raise ValueError('source_bucket and source_object cannot be empty.')

        client = self.get_conn()
        source_bucket = client.bucket(source_bucket)
        source_object = source_bucket.blob(blob_name=source_object)  # type: ignore[attr-defined]
        destination_bucket = client.bucket(destination_bucket)

        token, bytes_rewritten, total_bytes = destination_bucket.blob(  # type: ignore[attr-defined]
            blob_name=destination_object
        ).rewrite(source=source_object)

        self.log.info('Total Bytes: %s | Bytes Written: %s', total_bytes, bytes_rewritten)

        while token is not None:
            token, bytes_rewritten, total_bytes = destination_bucket.blob(  # type: ignore[attr-defined]
                blob_name=destination_object
            ).rewrite(source=source_object, token=token)

            self.log.info('Total Bytes: %s | Bytes Written: %s', total_bytes, bytes_rewritten)
        self.log.info(
            'Object %s in bucket %s rewritten to object %s in bucket %s',
            source_object.name,  # type: ignore[attr-defined]
            source_bucket.name,  # type: ignore[attr-defined]
            destination_object,
            destination_bucket.name,  # type: ignore[attr-defined]
        )

    def download(
        self,
        object_name: str,
        bucket_name: Optional[str],
        filename: Optional[str] = None,
        chunk_size: Optional[int] = None,
        timeout: Optional[int] = DEFAULT_TIMEOUT,
        num_max_attempts: Optional[int] = 1,
    ) -> Union[str, bytes]:
        """
        Downloads a file from Google Cloud Storage.

        When no filename is supplied, the operator loads the file into memory and returns its
        content. When a filename is supplied, it writes the file to the specified location and
        returns the location. For file sizes that exceed the available memory it is recommended
        to write to a file.

        :param object_name: The object to fetch.
        :type object_name: str
        :param bucket_name: The bucket to fetch from.
        :type bucket_name: str
        :param filename: If set, a local file path where the file should be written to.
        :type filename: str
        :param chunk_size: Blob chunk size.
        :type chunk_size: int
        :param timeout: Request timeout in seconds.
        :type timeout: int
        :param num_max_attempts: Number of attempts to download the file.
        :type num_max_attempts: int
        """
        # TODO: future improvement check file size before downloading,
        #  to check for local space availability

        num_file_attempts = 0

        while num_file_attempts < num_max_attempts:
            try:
                num_file_attempts += 1
                client = self.get_conn()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(blob_name=object_name, chunk_size=chunk_size)

                if filename:
                    blob.download_to_filename(filename, timeout=timeout)
                    self.log.info('File downloaded to %s', filename)
                    return filename
                else:
                    return blob.download_as_string()

            except GoogleCloudError:
                if num_file_attempts == num_max_attempts:
                    self.log.error(
                        'Download attempt of object: %s from %s has failed. Attempt: %s, max %s.',
                        object_name,
                        object_name,
                        num_file_attempts,
                        num_max_attempts,
                    )
                    raise

                # Wait with exponential backoff scheme before retrying.
                timeout_seconds = 1.0 * 2 ** (num_file_attempts - 1)
                time.sleep(timeout_seconds)
                continue

    @_fallback_object_url_to_object_name_and_bucket_name()
    @contextmanager
    def provide_file(
        self,
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
        object_url: Optional[str] = None,  # pylint: disable=unused-argument
    ):
        """
        Downloads the file to a temporary directory and returns a file handle

        You can use this method by passing the bucket_name and object_name parameters
        or just object_url parameter.

        :param bucket_name: The bucket to fetch from.
        :type bucket_name: str
        :param object_name: The object to fetch.
        :type object_name: str
        :param object_url: File reference url. Must start with "gs: //"
        :type object_url: str
        :return: File handler
        """
        if object_name is None:
            raise ValueError("Object name can not be empty")
        _, _, file_name = object_name.rpartition("/")
        with NamedTemporaryFile(suffix=file_name) as tmp_file:
            self.download(bucket_name=bucket_name, object_name=object_name, filename=tmp_file.name)
            tmp_file.flush()
            yield tmp_file

    @_fallback_object_url_to_object_name_and_bucket_name()
    @contextmanager
    def provide_file_and_upload(
        self,
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
        object_url: Optional[str] = None,  # pylint: disable=unused-argument
    ):
        """
        Creates temporary file, returns a file handle and uploads the files content
        on close.

        You can use this method by passing the bucket_name and object_name parameters
        or just object_url parameter.

        :param bucket_name: The bucket to fetch from.
        :type bucket_name: str
        :param object_name: The object to fetch.
        :type object_name: str
        :param object_url: File reference url. Must start with "gs: //"
        :type object_url: str
        :return: File handler
        """
        if object_name is None:
            raise ValueError("Object name can not be empty")

        _, _, file_name = object_name.rpartition("/")
        with NamedTemporaryFile(suffix=file_name) as tmp_file:
            yield tmp_file
            tmp_file.flush()
            self.upload(bucket_name=bucket_name, object_name=object_name, filename=tmp_file.name)

    def upload(  # pylint: disable=too-many-arguments
        self,
        bucket_name: str,
        object_name: str,
        filename: Optional[str] = None,
        data: Optional[Union[str, bytes]] = None,
        mime_type: Optional[str] = None,
        gzip: bool = False,
        encoding: str = 'utf-8',
        chunk_size: Optional[int] = None,
        timeout: Optional[int] = DEFAULT_TIMEOUT,
        num_max_attempts: int = 1,
    ) -> None:
        """
        Uploads a local file or file data as string or bytes to Google Cloud Storage.

        :param bucket_name: The bucket to upload to.
        :type bucket_name: str
        :param object_name: The object name to set when uploading the file.
        :type object_name: str
        :param filename: The local file path to the file to be uploaded.
        :type filename: str
        :param data: The file's data as a string or bytes to be uploaded.
        :type data: str
        :param mime_type: The file's mime type set when uploading the file.
        :type mime_type: str
        :param gzip: Option to compress local file or file data for upload
        :type gzip: bool
        :param encoding: bytes encoding for file data if provided as string
        :type encoding: str
        :param chunk_size: Blob chunk size.
        :type chunk_size: int
        :param timeout: Request timeout in seconds.
        :type timeout: int
        :param num_max_attempts: Number of attempts to try to upload the file.
        :type num_max_attempts: int
        """

        def _call_with_retry(f: Callable[[], None]) -> None:
            """Helper functions to upload a file or a string with a retry mechanism and exponential back-off.
            :param f: Callable that should be retried.
            :type f: Callable[[], None]
            """
            num_file_attempts = 0

            while num_file_attempts < num_max_attempts:
                try:
                    num_file_attempts += 1
                    f()

                except GoogleCloudError as e:
                    if num_file_attempts == num_max_attempts:
                        self.log.error(
                            'Upload attempt of object: %s from %s has failed. Attempt: %s, max %s.',
                            object_name,
                            object_name,
                            num_file_attempts,
                            num_max_attempts,
                        )
                        raise e

                    # Wait with exponential backoff scheme before retrying.
                    timeout_seconds = 1.0 * 2 ** (num_file_attempts - 1)
                    time.sleep(timeout_seconds)
                    continue

        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=object_name, chunk_size=chunk_size)
        if filename and data:
            raise ValueError(
                "'filename' and 'data' parameter provided. Please "
                "specify a single parameter, either 'filename' for "
                "local file uploads or 'data' for file content uploads."
            )
        elif filename:
            if not mime_type:
                mime_type = 'application/octet-stream'
            if gzip:
                filename_gz = filename + '.gz'

                with open(filename, 'rb') as f_in:
                    with gz.open(filename_gz, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        filename = filename_gz

            _call_with_retry(
                partial(blob.upload_from_filename, filename=filename, content_type=mime_type, timeout=timeout)
            )

            if gzip:
                os.remove(filename)
            self.log.info('File %s uploaded to %s in %s bucket', filename, object_name, bucket_name)
        elif data:
            if not mime_type:
                mime_type = 'text/plain'
            if gzip:
                if isinstance(data, str):
                    data = bytes(data, encoding)
                out = BytesIO()
                with gz.GzipFile(fileobj=out, mode="w") as f:
                    f.write(data)
                data = out.getvalue()

            _call_with_retry(partial(blob.upload_from_string, data, content_type=mime_type, timeout=timeout))

            self.log.info('Data stream uploaded to %s in %s bucket', object_name, bucket_name)
        else:
            raise ValueError("'filename' and 'data' parameter missing. One is required to upload to gcs.")

    def exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Checks for the existence of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :type bucket_name: str
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        :type object_name: str
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=object_name)
        return blob.exists()

    def get_blob_update_time(self, bucket_name: str, object_name: str):
        """
        Get the update time of a file in Google Cloud Storage

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :type bucket_name: str
        :param object_name: The name of the blob to get updated time from the Google cloud
            storage bucket.
        :type object_name: str
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name=object_name)
        if blob is None:
            raise ValueError(f"Object ({object_name}) not found in Bucket ({bucket_name})")
        return blob.updated

    def is_updated_after(self, bucket_name: str, object_name: str, ts: datetime) -> bool:
        """
        Checks if an blob_name is updated in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google cloud
            storage bucket.
        :type object_name: str
        :param ts: The timestamp to check against.
        :type ts: datetime.datetime
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:

            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            self.log.info("Verify object date: %s > %s", blob_update_time, ts)
            if blob_update_time > ts:
                return True
        return False

    def is_updated_between(
        self, bucket_name: str, object_name: str, min_ts: datetime, max_ts: datetime
    ) -> bool:
        """
        Checks if an blob_name is updated in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google cloud
                storage bucket.
        :type object_name: str
        :param min_ts: The minimum timestamp to check against.
        :type min_ts: datetime.datetime
        :param max_ts: The maximum timestamp to check against.
        :type max_ts: datetime.datetime
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:

            if not min_ts.tzinfo:
                min_ts = min_ts.replace(tzinfo=timezone.utc)
            if not max_ts.tzinfo:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            self.log.info("Verify object date: %s is between %s and %s", blob_update_time, min_ts, max_ts)
            if min_ts <= blob_update_time < max_ts:
                return True
        return False

    def is_updated_before(self, bucket_name: str, object_name: str, ts: datetime) -> bool:
        """
        Checks if an blob_name is updated before given time in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google cloud
            storage bucket.
        :type object_name: str
        :param ts: The timestamp to check against.
        :type ts: datetime.datetime
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:

            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            self.log.info("Verify object date: %s < %s", blob_update_time, ts)
            if blob_update_time < ts:
                return True
        return False

    def is_older_than(self, bucket_name: str, object_name: str, seconds: int) -> bool:
        """
        Check if object is older than given time

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google cloud
            storage bucket.
        :type object_name: str
        :param seconds: The time in seconds to check against
        :type seconds: int
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:
            from datetime import timedelta

            current_time = timezone.utcnow()
            given_time = current_time - timedelta(seconds=seconds)
            self.log.info("Verify object date: %s is older than %s", blob_update_time, given_time)
            if blob_update_time < given_time:
                return True
        return False

    def delete(self, bucket_name: str, object_name: str) -> None:
        """
        Deletes an object from the bucket.

        :param bucket_name: name of the bucket, where the object resides
        :type bucket_name: str
        :param object_name: name of the object to delete
        :type object_name: str
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=object_name)
        blob.delete()

        self.log.info('Blob %s deleted.', object_name)

    def delete_bucket(self, bucket_name: str, force: bool = False) -> None:
        """
        Delete a bucket object from the Google Cloud Storage.

        :param bucket_name: name of the bucket which will be deleted
        :type bucket_name: str
        :param force: false not allow to delete non empty bucket, set force=True
            allows to delete non empty bucket
        :type: bool
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)

        self.log.info("Deleting %s bucket", bucket_name)
        try:
            bucket.delete(force=force)
            self.log.info("Bucket %s has been deleted", bucket_name)
        except NotFound:
            self.log.info("Bucket %s not exists", bucket_name)

    def list(self, bucket_name, versions=None, max_results=None, prefix=None, delimiter=None) -> list:
        """
        List all objects from the bucket with the give string prefix in name

        :param bucket_name: bucket name
        :type bucket_name: str
        :param versions: if true, list all versions of the objects
        :type versions: bool
        :param max_results: max count of items to return in a single page of responses
        :type max_results: int
        :param prefix: prefix string which filters objects whose name begin with
            this prefix
        :type prefix: str
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: str
        :return: a stream of object names matching the filtering criteria
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)

        ids = []
        page_token = None
        while True:
            blobs = bucket.list_blobs(
                max_results=max_results,
                page_token=page_token,
                prefix=prefix,
                delimiter=delimiter,
                versions=versions,
            )

            blob_names = []
            for blob in blobs:
                blob_names.append(blob.name)

            prefixes = blobs.prefixes
            if prefixes:
                ids += list(prefixes)
            else:
                ids += blob_names

            page_token = blobs.next_page_token
            if page_token is None:
                # empty next page token
                break
        return ids

    def list_by_timespan(
        self,
        bucket_name: str,
        timespan_start: datetime,
        timespan_end: datetime,
        versions: bool = None,
        max_results: int = None,
        prefix: str = None,
        delimiter: str = None,
    ) -> list:
        """
        List all objects from the bucket with the give string prefix in name that were
        updated in the time between ``timespan_start`` and ``timespan_end``.

        :param bucket_name: bucket name
        :type bucket_name: str
        :param timespan_start: will return objects that were updated at or after this datetime (UTC)
        :type timespan_start: datetime
        :param timespan_end: will return objects that were updated before this datetime (UTC)
        :type timespan_end: datetime
        :param versions: if true, list all versions of the objects
        :type versions: bool
        :param max_results: max count of items to return in a single page of responses
        :type max_results: int
        :param prefix: prefix string which filters objects whose name begin with
            this prefix
        :type prefix: str
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: str
        :return: a stream of object names matching the filtering criteria
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)

        ids = []
        page_token = None

        while True:
            blobs = bucket.list_blobs(
                max_results=max_results,
                page_token=page_token,
                prefix=prefix,
                delimiter=delimiter,
                versions=versions,
            )

            blob_names = []
            for blob in blobs:
                if timespan_start <= blob.updated.replace(tzinfo=timezone.utc) < timespan_end:
                    blob_names.append(blob.name)

            prefixes = blobs.prefixes
            if prefixes:
                ids += list(prefixes)
            else:
                ids += blob_names

            page_token = blobs.next_page_token
            if page_token is None:
                # empty next page token
                break
        return ids

    def get_size(self, bucket_name: str, object_name: str) -> int:
        """
        Gets the size of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google
            cloud storage bucket_name.
        :type object_name: str

        """
        self.log.info('Checking the file size of object: %s in bucket_name: %s', object_name, bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name=object_name)
        blob_size = blob.size
        self.log.info('The file size of %s is %s bytes.', object_name, blob_size)
        return blob_size

    def get_crc32c(self, bucket_name: str, object_name: str):
        """
        Gets the CRC32c checksum of an object in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google cloud
            storage bucket_name.
        :type object_name: str
        """
        self.log.info(
            'Retrieving the crc32c checksum of object_name: %s in bucket_name: %s',
            object_name,
            bucket_name,
        )
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name=object_name)
        blob_crc32c = blob.crc32c
        self.log.info('The crc32c checksum of %s is %s', object_name, blob_crc32c)
        return blob_crc32c

    def get_md5hash(self, bucket_name: str, object_name: str) -> str:
        """
        Gets the MD5 hash of an object in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :type bucket_name: str
        :param object_name: The name of the object to check in the Google cloud
            storage bucket_name.
        :type object_name: str
        """
        self.log.info('Retrieving the MD5 hash of object: %s in bucket: %s', object_name, bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name=object_name)
        blob_md5hash = blob.md5_hash
        self.log.info('The md5Hash of %s is %s', object_name, blob_md5hash)
        return blob_md5hash

    @GoogleBaseHook.fallback_to_default_project_id
    def create_bucket(
        self,
        bucket_name: str,
        resource: Optional[dict] = None,
        storage_class: str = 'MULTI_REGIONAL',
        location: str = 'US',
        project_id: Optional[str] = None,
        labels: Optional[dict] = None,
    ) -> str:
        """
        Creates a new bucket. Google Cloud Storage uses a flat namespace, so
        you can't create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :param resource: An optional dict with parameters for creating the bucket.
            For information on available parameters, see Cloud Storage API doc:
            https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
        :type resource: dict
        :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage. Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.

            If this value is not specified when the bucket is
            created, it will default to STANDARD.
        :type storage_class: str
        :param location: The location of the bucket.
            Object data for objects in the bucket resides in physical storage
            within this region. Defaults to US.

            .. seealso::
                https://developers.google.com/storage/docs/bucket-locations

        :type location: str
        :param project_id: The ID of the Google Cloud Project.
        :type project_id: str
        :param labels: User-provided labels, in key/value pairs.
        :type labels: dict
        :return: If successful, it returns the ``id`` of the bucket.
        """
        self.log.info(
            'Creating Bucket: %s; Location: %s; Storage Class: %s', bucket_name, location, storage_class
        )

        # Add airflow-version label to the bucket
        labels = labels or {}
        labels['airflow-version'] = 'v' + version.replace('.', '-').replace('+', '-')

        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        bucket_resource = resource or {}

        for item in bucket_resource:
            if item != "name":
                bucket._patch_property(  # pylint: disable=protected-access
                    name=item, value=resource[item]  # type: ignore[index]
                )

        bucket.storage_class = storage_class
        bucket.labels = labels
        bucket.create(project=project_id, location=location)
        return bucket.id

    def insert_bucket_acl(
        self, bucket_name: str, entity: str, role: str, user_project: Optional[str] = None
    ) -> None:
        """
        Creates a new ACL entry on the specified bucket_name.
        See: https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert

        :param bucket_name: Name of a bucket_name.
        :type bucket_name: str
        :param entity: The entity holding the permission, in one of the following forms:
            user-userId, user-email, group-groupId, group-email, domain-domain,
            project-team-projectId, allUsers, allAuthenticatedUsers.
            See: https://cloud.google.com/storage/docs/access-control/lists#scopes
        :type entity: str
        :param role: The access permission for the entity.
            Acceptable values are: "OWNER", "READER", "WRITER".
        :type role: str
        :param user_project: (Optional) The project to be billed for this request.
            Required for Requester Pays buckets.
        :type user_project: str
        """
        self.log.info('Creating a new ACL entry in bucket: %s', bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        bucket.acl.reload()
        bucket.acl.entity_from_dict(entity_dict={"entity": entity, "role": role})
        if user_project:
            bucket.acl.user_project = user_project
        bucket.acl.save()

        self.log.info('A new ACL entry created in bucket: %s', bucket_name)

    def insert_object_acl(
        self,
        bucket_name: str,
        object_name: str,
        entity: str,
        role: str,
        generation: Optional[int] = None,
        user_project: Optional[str] = None,
    ) -> None:
        """
        Creates a new ACL entry on the specified object.
        See: https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert

        :param bucket_name: Name of a bucket_name.
        :type bucket_name: str
        :param object_name: Name of the object. For information about how to URL encode
            object names to be path safe, see:
            https://cloud.google.com/storage/docs/json_api/#encoding
        :type object_name: str
        :param entity: The entity holding the permission, in one of the following forms:
            user-userId, user-email, group-groupId, group-email, domain-domain,
            project-team-projectId, allUsers, allAuthenticatedUsers
            See: https://cloud.google.com/storage/docs/access-control/lists#scopes
        :type entity: str
        :param role: The access permission for the entity.
            Acceptable values are: "OWNER", "READER".
        :type role: str
        :param generation: Optional. If present, selects a specific revision of this object.
        :type generation: long
        :param user_project: (Optional) The project to be billed for this request.
            Required for Requester Pays buckets.
        :type user_project: str
        """
        self.log.info('Creating a new ACL entry for object: %s in bucket: %s', object_name, bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        blob = bucket.blob(blob_name=object_name, generation=generation)
        # Reload fetches the current ACL from Cloud Storage.
        blob.acl.reload()
        blob.acl.entity_from_dict(entity_dict={"entity": entity, "role": role})
        if user_project:
            blob.acl.user_project = user_project
        blob.acl.save()

        self.log.info('A new ACL entry created for object: %s in bucket: %s', object_name, bucket_name)

    def compose(self, bucket_name: str, source_objects: List, destination_object: str) -> None:
        """
        Composes a list of existing object into a new object in the same storage bucket_name

        Currently it only supports up to 32 objects that can be concatenated
        in a single operation

        https://cloud.google.com/storage/docs/json_api/v1/objects/compose

        :param bucket_name: The name of the bucket containing the source objects.
            This is also the same bucket to store the composed destination object.
        :type bucket_name: str
        :param source_objects: The list of source objects that will be composed
            into a single object.
        :type source_objects: list
        :param destination_object: The path of the object if given.
        :type destination_object: str
        """
        if not source_objects:
            raise ValueError('source_objects cannot be empty.')

        if not bucket_name or not destination_object:
            raise ValueError('bucket_name and destination_object cannot be empty.')

        self.log.info("Composing %s to %s in the bucket %s", source_objects, destination_object, bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        destination_blob = bucket.blob(destination_object)
        destination_blob.compose(
            sources=[bucket.blob(blob_name=source_object) for source_object in source_objects]
        )

        self.log.info("Completed successfully.")

    def sync(
        self,
        source_bucket: str,
        destination_bucket: str,
        source_object: Optional[str] = None,
        destination_object: Optional[str] = None,
        recursive: bool = True,
        allow_overwrite: bool = False,
        delete_extra_files: bool = False,
    ) -> None:
        """
        Synchronizes the contents of the buckets.

        Parameters ``source_object`` and ``destination_object`` describe the root sync directories. If they
        are not passed, the entire bucket will be synchronized. If they are passed, they should point
        to directories.

        .. note::
            The synchronization of individual files is not supported. Only entire directories can be
            synchronized.

        :param source_bucket: The name of the bucket containing the source objects.
        :type source_bucket: str
        :param destination_bucket: The name of the bucket containing the destination objects.
        :type destination_bucket: str
        :param source_object: The root sync directory in the source bucket.
        :type source_object: Optional[str]
        :param destination_object: The root sync directory in the destination bucket.
        :type destination_object: Optional[str]
        :param recursive: If True, subdirectories will be considered
        :type recursive: bool
        :param recursive: If True, subdirectories will be considered
        :type recursive: bool
        :param allow_overwrite: if True, the files will be overwritten if a mismatched file is found.
            By default, overwriting files is not allowed
        :type allow_overwrite: bool
        :param delete_extra_files: if True, deletes additional files from the source that not found in the
            destination. By default extra files are not deleted.

            .. note::
                This option can delete data quickly if you specify the wrong source/destination combination.

        :type delete_extra_files: bool
        :return: none
        """
        client = self.get_conn()
        # Create bucket object
        source_bucket_obj = client.bucket(source_bucket)
        destination_bucket_obj = client.bucket(destination_bucket)
        # Normalize parameters when they are passed
        source_object = self._normalize_directory_path(source_object)
        destination_object = self._normalize_directory_path(destination_object)
        # Calculate the number of characters that remove from the name, because they contain information
        # about the parent's path
        source_object_prefix_len = len(source_object) if source_object else 0
        # Prepare synchronization plan
        to_copy_blobs, to_delete_blobs, to_rewrite_blobs = self._prepare_sync_plan(
            source_bucket=source_bucket_obj,
            destination_bucket=destination_bucket_obj,
            source_object=source_object,
            destination_object=destination_object,
            recursive=recursive,
        )
        self.log.info(
            "Planned synchronization. To delete blobs count: %s, to upload blobs count: %s, "
            "to rewrite blobs count: %s",
            len(to_delete_blobs),
            len(to_copy_blobs),
            len(to_rewrite_blobs),
        )

        # Copy missing object to new bucket
        if not to_copy_blobs:
            self.log.info("Skipped blobs copying.")
        else:
            for blob in to_copy_blobs:
                dst_object = self._calculate_sync_destination_path(
                    blob, destination_object, source_object_prefix_len
                )
                self.copy(
                    source_bucket=source_bucket_obj.name,
                    source_object=blob.name,
                    destination_bucket=destination_bucket_obj.name,
                    destination_object=dst_object,
                )
            self.log.info("Blobs copied.")
        # Delete redundant files
        if not to_delete_blobs:
            self.log.info("Skipped blobs deleting.")
        elif delete_extra_files:
            # TODO: Add batch. I tried to do it, but the Google library is not stable at the moment.
            for blob in to_delete_blobs:
                self.delete(blob.bucket.name, blob.name)
            self.log.info("Blobs deleted.")

        # Overwrite files that are different
        if not to_rewrite_blobs:
            self.log.info("Skipped blobs overwriting.")
        elif allow_overwrite:
            for blob in to_rewrite_blobs:
                dst_object = self._calculate_sync_destination_path(
                    blob, destination_object, source_object_prefix_len
                )
                self.rewrite(
                    source_bucket=source_bucket_obj.name,
                    source_object=blob.name,
                    destination_bucket=destination_bucket_obj.name,
                    destination_object=dst_object,
                )
            self.log.info("Blobs rewritten.")

        self.log.info("Synchronization finished.")

    def _calculate_sync_destination_path(
        self, blob: storage.Blob, destination_object: Optional[str], source_object_prefix_len: int
    ) -> str:
        return (
            path.join(destination_object, blob.name[source_object_prefix_len:])
            if destination_object
            else blob.name[source_object_prefix_len:]
        )

    def _normalize_directory_path(self, source_object: Optional[str]) -> Optional[str]:
        return source_object + "/" if source_object and not source_object.endswith("/") else source_object

    @staticmethod
    def _prepare_sync_plan(
        source_bucket: storage.Bucket,
        destination_bucket: storage.Bucket,
        source_object: Optional[str],
        destination_object: Optional[str],
        recursive: bool,
    ) -> Tuple[Set[storage.Blob], Set[storage.Blob], Set[storage.Blob]]:
        # Calculate the number of characters that remove from the name, because they contain information
        # about the parent's path
        source_object_prefix_len = len(source_object) if source_object else 0
        destination_object_prefix_len = len(destination_object) if destination_object else 0
        delimiter = "/" if not recursive else None
        # Fetch blobs list
        source_blobs = list(source_bucket.list_blobs(prefix=source_object, delimiter=delimiter))
        destination_blobs = list(
            destination_bucket.list_blobs(prefix=destination_object, delimiter=delimiter)
        )
        # Create indexes that allow you to identify blobs based on their name
        source_names_index = {a.name[source_object_prefix_len:]: a for a in source_blobs}
        destination_names_index = {a.name[destination_object_prefix_len:]: a for a in destination_blobs}
        # Create sets with names without parent object name
        source_names = set(source_names_index.keys())
        destination_names = set(destination_names_index.keys())
        # Determine objects to copy and delete
        to_copy = source_names - destination_names
        to_delete = destination_names - source_names
        to_copy_blobs = {source_names_index[a] for a in to_copy}  # type: Set[storage.Blob]
        to_delete_blobs = {destination_names_index[a] for a in to_delete}  # type: Set[storage.Blob]
        # Find names that are in both buckets
        names_to_check = source_names.intersection(destination_names)
        to_rewrite_blobs = set()  # type: Set[storage.Blob]
        # Compare objects based on crc32
        for current_name in names_to_check:
            source_blob = source_names_index[current_name]
            destination_blob = destination_names_index[current_name]
            # if the objects are different, save it
            if source_blob.crc32c != destination_blob.crc32c:
                to_rewrite_blobs.add(source_blob)
        return to_copy_blobs, to_delete_blobs, to_rewrite_blobs


def gcs_object_is_directory(bucket: str) -> bool:
    """
    Return True if given Google Cloud Storage URL (gs://<bucket>/<blob>)
    is a directory or an empty bucket. Otherwise return False.
    """
    _, blob = _parse_gcs_url(bucket)

    return len(blob) == 0 or blob.endswith('/')


def _parse_gcs_url(gsurl: str) -> Tuple[str, str]:
    """
    Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
    tuple containing the corresponding bucket and blob.
    """
    parsed_url = urlparse(gsurl)
    if not parsed_url.netloc:
        raise AirflowException('Please provide a bucket name')
    if parsed_url.scheme.lower() != "gs":
        raise AirflowException(f"Schema must be to 'gs://': Current schema: '{parsed_url.scheme}://'")

    bucket = parsed_url.netloc
    # Remove leading '/' but NOT trailing one
    blob = parsed_url.path.lstrip('/')
    return bucket, blob
