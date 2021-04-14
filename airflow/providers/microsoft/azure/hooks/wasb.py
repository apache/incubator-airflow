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
This module contains integration with Azure Blob Storage.

It communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).

"""

from typing import Any, Dict, List, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient, StorageStreamDownloader

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class WasbHook(BaseHook):
    """
    Interacts with Azure Blob Storage through the ``wasb://`` protocol.

    These parameters have to be passed in Airflow Data Base: account_name and account_key.

    Additional options passed in the 'extra' field of the connection will be
    passed to the `BlockBlockService()` constructor. For example, authenticate
    using a SAS token by adding {"sas_token": "YOUR_TOKEN"}.

    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param public_read: Whether an anonymous public read access should be used. default is False
    :type public_read: bool
    """

    conn_name_attr = 'wasb_conn_id'
    default_conn_name = 'wasb_default'
    conn_type = 'wasb'
    hook_name = 'Azure Blob Storage'

    def __init__(self, wasb_conn_id: str = default_conn_name, public_read: bool = False) -> None:
        super().__init__()
        self.conn_id = wasb_conn_id
        self.public_read = public_read
        self.connection = self.get_conn()

    def get_conn(self) -> BlobServiceClient:  # pylint: disable=too-many-return-statements
        """Return the BlobServiceClient object."""
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        if self.public_read:
            # Here we use anonymous public read
            # more info
            # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-manage-access-to-resources
            return BlobServiceClient(account_url=conn.host)

        if extra.get('connection_string'):
            # connection_string auth takes priority
            return BlobServiceClient.from_connection_string(extra.get('connection_string'))
        if extra.get('shared_access_key'):
            # using shared access key
            return BlobServiceClient(account_url=conn.host, credential=extra.get('shared_access_key'))
        if extra.get('tenant_id'):
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            token_credential = ClientSecretCredential(extra.get('tenant_id'), app_id, app_secret)
            return BlobServiceClient(account_url=conn.host, credential=token_credential)
        sas_token = extra.get('sas_token')
        if sas_token and sas_token.startswith('https'):
            return BlobServiceClient(account_url=extra.get('sas_token'))
        if sas_token and not sas_token.startswith('https'):
            return BlobServiceClient(account_url=f"https://{conn.login}.blob.core.windows.net/" + sas_token)
        else:
            # Fall back to old auth
            return BlobServiceClient(
                account_url=f"https://{conn.login}.blob.core.windows.net/", credential=conn.password, **extra
            )

    def _get_container_client(self, container_name: str) -> ContainerClient:
        """
        Instantiates a container client

        :param container_name: The name of the container
        :type container_name: str
        :return: ContainerClient
        """
        return self.connection.get_container_client(container_name)

    def _get_blob_client(self, container_name: str, blob_name: str) -> BlobClient:
        """
        Instantiates a blob client

        :param container_name: The name of the blob container
        :type container_name: str
        :param blob_name: The name of the blob. This needs not be existing
        :type blob_name: str
        """
        container_client = self.create_container(container_name)
        return container_client.get_blob_client(blob_name)

    def check_for_blob(self, container_name: str, blob_name: str, **kwargs) -> bool:
        """
        Check if a blob exists on Azure Blob Storage.

        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments for ``BlobClient.get_blob_properties`` takes.
        :type kwargs: object
        :return: True if the blob exists, False otherwise.
        :rtype: bool
        """
        try:
            self._get_blob_client(container_name, blob_name).get_blob_properties(**kwargs)
        except ResourceNotFoundError:
            return False
        return True

    def check_for_prefix(self, container_name: str, prefix: str, **kwargs):
        """
        Check if a prefix exists on Azure Blob storage.

        :param container_name: Name of the container.
        :type container_name: str
        :param prefix: Prefix of the blob.
        :type prefix: str
        :param kwargs: Optional keyword arguments that ``ContainerClient.walk_blobs`` takes
        :type kwargs: object
        :return: True if blobs matching the prefix exist, False otherwise.
        :rtype: bool
        """
        blobs = self.get_blobs_list(container_name=container_name, prefix=prefix, **kwargs)
        return len(blobs) > 0

    def get_blobs_list(
        self,
        container_name: str,
        prefix: Optional[str] = None,
        include: Optional[List[str]] = None,
        delimiter: Optional[str] = '/',
        **kwargs,
    ) -> List:
        """
        List blobs in a given container

        :param container_name: The name of the container
        :type container_name: str
        :param prefix: Filters the results to return only blobs whose names
            begin with the specified prefix.
        :type prefix: str
        :param include: Specifies one or more additional datasets to include in the
            response. Options include: ``snapshots``, ``metadata``, ``uncommittedblobs``,
            ``copy`, ``deleted``.
        :type include: List[str]
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: str
        """
        container = self._get_container_client(container_name)
        blob_list = []
        blobs = container.walk_blobs(name_starts_with=prefix, include=include, delimiter=delimiter, **kwargs)
        for blob in blobs:
            blob_list.append(blob.name)
        return blob_list

    def load_file(self, file_path: str, container_name: str, blob_name: str, **kwargs) -> None:
        """
        Upload a file to Azure Blob Storage.

        :param file_path: Path to the file to load.
        :type file_path: str
        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that ``BlobClient.upload_blob()`` takes.
        :type kwargs: object
        """
        with open(file_path, 'rb') as data:
            self.upload(container_name=container_name, blob_name=blob_name, data=data, **kwargs)

    def load_string(self, string_data: str, container_name: str, blob_name: str, **kwargs) -> None:
        """
        Upload a string to Azure Blob Storage.

        :param string_data: String to load.
        :type string_data: str
        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that ``BlobClient.upload()`` takes.
        :type kwargs: object
        """
        # Reorder the argument order from airflow.providers.amazon.aws.hooks.s3.load_string.
        self.upload(container_name, blob_name, string_data, **kwargs)

    def get_file(self, file_path: str, container_name: str, blob_name: str, **kwargs):
        """
        Download a file from Azure Blob Storage.

        :param file_path: Path to the file to download.
        :type file_path: str
        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that `BlobClient.download_blob()` takes.
        :type kwargs: object
        """
        with open(file_path, "wb") as fileblob:
            stream = self.download(container_name=container_name, blob_name=blob_name, **kwargs)
            fileblob.write(stream.readall())

    def read_file(self, container_name: str, blob_name: str, **kwargs):
        """
        Read a file from Azure Blob Storage and return as a string.

        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param kwargs: Optional keyword arguments that `BlobClient.download_blob` takes.
        :type kwargs: object
        """
        return self.download(container_name, blob_name, **kwargs).content_as_text()

    def upload(
        self,
        container_name,
        blob_name,
        data,
        blob_type: str = 'BlockBlob',
        length: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Creates a new blob from a data source with automatic chunking.

        :param container_name: The name of the container to upload data
        :type container_name: str
        :param blob_name: The name of the blob to upload. This need not exist in the container
        :type blob_name: str
        :param data: The blob data to upload
        :param blob_type: The type of the blob. This can be either ``BlockBlob``,
            ``PageBlob`` or ``AppendBlob``. The default value is ``BlockBlob``.
        :type blob_type: storage.BlobType
        :param length: Number of bytes to read from the stream. This is optional,
            but should be supplied for optimal performance.
        :type length: int
        """
        blob_client = self._get_blob_client(container_name, blob_name)
        return blob_client.upload_blob(data, blob_type, length=length, **kwargs)

    def download(
        self, container_name, blob_name, offset: Optional[int] = None, length: Optional[int] = None, **kwargs
    ) -> StorageStreamDownloader:
        """
        Downloads a blob to the StorageStreamDownloader

        :param container_name: The name of the container containing the blob
        :type container_name: str
        :param blob_name: The name of the blob to download
        :type blob_name: str
        :param offset: Start of byte range to use for downloading a section of the blob.
            Must be set if length is provided.
        :type offset: int
        :param length: Number of bytes to read from the stream.
        :type length: int
        """
        blob_client = self._get_blob_client(container_name, blob_name)
        return blob_client.download_blob(offset=offset, length=length, **kwargs)

    def create_container(self, container_name: str) -> ContainerClient:
        """
        Create container object if not already existing

        :param container_name: The name of the container to create
        :type container_name: str
        """
        container_client = self._get_container_client(container_name)
        try:
            self.log.info('Attempting to create container: %s', container_name)
            container_client.create_container()
            self.log.info("Created container: %s", container_name)
            return container_client
        except ResourceExistsError:
            self.log.info("Container %s already exists", container_name)
            return container_client

    def delete_container(self, container_name: str) -> None:
        """
        Delete a container object

        :param container_name: The name of the container
        :type container_name: str
        """
        try:
            self.log.info('Attempting to delete container: %s', container_name)
            self._get_container_client(container_name).delete_container()
            self.log.info('Deleted container: %s', container_name)
        except ResourceNotFoundError:
            self.log.info('Container %s not found', container_name)

    def delete_blobs(self, container_name: str, *blobs, **kwargs) -> None:
        """
        Marks the specified blobs or snapshots for deletion.

        :param container_name: The name of the container containing the blobs
        :type container_name: str
        :param blobs: The blobs to delete. This can be a single blob, or multiple values
            can be supplied, where each value is either the name of the blob (str) or BlobProperties.
        :type blobs: Union[str, BlobProperties]
        """
        self._get_container_client(container_name).delete_blobs(*blobs, **kwargs)
        self.log.info("Deleted blobs: %s", blobs)

    def delete_file(
        self,
        container_name: str,
        blob_name: str,
        is_prefix: bool = False,
        ignore_if_missing: bool = False,
        **kwargs,
    ) -> None:
        """
        Delete a file from Azure Blob Storage.

        :param container_name: Name of the container.
        :type container_name: str
        :param blob_name: Name of the blob.
        :type blob_name: str
        :param is_prefix: If blob_name is a prefix, delete all matching files
        :type is_prefix: bool
        :param ignore_if_missing: if True, then return success even if the
            blob does not exist.
        :type ignore_if_missing: bool
        :param kwargs: Optional keyword arguments that ``ContainerClient.delete_blobs()`` takes.
        :type kwargs: object
        """
        if is_prefix:
            blobs_to_delete = self.get_blobs_list(container_name, prefix=blob_name, **kwargs)
        elif self.check_for_blob(container_name, blob_name):
            blobs_to_delete = [blob_name]
        else:
            blobs_to_delete = []
        if not ignore_if_missing and len(blobs_to_delete) == 0:
            raise AirflowException(f'Blob(s) not found: {blob_name}')

        self.delete_blobs(container_name, *blobs_to_delete, **kwargs)
