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
from typing import Iterable, Optional, Sequence, Union

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url, gcs_object_is_directory
from airflow.providers.microsoft.azure.hooks.azure_fileshare import AzureFileShareHook
from airflow.utils.decorators import apply_defaults


class AzureFileShareToGCSOperator(BaseOperator):
    """
    Synchronizes a Azure FileShare directory content (excluding subdirectories),
    possibly filtered by a prefix, with a Google Cloud Storage destination path.

    :param share_name: The Azure FileShare share where to find the objects. (templated)
    :type share_name: str
    :param directory_name: (Optional) Path to Azure FileShare directory which content is to be transferred.
        Defaults to root directory (templated)
    :type directory_name: str
    :param prefix: Prefix string which filters objects whose name begin with
        such prefix. (templated)
    :type prefix: str
    :param wasb_conn_id: The source WASB connection
    :type wasb_conn_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param dest_gcs: The destination Google Cloud Storage bucket and prefix
        where you want to store the files. (templated)
    :type dest_gcs: str
    :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param replace: Whether you want to replace existing destination files
        or not.
    :type replace: bool
    :param gzip: Option to compress file for upload
    :type gzip: bool
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type google_impersonation_chain: Optional[Union[str, Sequence[str]]]

    Note that ``share_name``, ``directory_name``, ``prefix``, ``delimiter`` and ``dest_gcs`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields: Iterable[str] = (
        'share_name',
        'directory_name',
        'prefix',
        'dest_gcs',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        share_name: str,
        dest_gcs: str,
        directory_name: Optional[str] = None,
        prefix: str = '',
        wasb_conn_id: str = 'wasb_default',
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        replace: bool = False,
        gzip: bool = False,
        google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.share_name = share_name
        self.directory_name = directory_name
        self.prefix = prefix
        self.wasb_conn_id = wasb_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.dest_gcs = dest_gcs
        self.delegate_to = delegate_to
        self.replace = replace
        self.gzip = gzip
        self.google_impersonation_chain = google_impersonation_chain

        if dest_gcs and not gcs_object_is_directory(self.dest_gcs):
            self.log.info(
                'Destination Google Cloud Storage path is not a valid '
                '"directory", define a path that ends with a slash "/" or '
                'leave it empty for the root of the bucket.'
            )
            raise AirflowException(
                'The destination Google Cloud Storage path must end with a slash "/" or be empty.'
            )

    def execute(self, context):
        azure_fileshare_hook = AzureFileShareHook(self.wasb_conn_id)
        files = azure_fileshare_hook.list_files(
            share_name=self.share_name, directory_name=self.directory_name
        )

        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.google_impersonation_chain,
        )

        dest_gcs_bucket, dest_gcs_object_prefix = _parse_gcs_url(self.dest_gcs)

        # pylint: disable=too-many-nested-blocks
        if not self.replace:
            # if we are not replacing -> list all files in the GCS bucket
            # and only keep those files which are present in
            # S3 and not in Google Cloud Storage
            existing_files_prefixed = gcs_hook.list(dest_gcs_bucket, prefix=dest_gcs_object_prefix)

            existing_files = []

            # Remove the object prefix itself, an empty directory was found
            if dest_gcs_object_prefix in existing_files_prefixed:
                existing_files_prefixed.remove(dest_gcs_object_prefix)

            # Remove the object prefix from all object string paths
            for file in existing_files_prefixed:
                if file.startswith(dest_gcs_object_prefix):
                    existing_files.append(file[len(dest_gcs_object_prefix) :])
                else:
                    existing_files.append(file)

            files = list(set(files) - set(existing_files))

        if files:
            self.log.info('%s files are going to be synced.', len(files))
        else:
            self.log.info('There are no new files to sync. Have a nice day!')

        for file in files:
            with NamedTemporaryFile() as temp_file:
                azure_fileshare_hook.get_file_to_stream(
                    stream=temp_file,
                    share_name=self.share_name,
                    directory_name=self.directory_name,
                    file_name=file,
                )
                temp_file.flush()

                # There will always be a '/' before file because it is
                # enforced at instantiation time
                dest_gcs_object = dest_gcs_object_prefix + file
                gcs_hook.upload(dest_gcs_bucket, dest_gcs_object, temp_file.name, gzip=self.gzip)

        if files:
            self.log.info("All done, uploaded %d files to Google Cloud Storage.", len(files))
        else:
            self.log.info('In sync, no files needed to be uploaded to Google Cloud Storage')

        return files
