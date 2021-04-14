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
"""Hook for Google Drive service"""
from io import TextIOWrapper
from typing import Any, Optional, Sequence, Union

from googleapiclient.discovery import Resource, build
from googleapiclient.http import HttpRequest, MediaFileUpload

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDriveHook(GoogleBaseHook):
    """
    Hook for the Google Drive APIs.

    :param api_version: API version used (for example v3).
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> Any:
        """
        Retrieves the connection to Google Drive.

        :return: Google Drive services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("drive", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    def _ensure_folders_exists(self, path: str) -> str:
        service = self.get_conn()
        current_parent = "root"
        folders = path.split("/")
        depth = 0
        # First tries to enter directories
        for current_folder in folders:
            self.log.debug("Looking for %s directory with %s parent", current_folder, current_parent)
            conditions = [
                "mimeType = 'application/vnd.google-apps.folder'",
                f"name='{current_folder}'",
                f"'{current_parent}' in parents",
            ]
            result = (
                service.files()  # pylint: disable=no-member
                .list(q=" and ".join(conditions), spaces="drive", fields="files(id, name)")
                .execute(num_retries=self.num_retries)
            )
            files = result.get("files", [])
            if not files:
                self.log.info("Not found %s directory", current_folder)
                # If the directory does not exist, break loops
                break
            depth += 1
            current_parent = files[0].get("id")

        # Check if there are directories to process
        if depth != len(folders):
            # Create missing directories
            for current_folder in folders[depth:]:
                file_metadata = {
                    "name": current_folder,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [current_parent],
                }
                file = (
                    service.files()  # pylint: disable=no-member
                    .create(body=file_metadata, fields="id")
                    .execute(num_retries=self.num_retries)
                )
                self.log.info("Created %s directory", current_folder)

                current_parent = file.get("id")
        # Return the ID of the last directory
        return current_parent

    def get_media_request(self, file_id: str) -> HttpRequest:
        """
        Returns a get_media http request to a Google Drive object.

        :param file_id: The Google Drive file id
        :type file_id: str
        :return: request
        :rtype: HttpRequest
        """
        service = self.get_conn()
        request = service.files().get_media(fileId=file_id)  # pylint: disable=no-member
        return request

    def exists(self, folder_id: str, file_name: str, drive_id: Optional[str] = None):
        """
        Checks to see if a file exists within a Google Drive folder

        :param folder_id: The id of the Google Drive folder in which the file resides
        :type folder_id: str
        :param file_name: The name of a file in Google Drive
        :type file_name: str
        :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
        :type drive_id: str
        :return: True if the file exists, False otherwise
        :rtype: bool
        """
        return bool(self.get_file_id(folder_id=folder_id, file_name=file_name, drive_id=drive_id))

    def get_file_id(self, folder_id: str, file_name: str, drive_id: Optional[str] = None):
        """
        Returns the file id of a Google Drive file

        :param folder_id: The id of the Google Drive folder in which the file resides
        :type folder_id: str
        :param file_name: The name of a file in Google Drive
        :type file_name: str
        :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
        :type drive_id: str
        :return: Google Drive file id if the file exists, otherwise None
        :rtype: str if file exists else None
        """
        query = f"name = '{file_name}'"
        if folder_id:
            query += f" and parents in '{folder_id}'"
        service = self.get_conn()
        if drive_id:
            files = (
                service.files()  # pylint: disable=no-member
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, mimeType)",
                    orderBy="modifiedTime desc",
                    driveId=drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    corpora="drive",
                )
                .execute(num_retries=self.num_retries)
            )
        else:
            files = (
                service.files()  # pylint: disable=no-member
                .list(q=query, spaces="drive", fields="files(id, mimeType)", orderBy="modifiedTime desc")
                .execute(num_retries=self.num_retries)
            )
        file_metadata = {}
        if files['files']:
            file_metadata = {"id": files['files'][0]['id'], "mime_type": files['files'][0]['mimeType']}
        return file_metadata

    def upload_file(self, local_location: str, remote_location: str) -> str:
        """
        Uploads a file that is available locally to a Google Drive service.

        :param local_location: The path where the file is available.
        :type local_location: str
        :param remote_location: The path where the file will be send
        :type remote_location: str
        :return: File ID
        :rtype: str
        """
        service = self.get_conn()
        directory_path, _, file_name = remote_location.rpartition("/")
        if directory_path:
            parent = self._ensure_folders_exists(directory_path)
        else:
            parent = "root"

        file_metadata = {"name": file_name, "parents": [parent]}
        media = MediaFileUpload(local_location)
        file = (
            service.files()  # pylint: disable=no-member
            .create(body=file_metadata, media_body=media, fields="id")
            .execute(num_retries=self.num_retries)
        )
        self.log.info("File %s uploaded to gdrive://%s.", local_location, remote_location)
        return file.get("id")

    def download_file(self, file_id: str, file_handle: TextIOWrapper, chunk_size: int = 104857600):
        """
        Download a file from Google Drive.

        :param file_id: the id of the file
        :type file_id: str
        :param file_handle: file handle used to write the content to
        :type file_handle: io.TextIOWrapper
        """
        request = self.get_media_request(file_id=file_id)
        self.download_content_from_request(file_handle=file_handle, request=request, chunk_size=chunk_size)
