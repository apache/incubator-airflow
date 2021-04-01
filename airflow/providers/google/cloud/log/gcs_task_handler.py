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
import os
from typing import Collection, Optional

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.client_info import ClientInfo
from google.cloud import storage

from airflow import version
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

_DEFAULT_SCOPESS = frozenset(
    [
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]
)


class GCSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    GCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk.

    :param base_log_folder: Base log folder to place logs.
    :type base_log_folder: str
    :param gcs_log_folder: Path to a remote location where logs will be saved. It must have the prefix
        ``gs://``. For example: ``gs://bucket/remote/log/location``
    :type gcs_log_folder: str
    :param filename_template: template filename string
    :type filename_template: str
    :param gcp_key_path: Path to Google Cloud Service Account file (JSON). Mutually exclusive with
        gcp_keyfile_dict.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :type gcp_key_path: str
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. Mutually exclusive with gcp_key_path.
    :type gcp_keyfile_dict: dict
    :param gcp_scopes: Comma-separated string containing OAuth2 scopes
    :type gcp_scopes: str
    :param project_id: Project ID to read the secrets from. If not passed, the project ID from credentials
        will be used.
    :type project_id: str
    """

    def __init__(
        self,
        *,
        base_log_folder: str,
        gcs_log_folder: str,
        filename_template: str,
        gcp_key_path: Optional[str] = None,
        gcp_keyfile_dict: Optional[dict] = None,
        # See: https://github.com/PyCQA/pylint/issues/2377
        gcp_scopes: Optional[Collection[str]] = _DEFAULT_SCOPESS,  # pylint: disable=unsubscriptable-object
        project_id: Optional[str] = None,
    ):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = gcs_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False
        self.upload_on_close = True
        self.gcp_key_path = gcp_key_path
        self.gcp_keyfile_dict = gcp_keyfile_dict
        self.scopes = gcp_scopes
        self.project_id = project_id

    @cached_property
    def client(self) -> storage.Client:
        """Returns GCS Client."""
        credentials, project_id = get_credentials_and_project_id(
            key_path=self.gcp_key_path,
            keyfile_dict=self.gcp_keyfile_dict,
            scopes=self.scopes,
            disable_logging=True,
        )
        return storage.Client(
            credentials=credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version),
            project=self.project_id if self.project_id else project_id,
        )

    def set_context(self, ti):
        super().set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

    def close(self):
        """Close and upload local log file to remote storage GCS."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc) as logfile:
                log = logfile.read()
            self.gcs_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        try:
            blob = storage.Blob.from_string(remote_loc, self.client)
            remote_log = blob.download_as_bytes().decode()
            log = f'*** Reading remote log from {remote_loc}.\n{remote_log}\n'
            return log, {'end_of_log': True}
        except Exception as e:  # pylint: disable=broad-except
            log = f'*** Unable to read remote log from {remote_loc}\n*** {str(e)}\n\n'
            self.log.error(log)
            local_log, metadata = super()._read(ti, try_number)
            log += local_log
            return log, metadata

    def gcs_write(self, log, remote_log_location):
        """
        Writes the log to the remote_log_location. Fails silently if no log
        was created.

        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        """
        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            old_log = blob.download_as_bytes().decode()
            log = '\n'.join([old_log, log]) if old_log else log
        except Exception as e:  # pylint: disable=broad-except
            if not hasattr(e, 'resp') or e.resp.get('status') != '404':  # pylint: disable=no-member
                log = f'*** Previous log discarded: {str(e)}\n\n' + log
                self.log.info("Previous log discarded: %s", e)

        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            blob.upload_from_string(log, content_type="text/plain")
        except Exception as e:  # pylint: disable=broad-except
            self.log.error('Could not write logs to %s: %s', remote_log_location, e)
