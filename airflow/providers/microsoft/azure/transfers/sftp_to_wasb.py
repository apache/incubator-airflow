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
"""This module contains SFTP to Azure Blob Storage operator."""
import os
from collections import namedtuple
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Tuple

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.decorators import apply_defaults

WILDCARD = "*"
SftpFile = namedtuple('SftpFile', 'sftp_file_path, blob_name')


class SFTPToWasbOperator(BaseOperator):
    """
    Transfer files to Azure Blob Storage from SFTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToWasbOperator`

    :param sftp_source_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the SFTP server.
        You can use only one wildcard within your path. The wildcard can appear
        inside the path or at the end of the path.
    :type sftp_source_path: str
    :param container_name: Name of the container.
    :type container_name: str
    :param blob_prefix: Prefix to name a blob.
    :type blob_prefix: str
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param load_options: Optional keyword arguments that
        ``WasbHook.load_file()`` takes.
    :type load_options: dict
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :type move_object: bool
    """

    template_fields = ("sftp_source_path", "container_name", "blob_prefix")

    @apply_defaults
    def __init__(
        self,
        *,
        sftp_source_path: str,
        container_name: str,
        blob_prefix: str = "",
        sftp_conn_id: str = "sftp_default",
        wasb_conn_id: str = 'wasb_default',
        load_options: Optional[dict] = None,
        move_object: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sftp_source_path = sftp_source_path
        self.blob_prefix = blob_prefix
        self.sftp_conn_id = sftp_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options or {}
        self.move_object = move_object

    def dry_run(self) -> None:
        super().dry_run()
        sftp_files: List[SftpFile] = self.get_sftp_files_map()
        for file in sftp_files:
            self.log.info(
                'Process will upload file from (SFTP) %s to wasb://%s as %s',
                file.sftp_file_path,
                self.container_name,
                file.blob_name,
            )
            if self.move_object:
                self.log.info("Executing delete of %s", file)

    def execute(self, context: Optional[Dict[Any, Any]]) -> None:
        """Upload a file from SFTP to Azure Blob Storage."""
        sftp_files: List[SftpFile] = self.get_sftp_files_map()
        uploaded_files = self.copy_files_to_wasb(sftp_files)
        if self.move_object:
            self.delete_files(uploaded_files)

    def get_sftp_files_map(self) -> List[SftpFile]:
        """Get SFTP files from the source path, it may use a WILDCARD to this end."""
        sftp_files = []

        sftp_complete_path, prefix, delimiter = self.get_tree_behavior()

        found_files, _, _ = self.sftp_hook.get_tree_map(
            sftp_complete_path, prefix=prefix, delimiter=delimiter
        )

        self.log.info("Found %s files at sftp source path: %s", str(len(found_files)), self.sftp_source_path)

        for file in found_files:
            future_blob_name = self.get_future_blob_name(file)
            sftp_files.append(SftpFile(file, future_blob_name))

        return sftp_files

    def get_tree_behavior(self) -> Tuple[str, Optional[str], Optional[str]]:
        """Extracts from source path the tree behavior to interact with the remote folder"""
        self.check_wildcards_limit()

        if self.source_path_contains_wildcard:

            prefix, delimiter = self.sftp_source_path.split(WILDCARD, 1)

            sftp_complete_path = os.path.dirname(prefix)

            return sftp_complete_path, prefix, delimiter

        return self.sftp_source_path, None, None

    def check_wildcards_limit(self) -> Any:
        """Check if there is multiple Wildcard."""
        total_wildcards = self.sftp_source_path.count(WILDCARD)
        if total_wildcards > 1:
            raise AirflowException(
                "Only one wildcard '*' is allowed in sftp_source_path parameter. "
                "Found {} in {}.".format(total_wildcards, self.sftp_source_path)
            )

    @property
    def source_path_contains_wildcard(self) -> bool:
        """Does source path contains a wildcard"""
        return WILDCARD in self.sftp_source_path

    @cached_property
    def sftp_hook(self) -> SFTPHook:
        """Property of sftp hook to be re-used."""
        return SFTPHook(self.sftp_conn_id)

    def get_future_blob_name(self, file: str) -> str:
        """Get a blob name based by the previous name and a blob_prefix variable"""
        return self.blob_prefix + os.path.basename(file)

    def copy_files_to_wasb(self, sftp_files: List[SftpFile]) -> List[str]:
        """Upload a list of files from sftp_files to Azure Blob Storage with a new Blob Name."""
        uploaded_files = []
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        for file in sftp_files:
            with NamedTemporaryFile("w") as tmp:
                self.sftp_hook.retrieve_file(file.sftp_file_path, tmp.name)
                self.log.info(
                    'Uploading %s to wasb://%s as %s',
                    file.sftp_file_path,
                    self.container_name,
                    file.blob_name,
                )
                wasb_hook.load_file(tmp.name, self.container_name, file.blob_name, **self.load_options)

                uploaded_files.append(file.sftp_file_path)

        return uploaded_files

    def delete_files(self, uploaded_files: List[str]) -> None:
        """Performs a move of a list of files at SFTP to Azure Blob Storage."""
        for sftp_file_path in uploaded_files:
            self.log.info("Executing delete of %s", sftp_file_path)
            self.sftp_hook.delete_file(sftp_file_path)
