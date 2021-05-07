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
"""Objects relating to retrieving connections and variables from local file"""
import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from airflow.exceptions import AirflowException, ConnectionNotUnique
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.parse import parse_file

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.models.connection import Connection


def load_variables(file_path: str) -> Dict[str, str]:
    """
    Load variables from a text file.

    ``JSON``, `YAML` and ``.env`` files are supported.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :rtype: Dict[str, List[str]]
    """
    log.debug("Loading variables from a text file")

    secrets = parse_file(file_path)
    invalid_keys = [key for key, values in secrets.items() if isinstance(values, list) and len(values) != 1]
    if invalid_keys:
        raise AirflowException(f'The "{file_path}" file contains multiple values for keys: {invalid_keys}')
    variables = {key: values[0] if isinstance(values, list) else values for key, values in secrets.items()}
    log.debug("Loaded %d variables: ", len(variables))
    return variables


def load_connections(file_path) -> Dict[str, List[Any]]:
    """This function is deprecated. Please use `airflow.secrets.local_filesystem.load_connections_dict`.","""
    warnings.warn(
        "This function is deprecated. Please use `airflow.secrets.local_filesystem.load_connections_dict`.",
        DeprecationWarning,
        stacklevel=2,
    )
    return {k: [v] for k, v in load_connections_dict(file_path).values()}


def load_connections_dict(file_path: str) -> Dict[str, Any]:
    """
    Load connection from text file.

    ``JSON``, `YAML` and ``.env`` files are supported.

    :return: A dictionary where the key contains a connection ID and the value contains the connection.
    :rtype: Dict[str, airflow.models.connection.Connection]
    """
    from airflow.models.connection import Connection

    log.debug("Loading connection")

    secrets: Dict[str, Any] = parse_file(file_path)
    connection_by_conn_id = {}
    for key, secret_values in list(secrets.items()):
        if isinstance(secret_values, list):
            # secret_values is either length 0, 1 or 2+ -- only length 1 is valid
            if not secret_values:
                log.debug("No secret values for %s", key)
                continue

            if len(secret_values) >= 2:
                raise ConnectionNotUnique(f"Found multiple values for {key} in {file_path}.")

            # secret_values must be of length one, so unpack it
            elif secret_values:
                secret_values = secret_values[0]

        if isinstance(secret_values, dict):
            connection_by_conn_id[key] = Connection.from_dict(key, secret_values)
        elif isinstance(secret_values, str):
            connection_by_conn_id[key] = Connection(uri=secret_values)
        else:
            raise AirflowException(f"Unexpected value type: {type(secret_values)}.")

    num_conn = len(connection_by_conn_id)
    log.debug("Loaded %d connections", num_conn)

    return connection_by_conn_id


class LocalFilesystemBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection objects and Variables from local files

    ``JSON``, `YAML` and ``.env`` files are supported.

    :param variables_file_path: File location with variables data.
    :type variables_file_path: str
    :param connections_file_path: File location with connection data.
    :type connections_file_path: str
    """

    def __init__(
        self, variables_file_path: Optional[str] = None, connections_file_path: Optional[str] = None
    ):
        super().__init__()
        self.variables_file = variables_file_path
        self.connections_file = connections_file_path

    @property
    def _local_variables(self) -> Dict[str, str]:
        if not self.variables_file:
            self.log.debug("The file for variables is not specified. Skipping")
            # The user may not specify any file.
            return {}
        secrets = load_variables(self.variables_file)
        return secrets

    @property
    def _local_connections(self) -> Dict[str, 'Connection']:
        if not self.connections_file:
            self.log.debug("The file for connection is not specified. Skipping")
            # The user may not specify any file.
            return {}
        return load_connections_dict(self.connections_file)

    def get_connection(self, conn_id: str) -> Optional['Connection']:
        if conn_id in self._local_connections:
            return self._local_connections[conn_id]
        return None

    def get_connections(self, conn_id: str) -> List[Any]:
        warnings.warn(
            "This method is deprecated. Please use "
            "`airflow.secrets.local_filesystem.LocalFilesystemBackend.get_connection`.",
            PendingDeprecationWarning,
            stacklevel=2,
        )
        conn = self.get_connection(conn_id=conn_id)
        if conn:
            return [conn]
        return []

    def get_variable(self, key: str) -> Optional[str]:
        return self._local_variables.get(key)
