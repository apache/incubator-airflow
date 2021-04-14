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
import json
import logging
import os
import warnings
from collections import defaultdict
from inspect import signature
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

import airflow.utils.yaml as yaml
from airflow.exceptions import (
    AirflowException,
    AirflowFileParseException,
    ConnectionNotUnique,
    FileSyntaxError,
)
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.utils.file import COMMENT_PATTERN
from airflow.utils.log.logging_mixin import LoggingMixin

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.models.connection import Connection


def get_connection_parameter_names() -> Set[str]:
    """Returns :class:`airflow.models.connection.Connection` constructor parameters."""
    from airflow.models.connection import Connection

    return {k for k in signature(Connection.__init__).parameters.keys() if k != "self"}


def _parse_env_file(file_path: str) -> Tuple[Dict[str, List[str]], List[FileSyntaxError]]:
    """
    Parse a file in the ``.env`` format.

    .. code-block:: text

        MY_CONN_ID=my-conn-type://my-login:my-pa%2Fssword@my-host:5432/my-schema?param1=val1&param2=val2

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Tuple with mapping of key and list of values and list of syntax errors
    """
    with open(file_path) as f:
        content = f.read()

    secrets: Dict[str, List[str]] = defaultdict(list)
    errors: List[FileSyntaxError] = []
    for line_no, line in enumerate(content.splitlines(), 1):
        if not line:
            # Ignore empty line
            continue

        if COMMENT_PATTERN.match(line):
            # Ignore comments
            continue

        var_parts: List[str] = line.split("=", 2)
        if len(var_parts) != 2:
            errors.append(
                FileSyntaxError(
                    line_no=line_no,
                    message='Invalid line format. The line should contain at least one equal sign ("=").',
                )
            )
            continue

        key, value = var_parts
        if not key:
            errors.append(
                FileSyntaxError(
                    line_no=line_no,
                    message="Invalid line format. Key is empty.",
                )
            )
        secrets[key].append(value)
    return secrets, errors


def _parse_yaml_file(file_path: str) -> Tuple[Dict[str, List[str]], List[FileSyntaxError]]:
    """
    Parse a file in the YAML format.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Tuple with mapping of key and list of values and list of syntax errors
    """
    with open(file_path) as f:
        content = f.read()

    if not content:
        return {}, [FileSyntaxError(line_no=1, message="The file is empty.")]
    try:
        secrets = yaml.safe_load(content)

    except yaml.MarkedYAMLError as e:
        return {}, [FileSyntaxError(line_no=e.problem_mark.line, message=str(e))]
    if not isinstance(secrets, dict):
        return {}, [FileSyntaxError(line_no=1, message="The file should contain the object.")]

    return secrets, []


def _parse_json_file(file_path: str) -> Tuple[Dict[str, Any], List[FileSyntaxError]]:
    """
    Parse a file in the JSON format.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Tuple with mapping of key and list of values and list of syntax errors
    """
    with open(file_path) as f:
        content = f.read()

    if not content:
        return {}, [FileSyntaxError(line_no=1, message="The file is empty.")]
    try:
        secrets = json.loads(content)
    except JSONDecodeError as e:
        return {}, [FileSyntaxError(line_no=int(e.lineno), message=e.msg)]
    if not isinstance(secrets, dict):
        return {}, [FileSyntaxError(line_no=1, message="The file should contain the object.")]

    return secrets, []


FILE_PARSERS = {
    "env": _parse_env_file,
    "json": _parse_json_file,
    "yaml": _parse_yaml_file,
}


def _parse_secret_file(file_path: str) -> Dict[str, Any]:
    """
    Based on the file extension format, selects a parser, and parses the file.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Map of secret key (e.g. connection ID) and value.
    """
    if not os.path.exists(file_path):
        raise AirflowException(
            f"File {file_path} was not found. Check the configuration of your Secrets backend."
        )

    log.debug("Parsing file: %s", file_path)

    ext = file_path.rsplit(".", 2)[-1].lower()

    if ext not in FILE_PARSERS:
        raise AirflowException(
            "Unsupported file format. The file must have the extension .env or .json or .yaml"
        )

    secrets, parse_errors = FILE_PARSERS[ext](file_path)

    log.debug("Parsed file: len(parse_errors)=%d, len(secrets)=%d", len(parse_errors), len(secrets))

    if parse_errors:
        raise AirflowFileParseException(
            "Failed to load the secret file.", file_path=file_path, parse_errors=parse_errors
        )

    return secrets


def _create_connection(conn_id: str, value: Any):
    """Creates a connection based on a URL or JSON object."""
    from airflow.models.connection import Connection

    if isinstance(value, str):
        return Connection(conn_id=conn_id, uri=value)
    if isinstance(value, dict):
        connection_parameter_names = get_connection_parameter_names() | {"extra_dejson"}
        current_keys = set(value.keys())
        if not current_keys.issubset(connection_parameter_names):
            illegal_keys = current_keys - connection_parameter_names
            illegal_keys_list = ", ".join(illegal_keys)
            raise AirflowException(
                f"The object have illegal keys: {illegal_keys_list}. "
                f"The dictionary can only contain the following keys: {connection_parameter_names}"
            )
        if "extra" in value and "extra_dejson" in value:
            raise AirflowException(
                "The extra and extra_dejson parameters are mutually exclusive. "
                "Please provide only one parameter."
            )
        if "extra_dejson" in value:
            value["extra"] = json.dumps(value["extra_dejson"])
            del value["extra_dejson"]

        if "conn_id" in current_keys and conn_id != value["conn_id"]:
            raise AirflowException(
                f"Mismatch conn_id. "
                f"The dictionary key has the value: {value['conn_id']}. "
                f"The item has the value: {conn_id}."
            )
        value["conn_id"] = conn_id
        return Connection(**value)
    raise AirflowException(
        f"Unexpected value type: {type(value)}. The connection can only be defined using a string or object."
    )


def load_variables(file_path: str) -> Dict[str, str]:
    """
    Load variables from a text file.

    ``JSON``, `YAML` and ``.env`` files are supported.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :rtype: Dict[str, List[str]]
    """
    log.debug("Loading variables from a text file")

    secrets = _parse_secret_file(file_path)
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
    log.debug("Loading connection")

    secrets: Dict[str, Any] = _parse_secret_file(file_path)
    connection_by_conn_id = {}
    for key, secret_values in list(secrets.items()):
        if isinstance(secret_values, list):
            if len(secret_values) > 1:
                raise ConnectionNotUnique(f"Found multiple values for {key} in {file_path}.")

            for secret_value in secret_values:
                connection_by_conn_id[key] = _create_connection(key, secret_value)
        else:
            connection_by_conn_id[key] = _create_connection(key, secret_values)

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
