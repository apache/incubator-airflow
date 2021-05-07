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

"""Parse data from a file if it uses a valid format."""
import json
import logging
import os
from collections import defaultdict
from json import JSONDecodeError
from typing import Any, Dict, List, Tuple

import airflow.utils.yaml as yaml
from airflow.exceptions import AirflowException, AirflowFileParseException, FileSyntaxError
from airflow.utils.file import COMMENT_PATTERN

log = logging.getLogger(__name__)


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

    contents_dict: Dict[str, List[str]] = defaultdict(list)
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
        contents_dict[key].append(value)
    return contents_dict, errors


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
        contents_dict = yaml.safe_load(content)

    except yaml.MarkedYAMLError as e:
        return {}, [FileSyntaxError(line_no=e.problem_mark.line, message=str(e))]
    if not isinstance(contents_dict, dict):
        return {}, [FileSyntaxError(line_no=1, message="The file should contain the object.")]

    return contents_dict, []


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
        contents_dict = json.loads(content)
    except JSONDecodeError as e:
        return {}, [FileSyntaxError(line_no=int(e.lineno), message=e.msg)]
    if not isinstance(contents_dict, dict):
        return {}, [FileSyntaxError(line_no=1, message="The file should contain an object.")]

    return contents_dict, []


FILE_PARSERS = {
    "env": _parse_env_file,
    "json": _parse_json_file,
    "yaml": _parse_yaml_file,
}


def parse_file(file_path: str) -> Dict[str, Any]:
    """
    Based on the file extension format, selects a parser, and parses the file.

    :param file_path: The location of the file that will be processed.
    :type file_path: str
    :return: Map of key (e.g. connection ID) and value.
    """
    if not os.path.exists(file_path):
        raise AirflowException(f"File {file_path} was not found.")

    log.debug("Parsing file: %s", file_path)

    ext = file_path.rsplit(".", 2)[-1].lower()

    if ext not in FILE_PARSERS:
        raise AirflowException(
            "Unsupported file format. The file must have the extension .env or .json or .yaml"
        )

    contents_dict, parse_errors = FILE_PARSERS[ext](file_path)

    log.debug(
        "Parsed file: len(parse_errors)=%d, len(contents_dict)=%d", len(parse_errors), len(contents_dict)
    )

    if parse_errors:
        raise AirflowFileParseException(
            "Failed to load the file.", file_path=file_path, parse_errors=parse_errors
        )

    return contents_dict
