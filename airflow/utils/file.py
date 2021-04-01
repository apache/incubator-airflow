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
import io
import logging
import os
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Pattern, Union

from airflow.configuration import conf

if TYPE_CHECKING:
    import pathlib

log = logging.getLogger(__name__)


def TemporaryDirectory(*args, **kwargs):  # pylint: disable=invalid-name
    """This function is deprecated. Please use `tempfile.TemporaryDirectory`"""
    import warnings
    from tempfile import TemporaryDirectory as TmpDir

    warnings.warn(
        "This function is deprecated. Please use `tempfile.TemporaryDirectory`",
        DeprecationWarning,
        stacklevel=2,
    )
    return TmpDir(*args, **kwargs)


def mkdirs(path, mode):
    """
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.

    :param path: The directory to create
    :type path: str
    :param mode: The mode to give to the directory e.g. 0o755, ignores umask
    :type mode: int
    """
    import warnings

    warnings.warn(
        f"This function is deprecated. Please use `pathlib.Path({path}).mkdir`",
        DeprecationWarning,
        stacklevel=2,
    )
    Path(path).mkdir(mode=mode, parents=True, exist_ok=True)


ZIP_REGEX = re.compile(r'((.*\.zip){})?(.*)'.format(re.escape(os.sep)))


def correct_maybe_zipped(fileloc):
    """
    If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive and path to zip is returned.
    """
    _, archive, _ = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return archive
    else:
        return fileloc


def open_maybe_zipped(fileloc, mode='r'):
    """
    Opens the given file. If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive, opening the file inside the archive.

    :return: a file object, as in `open`, or as in `ZipFile.open`.
    """
    _, archive, filename = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return io.TextIOWrapper(zipfile.ZipFile(archive, mode=mode).open(filename))
    else:
        return open(fileloc, mode=mode)


def find_path_from_directory(base_dir_path: str, ignore_file_name: str) -> Generator[str, None, None]:
    """
    Search the file and return the path of the file that should not be ignored.
    :param base_dir_path: the base path to be searched for.
    :param ignore_file_name: the file name in which specifies a regular expression pattern is written.

    :return : file path not to be ignored.
    """
    patterns_by_dir: Dict[str, List[Pattern[str]]] = {}

    for root, dirs, files in os.walk(str(base_dir_path), followlinks=True):
        patterns: List[Pattern[str]] = patterns_by_dir.get(root, [])

        ignore_file_path = os.path.join(root, ignore_file_name)
        if os.path.isfile(ignore_file_path):
            with open(ignore_file_path) as file:
                lines_no_comments = [re.sub(r"\s*#.*", "", line) for line in file.read().split("\n")]
                patterns += [re.compile(line) for line in lines_no_comments if line]
                patterns = list(set(patterns))

        dirs[:] = [
            subdir
            for subdir in dirs
            if not any(
                p.search(os.path.join(os.path.relpath(root, str(base_dir_path)), subdir)) for p in patterns
            )
        ]

        patterns_by_dir.update({os.path.join(root, sd): patterns.copy() for sd in dirs})

        for file in files:  # type: ignore
            if file == ignore_file_name:
                continue
            abs_file_path = os.path.join(root, str(file))
            rel_file_path = os.path.join(os.path.relpath(root, str(base_dir_path)), str(file))
            if any(p.search(rel_file_path) for p in patterns):
                continue
            yield str(abs_file_path)


def list_py_file_paths(
    directory: Union[str, "pathlib.Path"],
    safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE', fallback=True),
    include_examples: Optional[bool] = None,
    include_smart_sensor: Optional[bool] = conf.getboolean('smart_sensor', 'use_smart_sensor'),
):
    """
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :type directory: unicode
    :param safe_mode: whether to use a heuristic to determine whether a file
        contains Airflow DAG definitions. If not provided, use the
        core.DAG_DISCOVERY_SAFE_MODE configuration setting. If not set, default
        to safe.
    :type safe_mode: bool
    :param include_examples: include example DAGs
    :type include_examples: bool
    :param include_smart_sensor: include smart sensor native control DAGs
    :type include_examples: bool
    :return: a list of paths to Python files in the specified directory
    :rtype: list[unicode]
    """
    if include_examples is None:
        include_examples = conf.getboolean('core', 'LOAD_EXAMPLES')
    file_paths: List[str] = []
    if directory is None:
        file_paths = []
    elif os.path.isfile(directory):
        file_paths = [str(directory)]
    elif os.path.isdir(directory):
        file_paths.extend(find_dag_file_paths(directory, safe_mode))
    if include_examples:
        from airflow import example_dags

        example_dag_folder = example_dags.__path__[0]  # type: ignore
        file_paths.extend(list_py_file_paths(example_dag_folder, safe_mode, False, False))
    if include_smart_sensor:
        from airflow import smart_sensor_dags

        smart_sensor_dag_folder = smart_sensor_dags.__path__[0]  # type: ignore
        file_paths.extend(list_py_file_paths(smart_sensor_dag_folder, safe_mode, False, False))
    return file_paths


def find_dag_file_paths(directory: Union[str, "pathlib.Path"], safe_mode: bool) -> List[str]:
    """Finds file paths of all DAG files."""
    file_paths = []

    for file_path in find_path_from_directory(str(directory), ".airflowignore"):
        try:
            if not os.path.isfile(file_path):
                continue
            _, file_ext = os.path.splitext(os.path.split(file_path)[-1])
            if file_ext != '.py' and not zipfile.is_zipfile(file_path):
                continue
            if not might_contain_dag(file_path, safe_mode):
                continue

            file_paths.append(file_path)
        except Exception:  # noqa pylint: disable=broad-except
            log.exception("Error while examining %s", file_path)

    return file_paths


COMMENT_PATTERN = re.compile(r"\s*#.*")


def might_contain_dag(file_path: str, safe_mode: bool, zip_file: Optional[zipfile.ZipFile] = None):
    """
    Heuristic that guesses whether a Python file contains an Airflow DAG definition.

    :param file_path: Path to the file to be checked.
    :param safe_mode: Is safe mode active?. If no, this function always returns True.
    :param zip_file: if passed, checks the archive. Otherwise, check local filesystem.
    :return: True, if file might contain DAGS.
    """
    if not safe_mode:
        return True
    if zip_file:
        with zip_file.open(file_path) as current_file:
            content = current_file.read()
    else:
        if zipfile.is_zipfile(file_path):
            return True
        with open(file_path, 'rb') as dag_file:
            content = dag_file.read()
    content = content.lower()
    return all(s in content for s in (b'dag', b'airflow'))
