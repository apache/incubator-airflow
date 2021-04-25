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
import logging
import re
import sys
from typing import Any, Dict, List, Optional, Pattern, Type

from airflow import settings
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HdfsSensor`
    """

    template_fields = ('filepath',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @apply_defaults
    def __init__(
        self,
        *,
        filepath: str,
        hdfs_conn_id: str = 'hdfs_default',
        ignored_ext: Optional[List[str]] = None,
        ignore_copying: bool = True,
        file_size: Optional[int] = None,
        hook: Type[HDFSHook] = HDFSHook,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if ignored_ext is None:
            ignored_ext = ['_COPYING_']
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id
        self.file_size = file_size
        self.ignored_ext = ignored_ext
        self.ignore_copying = ignore_copying
        self.hook = hook

    @staticmethod
    def filter_for_filesize(result: List[Dict[Any, Any]], size: Optional[int] = None) -> List[Dict[Any, Any]]:
        """
        Will test the filepath result and test if its size is at least self.filesize

        :param result: a list of dicts returned by Snakebite ls
        :param size: the file size in MB a file should be at least to trigger True
        :return: (bool) depending on the matching criteria
        """
        if size:
            log.debug('Filtering for file size >= %s in files: %s', size, map(lambda x: x['path'], result))
            size *= settings.MEGABYTE
            result = [x for x in result if x['length'] >= size]
            log.debug('HdfsSensor.poke: after size filter result is %s', result)
        return result

    @staticmethod
    def filter_for_ignored_ext(
        result: List[Dict[Any, Any]], ignored_ext: List[str], ignore_copying: bool
    ) -> List[Dict[Any, Any]]:
        """
        Will filter if instructed to do so the result to remove matching criteria

        :param result: list of dicts returned by Snakebite ls
        :type result: list[dict]
        :param ignored_ext: list of ignored extensions
        :type ignored_ext: list
        :param ignore_copying: shall we ignore ?
        :type ignore_copying: bool
        :return: list of dicts which were not removed
        :rtype: list[dict]
        """
        if ignore_copying:
            regex_builder = r"^.*\.(%s$)$" % '$|'.join(ignored_ext)
            ignored_extensions_regex = re.compile(regex_builder)
            log.debug(
                'Filtering result for ignored extensions: %s in files %s',
                ignored_extensions_regex.pattern,
                map(lambda x: x['path'], result),
            )
            result = [x for x in result if not ignored_extensions_regex.match(x['path'])]
            log.debug('HdfsSensor.poke: after ext filter result is %s', result)
        return result

    def poke(self, context: Dict[Any, Any]) -> bool:
        """Get a snakebite client connection and check for file."""
        sb_client = self.hook(self.hdfs_conn_id).get_conn()
        self.log.info('Poking for file %s', self.filepath)
        try:
            # IMOO it's not right here, as there is no raise of any kind.
            # if the filepath is let's say '/data/mydirectory',
            # it's correct but if it is '/data/mydirectory/*',
            # it's not correct as the directory exists and sb_client does not raise any error
            # here is a quick fix
            result = sb_client.ls([self.filepath], include_toplevel=False)
            self.log.debug('HdfsSensor.poke: result is %s', result)
            result = self.filter_for_ignored_ext(result, self.ignored_ext, self.ignore_copying)
            result = self.filter_for_filesize(result, self.file_size)
            return bool(result)
        except Exception:  # pylint: disable=broad-except
            e = sys.exc_info()
            self.log.debug("Caught an exception !: %s", str(e))
            return False


class HdfsRegexSensor(HdfsSensor):
    """
    Waits for matching files by matching on regex

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HdfsRegexSensor`
    """

    def __init__(self, regex: Pattern[str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.regex = regex

    def poke(self, context: Dict[Any, Any]) -> bool:
        """
        Poke matching files in a directory with self.regex

        :return: Bool depending on the search criteria
        """
        sb_client = self.hook(self.hdfs_conn_id).get_conn()
        self.log.info(
            'Poking for %s to be a directory with files matching %s', self.filepath, self.regex.pattern
        )
        result = [
            f
            for f in sb_client.ls([self.filepath], include_toplevel=False)
            if f['file_type'] == 'f' and self.regex.match(f['path'].replace(f'{self.filepath}/', ''))
        ]
        result = self.filter_for_ignored_ext(result, self.ignored_ext, self.ignore_copying)
        result = self.filter_for_filesize(result, self.file_size)
        return bool(result)


class HdfsFolderSensor(HdfsSensor):
    """
    Waits for a non-empty directory

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HdfsFolderSensor`
    """

    def __init__(self, be_empty: bool = False, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.be_empty = be_empty

    def poke(self, context: Dict[str, Any]) -> bool:
        """
        Poke for a non empty directory

        :return: Bool depending on the search criteria
        """
        sb_client = self.hook(self.hdfs_conn_id).get_conn()
        result = sb_client.ls([self.filepath], include_toplevel=True)
        result = self.filter_for_ignored_ext(result, self.ignored_ext, self.ignore_copying)
        result = self.filter_for_filesize(result, self.file_size)
        if self.be_empty:
            self.log.info('Poking for filepath %s to a empty directory', self.filepath)
            return len(result) == 1 and result[0]['path'] == self.filepath
        else:
            self.log.info('Poking for filepath %s to a non empty directory', self.filepath)
            result.pop(0)
            return bool(result) and result[0]['file_type'] == 'f'
