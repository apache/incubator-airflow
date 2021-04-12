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

import datetime
import os
from glob import glob

from airflow.hooks.filesystem import FSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class FileSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in a filesystem.

    If the path given is a directory then this sensor will only return true if
    any files exist inside it (either directly, or within a subdirectory)

    :param fs_conn_id: reference to the File (path)
        connection id
    :type fs_conn_id: str
    :param filepath: File or folder name (relative to
        the base path set within the connection), can be a glob.
    :type filepath: str
    """

    template_fields = ('filepath',)
    ui_color = '#91818a'

    @apply_defaults
    def __init__(self, *, filepath, fs_conn_id='fs_default', **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id

    def poke(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.filepath)
        self.log.info('Poking for file %s', full_path)

        for path in glob(full_path):
            if os.path.isfile(path):
                mod_time = os.path.getmtime(path)
                mod_time = datetime.datetime.fromtimestamp(mod_time).strftime('%Y%m%d%H%M%S')
                self.log.info('Found File %s last modified: %s', str(path), str(mod_time))
                return True

            for _, _, files in os.walk(full_path):
                if len(files) > 0:
                    return True
        return False
