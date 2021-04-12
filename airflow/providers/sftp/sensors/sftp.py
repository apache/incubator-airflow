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
"""This module contains SFTP sensor."""
from typing import Optional

from paramiko import SFTP_NO_SUCH_FILE

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SFTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote file or directory path
    :type path: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """

    template_fields = ('path',)

    @apply_defaults
    def __init__(self, *, path: str, sftp_conn_id: str = 'sftp_default', **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.hook: Optional[SFTPHook] = None
        self.sftp_conn_id = sftp_conn_id

    def poke(self, context: dict) -> bool:
        self.hook = SFTPHook(self.sftp_conn_id)
        self.log.info('Poking for %s', self.path)
        try:
            mod_time = self.hook.get_mod_time(self.path)
            self.log.info('Found File %s last modified: %s', str(self.path), str(mod_time))
        except OSError as e:
            if e.errno != SFTP_NO_SUCH_FILE:
                raise e
            return False
        self.hook.close_conn()
        return True
