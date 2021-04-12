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
import ftplib
import re

from airflow.providers.ftp.hooks.ftp import FTPHook, FTPSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class FTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on FTP.

    :param path: Remote file or directory path
    :type path: str
    :param fail_on_transient_errors: Fail on all errors,
        including 4xx transient errors. Default True.
    :type fail_on_transient_errors: bool
    :param ftp_conn_id: The connection to run the sensor against
    :type ftp_conn_id: str
    """

    template_fields = ('path',)

    """Errors that are transient in nature, and where action can be retried"""
    transient_errors = [421, 425, 426, 434, 450, 451, 452]

    error_code_pattern = re.compile(r"([\d]+)")

    @apply_defaults
    def __init__(
        self, *, path: str, ftp_conn_id: str = 'ftp_default', fail_on_transient_errors: bool = True, **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.path = path
        self.ftp_conn_id = ftp_conn_id
        self.fail_on_transient_errors = fail_on_transient_errors

    def _create_hook(self) -> FTPHook:
        """Return connection hook."""
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    def _get_error_code(self, e):
        """Extract error code from ftp exception"""
        try:
            matches = self.error_code_pattern.match(str(e))
            code = int(matches.group(0))
            return code
        except ValueError:
            return e

    def poke(self, context: dict) -> bool:
        with self._create_hook() as hook:
            self.log.info('Poking for %s', self.path)
            try:
                mod_time = hook.get_mod_time(self.path)
                self.log.info('Found File %s last modified: %s', str(self.path), str(mod_time))

            except ftplib.error_perm as e:
                self.log.error('Ftp error encountered: %s', str(e))
                error_code = self._get_error_code(e)
                if (error_code != 550) and (
                    self.fail_on_transient_errors or (error_code not in self.transient_errors)
                ):
                    raise e

                return False

            return True


class FTPSSensor(FTPSensor):
    """Waits for a file or directory to be present on FTP over SSL."""

    def _create_hook(self) -> FTPHook:
        """Return connection hook."""
        return FTPSHook(ftp_conn_id=self.ftp_conn_id)
