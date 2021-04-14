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
import signal
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir
from typing import Dict, List, Optional

from airflow.hooks.base import BaseHook

EXIT_CODE_SKIP = 127

SubprocessResult = namedtuple('SubprocessResult', ['exit_code', 'output'])


class SubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module"""

    def __init__(self) -> None:
        self.sub_process = None
        super().__init__()

    def run_command(
        self, command: List[str], env: Optional[Dict[str, str]] = None, output_encoding: str = 'utf-8'
    ) -> SubprocessResult:
        """
        Execute the command in a temporary directory which will be cleaned afterwards

        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
        :param output_encoding: encoding to use for decoding stdout
        :return: :class:`namedtuple` containing ``exit_code`` and ``output``, the last line from stderr
            or stdout
        """
        self.log.info('Tmp dir root location: \n %s', gettempdir())

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info('Running command: %s', command)

            self.sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=tmp_dir,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info('Output:')
            line = ''
            for raw_line in iter(self.sub_process.stdout.readline, b''):
                line = raw_line.decode(output_encoding).rstrip()
                self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info('Command exited with return code %s', self.sub_process.returncode)

        return SubprocessResult(exit_code=self.sub_process.returncode, output=line)

    def send_sigterm(self):
        """Sends SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info('Sending SIGTERM signal to process group')
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)
