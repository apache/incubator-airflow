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
"""Base task runner"""
import os
import subprocess
import threading
from tempfile import NamedTemporaryFile
from typing import Optional, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.taskinstance import load_error_file
from airflow.utils.configuration import tmp_configuration_copy
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser

PYTHONPATH_VAR = 'PYTHONPATH'


class BaseTaskRunner(LoggingMixin):
    """
    Runs Airflow task instances by invoking the `airflow tasks run` command with raw
    mode enabled in a subprocess.

    :param local_task_job: The local task job associated with running the
        associated task instance.
    :type local_task_job: airflow.jobs.local_task_job.LocalTaskJob
    """

    def __init__(self, local_task_job):
        # Pass task instance context into log handlers to setup the logger.
        super().__init__(local_task_job.task_instance)
        self._task_instance = local_task_job.task_instance

        popen_prepend = []
        if self._task_instance.run_as_user:
            self.run_as_user = self._task_instance.run_as_user
        else:
            try:
                self.run_as_user = conf.get('core', 'default_impersonation')
            except AirflowConfigException:
                self.run_as_user = None

        # Add sudo commands to change user if we need to. Needed to handle SubDagOperator
        # case using a SequentialExecutor.
        self.log.debug("Planning to run as the %s user", self.run_as_user)
        if self.run_as_user and (self.run_as_user != getuser()):
            # We want to include any environment variables now, as we won't
            # want to have to specify them in the sudo call - they would show
            # up in `ps` that way! And run commands now, as the other user
            # might not be able to run the cmds to get credentials
            cfg_path = tmp_configuration_copy(chmod=0o600)

            # Give ownership of file to user; only they can read and write
            subprocess.call(['sudo', 'chown', self.run_as_user, cfg_path], close_fds=True)

            # propagate PYTHONPATH environment variable
            pythonpath_value = os.environ.get(PYTHONPATH_VAR, '')
            popen_prepend = ['sudo', '-E', '-H', '-u', self.run_as_user]

            if pythonpath_value:
                popen_prepend.append(f'{PYTHONPATH_VAR}={pythonpath_value}')

        else:
            # Always provide a copy of the configuration file settings. Since
            # we are running as the same user, and can pass through environment
            # variables then we don't need to include those in the config copy
            # - the runner can read/execute those values as it needs
            cfg_path = tmp_configuration_copy(chmod=0o600)

        self._error_file = NamedTemporaryFile(delete=True)
        self._cfg_path = cfg_path
        self._command = (
            popen_prepend
            + self._task_instance.command_as_list(
                raw=True,
                pickle_id=local_task_job.pickle_id,
                mark_success=local_task_job.mark_success,
                job_id=local_task_job.id,
                pool=local_task_job.pool,
                cfg_path=cfg_path,
            )
            + ["--error-file", self._error_file.name]
        )
        self.process = None

    def deserialize_run_error(self) -> Optional[Union[str, Exception]]:
        """Return task runtime error if its written to provided error file."""
        return load_error_file(self._error_file)

    def _read_task_logs(self, stream):
        while True:
            line = stream.readline()
            if isinstance(line, bytes):
                line = line.decode('utf-8')
            if not line:
                break
            self.log.info(
                'Job %s: Subtask %s %s',
                self._task_instance.job_id,
                self._task_instance.task_id,
                line.rstrip('\n'),
            )

    def run_command(self, run_with=None):
        """
        Run the task command.

        :param run_with: list of tokens to run the task command with e.g. ``['bash', '-c']``
        :type run_with: list
        :return: the process that was run
        :rtype: subprocess.Popen
        """
        run_with = run_with or []
        full_cmd = run_with + self._command

        self.log.info("Running on host: %s", get_hostname())
        self.log.info('Running: %s', full_cmd)
        # pylint: disable=subprocess-popen-preexec-fn
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            close_fds=True,
            env=os.environ.copy(),
            preexec_fn=os.setsid,
        )

        # Start daemon thread to read subprocess logging output
        log_reader = threading.Thread(
            target=self._read_task_logs,
            args=(proc.stdout,),
        )
        log_reader.daemon = True
        log_reader.start()
        return proc

    def start(self):
        """Start running the task instance in a subprocess."""
        raise NotImplementedError()

    def return_code(self) -> Optional[int]:
        """
        :return: The return code associated with running the task instance or
            None if the task is not yet done.
        :rtype: int
        """
        raise NotImplementedError()

    def terminate(self) -> None:
        """Force kill the running task instance."""
        raise NotImplementedError()

    def on_finish(self) -> None:
        """A callback that should be called when this is done running."""
        if self._cfg_path and os.path.isfile(self._cfg_path):
            if self.run_as_user:
                subprocess.call(['sudo', 'rm', self._cfg_path], close_fds=True)
            else:
                os.remove(self._cfg_path)
        self._error_file.close()
