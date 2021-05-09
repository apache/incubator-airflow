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
"""Standard task runner"""
import logging
import os
from typing import Optional

import psutil
from setproctitle import setproctitle  # pylint: disable=no-name-in-module

from airflow.settings import CAN_FORK
from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.process_utils import reap_process_group


class StandardTaskRunner(BaseTaskRunner):
    """Standard runner for all tasks."""

    def __init__(self, local_task_job):
        super().__init__(local_task_job)
        self._rc = None
        self.dag = local_task_job.task_instance.task.dag

    def start(self):
        if CAN_FORK and not self.run_as_user:
            self.process = self._start_by_fork()
        else:
            self.process = self._start_by_exec()

    def _start_by_exec(self):
        subprocess = self.run_command()
        return psutil.Process(subprocess.pid)

    def _start_by_fork(self):  # pylint: disable=inconsistent-return-statements
        rpipe, wpipe = os.pipe()
        pid = os.fork()
        if pid:
            self.log.info("Started process %d to run task", pid)
            os.close(wpipe)
            self.read_task_logs(os.fdopen(rpipe))
            return psutil.Process(pid)
        else:
            import signal
            import sys

            from airflow import settings
            from airflow.cli.cli_parser import get_parser
            from airflow.sentry import Sentry

            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            # Start a new process group
            os.setpgid(0, 0)

            # Force a new SQLAlchemy session. We can't share open DB handles
            # between process. The cli code will re-create this as part of its
            # normal startup
            settings.engine.pool.dispose()
            settings.engine.dispose()

            parser = get_parser()
            # [1:] - remove "airflow" from the start of the command
            args = parser.parse_args(self._command[1:])

            self.log.info('Running: %s', self._command)
            self.log.info('Job %s: Subtask %s', self._task_instance.job_id, self._task_instance.task_id)

            proc_title = "airflow task runner: {0.dag_id} {0.task_id} {0.execution_date}"
            if hasattr(args, "job_id"):
                proc_title += " {0.job_id}"
            setproctitle(proc_title.format(args))

            try:
                os.close(rpipe)
                # replace fds for the stdout/stderr with the newly created pipe
                os.dup2(wpipe, 1)
                os.dup2(wpipe, 2)
                os.close(wpipe)
                args.func(args, dag=self.dag)
                return_code = 0
            except Exception:  # pylint: disable=broad-except
                return_code = 1
            finally:
                # Explicitly flush any pending exception to Sentry if enabled
                Sentry.flush()
                sys.stdout.flush()
                sys.stderr.flush()
                logging.shutdown()
                os._exit(return_code)  # pylint: disable=protected-access

    def return_code(self, timeout: int = 0) -> Optional[int]:
        # We call this multiple times, but we can only wait on the process once
        if self._rc is not None or not self.process:
            return self._rc

        try:
            self._rc = self.process.wait(timeout=timeout)
            self.process = None
        except psutil.TimeoutExpired:
            pass

        return self._rc

    def terminate(self):
        if self.process is None:
            return

        # Reap the child process - it may already be finished
        _ = self.return_code(timeout=0)

        if self.process and self.process.is_running():
            rcs = reap_process_group(self.process.pid, self.log)
            self._rc = rcs.get(self.process.pid)

        self.process = None

        if self._rc is None:
            # Something else reaped it before we had a chance, so let's just "guess" at an error code.
            self._rc = -9

        if self._rc == -9:
            # If either we or psutil gives out a -9 return code, it likely means
            # an OOM happened
            self.log.error(
                'Job %s was killed before it finished (likely due to running out of memory)',
                self._task_instance.job_id,
            )
