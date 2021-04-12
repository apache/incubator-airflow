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
import os
import time
from logging.config import dictConfig
from unittest import mock

import psutil
import pytest

from airflow import models, settings
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.models import TaskInstance as TI
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.platform import getuser
from airflow.utils.state import State
from tests.test_utils.db import clear_db_runs

TEST_DAG_FOLDER = os.environ['AIRFLOW__CORE__DAGS_FOLDER']

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {'format': '[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s'},
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout',
        },
    },
    'loggers': {'airflow': {'handlers': ['console'], 'level': 'INFO', 'propagate': True}},
}


class TestStandardTaskRunner:
    @pytest.fixture(autouse=True, scope="class")
    def logging_and_db(self):
        """
        This fixture sets up logging to have a different setup on the way in
        (as the test environment does not have enough context for the normal
        way to run) and ensures they reset back to normal on the way out.
        """
        dictConfig(LOGGING_CONFIG)
        yield
        airflow_logger = logging.getLogger('airflow')
        airflow_logger.handlers = []
        try:
            clear_db_runs()
        except Exception:  # noqa pylint: disable=broad-except
            # It might happen that we lost connection to the server here so we need to ignore any errors here
            pass

    def test_start_and_terminate(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = None
        local_task_job.task_instance.command_as_list.return_value = [
            'airflow',
            'tasks',
            'test',
            'test_on_kill',
            'task1',
            '2016-01-01',
        ]

        runner = StandardTaskRunner(local_task_job)
        runner.start()
        time.sleep(0.5)

        pgid = os.getpgid(runner.process.pid)
        assert pgid > 0
        assert pgid != os.getpgid(0), "Task should be in a different process group to us"

        processes = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

        assert runner.return_code() is not None

    def test_start_and_terminate_run_as_user(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = getuser()
        local_task_job.task_instance.command_as_list.return_value = [
            'airflow',
            'tasks',
            'test',
            'test_on_kill',
            'task1',
            '2016-01-01',
        ]

        runner = StandardTaskRunner(local_task_job)

        runner.start()
        time.sleep(0.5)

        pgid = os.getpgid(runner.process.pid)
        assert pgid > 0
        assert pgid != os.getpgid(0), "Task should be in a different process group to us"

        processes = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

        assert runner.return_code() is not None

    def test_early_reap_exit(self, caplog):
        """
        Tests that when a child process running a task is killed externally
        (e.g. by an OOM error, which we fake here), then we get return code
        -9 and a log message.
        """
        # Set up mock task
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = getuser()
        local_task_job.task_instance.command_as_list.return_value = [
            'airflow',
            'tasks',
            'test',
            'test_on_kill',
            'task1',
            '2016-01-01',
        ]

        # Kick off the runner
        runner = StandardTaskRunner(local_task_job)
        runner.start()
        time.sleep(0.2)

        # Kill the child process externally from the runner
        # Note that we have to do this from ANOTHER process, as if we just
        # call os.kill here we're doing it from the parent process and it
        # won't be the same as an external kill in terms of OS tracking.
        pgid = os.getpgid(runner.process.pid)
        os.system(f"kill -s KILL {pgid}")
        time.sleep(0.2)

        runner.terminate()

        assert runner.return_code() == -9
        assert "running out of memory" in caplog.text

    def test_on_kill(self):
        """
        Test that ensures that clearing in the UI SIGTERMS
        the task
        """
        path = "/tmp/airflow_on_kill"
        try:
            os.unlink(path)
        except OSError:
            pass

        dagbag = models.DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_on_kill')
        task = dag.get_task('task1')

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(
            run_id="test",
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = TI(task=task, execution_date=DEFAULT_DATE)
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        session.commit()

        runner = StandardTaskRunner(job1)
        runner.start()

        # give the task some time to startup
        time.sleep(3)

        pgid = os.getpgid(runner.process.pid)
        assert pgid > 0
        assert pgid != os.getpgid(0), "Task should be in a different process group to us"

        processes = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        # Wait some time for the result
        for _ in range(20):
            if os.path.exists(path):
                break
            time.sleep(2)

        with open(path) as f:
            assert "ON_KILL_TEST" == f.readline()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

    @staticmethod
    def _procs_in_pgroup(pgid):
        for proc in psutil.process_iter(attrs=['pid', 'name']):
            try:
                if os.getpgid(proc.pid) == pgid and proc.pid != 0:
                    yield proc
            except OSError:
                pass
