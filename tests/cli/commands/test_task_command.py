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
import io
import json
import logging
import os
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.cli import cli_parser
from airflow.cli.commands import task_command
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.cli import get_dag
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools, clear_db_runs

DEFAULT_DATE = timezone.make_aware(datetime(2016, 1, 1))
ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


def reset(dag_id):
    with create_session() as session:
        tis = session.query(TaskInstance).filter_by(dag_id=dag_id)
        tis.delete()
        runs = session.query(DagRun).filter_by(dag_id=dag_id)
        runs.delete()


# TODO: Check if tests needs side effects - locally there's missing DAG
class TestCliTasks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    def test_cli_list_tasks(self):
        for dag_id in self.dagbag.dags:
            args = self.parser.parse_args(['tasks', 'list', dag_id])
            task_command.task_list(args)

        args = self.parser.parse_args(['tasks', 'list', 'example_bash_operator', '--tree'])
        task_command.task_list(args)

    @mock.patch("airflow.models.taskinstance.TaskInstance._run_mini_scheduler_on_child_tasks")
    def test_test(self, mock_run_mini_scheduler):
        """Test the `airflow test` command"""
        args = self.parser.parse_args(
            ["tasks", "test", "example_python_operator", 'print_the_context', '2018-01-01']
        )

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(args)

        mock_run_mini_scheduler.assert_not_called()
        # Check that prints, and log messages, are shown
        assert "'example_python_operator__print_the_context__20180101'" in stdout.getvalue()

    @mock.patch("airflow.cli.commands.task_command.LocalTaskJob")
    def test_run_naive_taskinstance(self, mock_local_job):
        """
        Test that we can run naive (non-localized) task instances
        """
        naive_date = datetime(2016, 1, 1)
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')

        task0_id = 'test_run_dependent_task'
        args0 = [
            'tasks',
            'run',
            '--ignore-all-dependencies',
            '--local',
            dag_id,
            task0_id,
            naive_date.isoformat(),
        ]

        task_command.task_run(self.parser.parse_args(args0), dag=dag)
        mock_local_job.assert_called_once_with(
            task_instance=mock.ANY,
            mark_success=False,
            ignore_all_deps=True,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pickle_id=None,
            pool=None,
        )

    def test_cli_test(self):
        task_command.task_test(
            self.parser.parse_args(
                ['tasks', 'test', 'example_bash_operator', 'runme_0', DEFAULT_DATE.isoformat()]
            )
        )
        task_command.task_test(
            self.parser.parse_args(
                ['tasks', 'test', 'example_bash_operator', 'runme_0', '--dry-run', DEFAULT_DATE.isoformat()]
            )
        )

    def test_cli_test_with_params(self):
        task_command.task_test(
            self.parser.parse_args(
                [
                    'tasks',
                    'test',
                    'example_passing_params_via_test_command',
                    'run_this',
                    '--task-params',
                    '{"foo":"bar"}',
                    DEFAULT_DATE.isoformat(),
                ]
            )
        )
        task_command.task_test(
            self.parser.parse_args(
                [
                    'tasks',
                    'test',
                    'example_passing_params_via_test_command',
                    'also_run_this',
                    '--task-params',
                    '{"foo":"bar"}',
                    DEFAULT_DATE.isoformat(),
                ]
            )
        )

    def test_cli_test_with_env_vars(self):
        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(
                self.parser.parse_args(
                    [
                        'tasks',
                        'test',
                        'example_passing_params_via_test_command',
                        'env_var_test_task',
                        '--env-vars',
                        '{"foo":"bar"}',
                        DEFAULT_DATE.isoformat(),
                    ]
                )
            )
        output = stdout.getvalue()
        assert 'foo=bar' in output
        assert 'AIRFLOW_TEST_MODE=True' in output

    def test_cli_run(self):
        task_command.task_run(
            self.parser.parse_args(
                ['tasks', 'run', 'example_bash_operator', 'runme_0', '--local', DEFAULT_DATE.isoformat()]
            )
        )

    @parameterized.expand(
        [
            ("--ignore-all-dependencies",),
            ("--ignore-depends-on-past",),
            ("--ignore-dependencies",),
            ("--force",),
        ],
    )
    def test_cli_run_invalid_raw_option(self, option: str):
        with pytest.raises(
            AirflowException,
            match="Option --raw does not work with some of the other options on this command.",
        ):
            task_command.task_run(
                self.parser.parse_args(
                    [  # type: ignore
                        'tasks',
                        'run',
                        'example_bash_operator',
                        'runme_0',
                        DEFAULT_DATE.isoformat(),
                        '--raw',
                        option,
                    ]
                )
            )

    def test_cli_run_mutually_exclusive(self):
        with pytest.raises(AirflowException, match="Option --raw and --local are mutually exclusive."):
            task_command.task_run(
                self.parser.parse_args(
                    [
                        'tasks',
                        'run',
                        'example_bash_operator',
                        'runme_0',
                        DEFAULT_DATE.isoformat(),
                        '--raw',
                        '--local',
                    ]
                )
            )

    def test_task_state(self):
        task_command.task_state(
            self.parser.parse_args(
                ['tasks', 'state', 'example_bash_operator', 'runme_0', DEFAULT_DATE.isoformat()]
            )
        )

    def test_task_states_for_dag_run(self):

        dag2 = DagBag().dags['example_python_operator']
        task2 = dag2.get_task(task_id='print_the_context')
        default_date2 = timezone.make_aware(datetime(2016, 1, 9))
        dag2.clear()

        ti2 = TaskInstance(task2, default_date2)

        ti2.set_state(State.SUCCESS)
        ti_start = ti2.start_date
        ti_end = ti2.end_date

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_states_for_dag_run(
                self.parser.parse_args(
                    [
                        'tasks',
                        'states-for-dag-run',
                        'example_python_operator',
                        default_date2.isoformat(),
                        '--output',
                        "json",
                    ]
                )
            )
        actual_out = json.loads(stdout.getvalue())

        assert len(actual_out) == 1
        assert actual_out[0] == {
            'dag_id': 'example_python_operator',
            'execution_date': '2016-01-09T00:00:00+00:00',
            'task_id': 'print_the_context',
            'state': 'success',
            'start_date': ti_start.isoformat(),
            'end_date': ti_end.isoformat(),
        }

    def test_subdag_clear(self):
        args = self.parser.parse_args(['tasks', 'clear', 'example_subdag_operator', '--yes'])
        task_command.task_clear(args)
        args = self.parser.parse_args(
            ['tasks', 'clear', 'example_subdag_operator', '--yes', '--exclude-subdags']
        )
        task_command.task_clear(args)

    def test_parentdag_downstream_clear(self):
        args = self.parser.parse_args(['tasks', 'clear', 'example_subdag_operator.section-1', '--yes'])
        task_command.task_clear(args)
        args = self.parser.parse_args(
            ['tasks', 'clear', 'example_subdag_operator.section-1', '--yes', '--exclude-parentdag']
        )
        task_command.task_clear(args)

    @pytest.mark.quarantined
    def test_local_run(self):
        args = self.parser.parse_args(
            [
                'tasks',
                'run',
                'example_python_operator',
                'print_the_context',
                '2018-04-27T08:39:51.298439+00:00',
                '--interactive',
                '--subdir',
                '/root/dags/example_python_operator.py',
            ]
        )

        dag = get_dag(args.subdir, args.dag_id)
        reset(dag.dag_id)

        task_command.task_run(args)
        task = dag.get_task(task_id=args.task_id)
        ti = TaskInstance(task, args.execution_date)
        ti.refresh_from_db()
        state = ti.current_state()
        assert state == State.SUCCESS


# For this test memory spins out of control on Python 3.6. TODO(potiuk): FIXME")
@pytest.mark.quarantined
class TestLogsfromTaskRunCommand(unittest.TestCase):
    def setUp(self) -> None:
        self.dag_id = "test_logging_dag"
        self.task_id = "test_task"
        self.dag_path = os.path.join(ROOT_FOLDER, "dags", "test_logging_in_dag.py")
        reset(self.dag_id)
        self.execution_date = timezone.make_aware(datetime(2017, 1, 1))
        self.execution_date_str = self.execution_date.isoformat()
        self.task_args = ['tasks', 'run', self.dag_id, self.task_id, '--local', self.execution_date_str]
        self.log_dir = conf.get('logging', 'base_log_folder')
        self.log_filename = f"{self.dag_id}/{self.task_id}/{self.execution_date_str}/1.log"
        self.ti_log_file_path = os.path.join(self.log_dir, self.log_filename)
        self.parser = cli_parser.get_parser()

        root = self.root_logger = logging.getLogger()
        self.root_handlers = root.handlers.copy()
        self.root_filters = root.filters.copy()
        self.root_level = root.level

        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def tearDown(self) -> None:
        root = self.root_logger
        root.setLevel(self.root_level)
        root.handlers[:] = self.root_handlers
        root.filters[:] = self.root_filters

        reset(self.dag_id)
        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def assert_log_line(self, text, logs_list, expect_from_logging_mixin=False):
        """
        Get Log Line and assert only 1 Entry exists with the given text. Also check that
        "logging_mixin" line does not appear in that log line to avoid duplicate logging as below:

        [2020-06-24 16:47:23,537] {logging_mixin.py:91} INFO - [2020-06-24 16:47:23,536] {python.py:135}
        """
        log_lines = [log for log in logs_list if text in log]
        assert len(log_lines) == 1
        log_line = log_lines[0]
        if not expect_from_logging_mixin:
            # Logs from print statement still show with logging_mixing as filename
            # Example: [2020-06-24 17:07:00,482] {logging_mixin.py:91} INFO - Log from Print statement
            assert "logging_mixin.py" not in log_line
        return log_line

    @unittest.skipIf(not hasattr(os, 'fork'), "Forking not available")
    def test_logging_with_run_task(self):
        #  We are not using self.assertLogs as we want to verify what actually is stored in the Log file
        # as that is what gets displayed

        with conf_vars({('core', 'dags_folder'): self.dag_path}):
            task_command.task_run(self.parser.parse_args(self.task_args))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)  # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        assert "INFO - Started process" in logs
        assert f"Subtask {self.task_id}" in logs
        assert "standard_task_runner.py" in logs
        assert (
            f"INFO - Running: ['airflow', 'tasks', 'run', '{self.dag_id}', "
            f"'{self.task_id}', '{self.execution_date_str}'," in logs
        )

        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        assert (
            f"INFO - Marking task as SUCCESS. dag_id={self.dag_id}, "
            f"task_id={self.task_id}, execution_date=20170101T000000" in logs
        )

    @mock.patch("airflow.task.task_runner.standard_task_runner.CAN_FORK", False)
    def test_logging_with_run_task_subprocess(self):
        # We are not using self.assertLogs as we want to verify what actually is stored in the Log file
        # as that is what gets displayed
        with conf_vars({('core', 'dags_folder'): self.dag_path}):
            task_command.task_run(self.parser.parse_args(self.task_args))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)  # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        assert f"Subtask {self.task_id}" in logs
        assert "base_task_runner.py" in logs
        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        assert (
            f"INFO - Running: ['airflow', 'tasks', 'run', '{self.dag_id}', "
            f"'{self.task_id}', '{self.execution_date_str}'," in logs
        )
        assert (
            f"INFO - Marking task as SUCCESS. dag_id={self.dag_id}, "
            f"task_id={self.task_id}, execution_date=20170101T000000" in logs
        )

    def test_log_file_template_with_run_task(self):
        """Verify that the taskinstance has the right context for log_filename_template"""

        with mock.patch.object(task_command, "_run_task_by_selected_method"):
            with conf_vars({('core', 'dags_folder'): self.dag_path}):
                # increment the try_number of the task to be run
                dag = DagBag().get_dag(self.dag_id)
                task = dag.get_task(self.task_id)
                with create_session() as session:
                    dag.create_dagrun(
                        execution_date=self.execution_date,
                        start_date=timezone.utcnow(),
                        state=State.RUNNING,
                        run_type=DagRunType.MANUAL,
                        session=session,
                    )
                    ti = TaskInstance(task, self.execution_date)
                    ti.refresh_from_db(session=session, lock_for_update=True)
                    ti.try_number = 1  # not running, so starts at 0
                    session.merge(ti)

                log_file_path = os.path.join(os.path.dirname(self.ti_log_file_path), "2.log")

                try:
                    task_command.task_run(self.parser.parse_args(self.task_args))

                    assert os.path.exists(log_file_path)
                finally:
                    try:
                        os.remove(log_file_path)
                    except OSError:
                        pass

    @mock.patch.object(task_command, "_run_task_by_selected_method")
    def test_root_logger_restored(self, run_task_mock):
        """Verify that the root logging context is restored"""

        logger = logging.getLogger("foo.bar")

        def task_inner(*args, **kwargs):
            logger.warning("redirected log message")

        run_task_mock.side_effect = task_inner

        config = {
            ('core', 'dags_folder'): self.dag_path,
            ('logging', 'logging_level'): "INFO",
        }

        with conf_vars(config):
            with self.assertLogs(level=logging.WARNING) as captured:
                logger.warning("not redirected")
                task_command.task_run(self.parser.parse_args(self.task_args))

                assert captured.output == ["WARNING:foo.bar:not redirected"]
                assert self.root_logger.level == logging.WARNING

        assert self.root_logger.handlers == self.root_handlers

    @mock.patch.object(task_command, "_run_task_by_selected_method")
    def test_disable_handler_modifying(self, run_task_mock):
        """If [core] donot_modify_handlers is set to True, the root logger is untouched"""
        from airflow import settings

        logger = logging.getLogger("foo.bar")

        def task_inner(*args, **kwargs):
            logger.warning("not redirected")

        run_task_mock.side_effect = task_inner

        config = {
            ('core', 'dags_folder'): self.dag_path,
            ('logging', 'logging_level'): "INFO",
        }
        old_value = settings.DONOT_MODIFY_HANDLERS
        settings.DONOT_MODIFY_HANDLERS = True

        with conf_vars(config):
            with self.assertLogs(level=logging.WARNING) as captured:
                task_command.task_run(self.parser.parse_args(self.task_args))

                assert captured.output == ["WARNING:foo.bar:not redirected"]

        settings.DONOT_MODIFY_HANDLERS = old_value


class TestCliTaskBackfill(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)

    def setUp(self):
        clear_db_runs()
        clear_db_pools()

        self.parser = cli_parser.get_parser()

    def test_run_ignores_all_dependencies(self):
        """
        Test that run respects ignore_all_dependencies
        """
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')
        dag.clear()

        task0_id = 'test_run_dependent_task'
        args0 = ['tasks', 'run', '--ignore-all-dependencies', dag_id, task0_id, DEFAULT_DATE.isoformat()]
        task_command.task_run(self.parser.parse_args(args0))
        ti_dependent0 = TaskInstance(task=dag.get_task(task0_id), execution_date=DEFAULT_DATE)

        ti_dependent0.refresh_from_db()
        assert ti_dependent0.state == State.FAILED

        task1_id = 'test_run_dependency_task'
        args1 = [
            'tasks',
            'run',
            '--ignore-all-dependencies',
            dag_id,
            task1_id,
            (DEFAULT_DATE + timedelta(days=1)).isoformat(),
        ]
        task_command.task_run(self.parser.parse_args(args1))

        ti_dependency = TaskInstance(
            task=dag.get_task(task1_id), execution_date=DEFAULT_DATE + timedelta(days=1)
        )
        ti_dependency.refresh_from_db()
        assert ti_dependency.state == State.FAILED

        task2_id = 'test_run_dependent_task'
        args2 = [
            'tasks',
            'run',
            '--ignore-all-dependencies',
            dag_id,
            task2_id,
            (DEFAULT_DATE + timedelta(days=1)).isoformat(),
        ]
        task_command.task_run(self.parser.parse_args(args2))

        ti_dependent = TaskInstance(
            task=dag.get_task(task2_id), execution_date=DEFAULT_DATE + timedelta(days=1)
        )
        ti_dependent.refresh_from_db()
        assert ti_dependent.state == State.SUCCESS
