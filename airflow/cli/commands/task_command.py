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
"""Task sub-commands"""
import importlib
import json
import logging
import os
import textwrap
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from typing import List

from airflow import settings
from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.models import DagPickle, TaskInstance
from airflow.models.dag import DAG
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.utils import cli as cli_utils
from airflow.utils.cli import (
    get_dag,
    get_dag_by_file_location,
    get_dag_by_pickle,
    get_dags,
    suppress_logs_and_warning,
)
from airflow.utils.log.logging_mixin import StreamLogWriter
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session


def _run_task_by_selected_method(args, dag: DAG, ti: TaskInstance) -> None:
    """
    Runs the task in one of 3 modes

    - using LocalTaskJob
    - as raw task
    - by executor
    """
    if args.local and args.raw:
        raise AirflowException(
            "Option --raw and --local are mutually exclusive. "
            "Please remove one option to execute the command."
        )
    if args.local:
        _run_task_by_local_task_job(args, ti)
    elif args.raw:
        _run_raw_task(args, ti)
    else:
        _run_task_by_executor(args, dag, ti)


def _run_task_by_executor(args, dag, ti):
    """
    Sends the task to the executor for execution. This can result in the task being started by another host
    if the executor implementation does
    """
    pickle_id = None
    if args.ship_dag:
        try:
            # Running remotely, so pickling the DAG
            with create_session() as session:
                pickle = DagPickle(dag)
                session.add(pickle)
                pickle_id = pickle.id
                # TODO: This should be written to a log
                print(f'Pickled dag {dag} as pickle_id: {pickle_id}')
        except Exception as e:
            print('Could not pickle the DAG')
            print(e)
            raise e
    executor = ExecutorLoader.get_default_executor()
    executor.start()
    print("Sending to executor.")
    executor.queue_task_instance(
        ti,
        mark_success=args.mark_success,
        pickle_id=pickle_id,
        ignore_all_deps=args.ignore_all_dependencies,
        ignore_depends_on_past=args.ignore_depends_on_past,
        ignore_task_deps=args.ignore_dependencies,
        ignore_ti_state=args.force,
        pool=args.pool,
    )
    executor.heartbeat()
    executor.end()


def _run_task_by_local_task_job(args, ti):
    """Run LocalTaskJob, which monitors the raw task execution process"""
    run_job = LocalTaskJob(
        task_instance=ti,
        mark_success=args.mark_success,
        pickle_id=args.pickle,
        ignore_all_deps=args.ignore_all_dependencies,
        ignore_depends_on_past=args.ignore_depends_on_past,
        ignore_task_deps=args.ignore_dependencies,
        ignore_ti_state=args.force,
        pool=args.pool,
    )
    try:
        run_job.run()

    finally:
        if args.shut_down_logging:
            logging.shutdown()


RAW_TASK_UNSUPPORTED_OPTION = [
    "ignore_all_dependencies",
    "ignore_depends_on_past",
    "ignore_dependencies",
    "force",
]


def _run_raw_task(args, ti: TaskInstance) -> None:
    """Runs the main task handling code"""
    unsupported_options = [o for o in RAW_TASK_UNSUPPORTED_OPTION if getattr(args, o)]

    if unsupported_options:
        raise AirflowException(
            "Option --raw does not work with some of the other options on this command. You "
            "can't use --raw option and the following options: {}. You provided the option {}. "
            "Delete it to execute the command".format(
                ", ".join(f"--{o}" for o in RAW_TASK_UNSUPPORTED_OPTION),
                ", ".join(f"--{o}" for o in unsupported_options),
            )
        )
    ti._run_raw_task(  # pylint: disable=protected-access
        mark_success=args.mark_success,
        job_id=args.job_id,
        pool=args.pool,
        error_file=args.error_file,
    )


@contextmanager
def _capture_task_logs(ti):
    """Manage logging context for a task run

    - Replace the root logger configuration with the airflow.task configuration
      so we can capture logs from any custom loggers used in the task.

    - Redirect stdout and stderr to the task instance log, as INFO and WARNING
      level messages, respectively.

    """
    modify = not settings.DONOT_MODIFY_HANDLERS

    if modify:
        root_logger, task_logger = logging.getLogger(), logging.getLogger('airflow.task')

        orig_level = root_logger.level
        root_logger.setLevel(task_logger.level)
        orig_handlers = root_logger.handlers.copy()
        root_logger.handlers[:] = task_logger.handlers

    try:
        info_writer = StreamLogWriter(ti.log, logging.INFO)
        warning_writer = StreamLogWriter(ti.log, logging.WARNING)

        with redirect_stdout(info_writer), redirect_stderr(warning_writer):
            yield

    finally:
        if modify:
            # Restore the root logger to its original state.
            root_logger.setLevel(orig_level)
            root_logger.handlers[:] = orig_handlers


@cli_utils.action_logging
def task_run(args, dag=None):
    """Runs a single task instance"""
    # Load custom airflow config
    if args.cfg_path:
        with open(args.cfg_path) as conf_file:
            conf_dict = json.load(conf_file)

        if os.path.exists(args.cfg_path):
            os.remove(args.cfg_path)

        conf.read_dict(conf_dict, source=args.cfg_path)
        settings.configure_vars()

    # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.configure_orm(disable_connection_pool=True)

    if dag and args.pickle:
        raise AirflowException("You cannot use the --pickle option when using DAG.cli() method.")
    elif args.pickle:
        print(f'Loading pickle id: {args.pickle}')
        dag = get_dag_by_pickle(args.pickle)
    elif not dag:
        dag = get_dag(args.subdir, args.dag_id)
    else:
        # Use DAG from parameter
        pass

    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.refresh_from_db()
    ti.init_run_context(raw=args.raw)

    hostname = get_hostname()

    print(f"Running {ti} on host {hostname}")

    if args.interactive:
        _run_task_by_selected_method(args, dag, ti)
    else:
        with _capture_task_logs(ti):
            _run_task_by_selected_method(args, dag, ti)


@cli_utils.action_logging
def task_failed_deps(args):
    """
    Returns the unmet dependencies for a task instance from the perspective of the
    scheduler (i.e. why a task instance doesn't get scheduled and then queued by the
    scheduler, and then run by an executor).
    >>> airflow tasks failed-deps tutorial sleep 2015-01-01
    Task instance dependencies not met:
    Dagrun Running: Task instance's dagrun did not exist: Unknown reason
    Trigger Rule: Task's trigger rule 'all_success' requires all upstream tasks
    to have succeeded, but found 1 non-success(es).
    """
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)

    dep_context = DepContext(deps=SCHEDULER_QUEUED_DEPS)
    failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
    # TODO, Do we want to print or log this
    if failed_deps:
        print("Task instance dependencies not met:")
        for dep in failed_deps:
            print(f"{dep.dep_name}: {dep.reason}")
    else:
        print("Task instance dependencies are all met.")


@cli_utils.action_logging
@suppress_logs_and_warning
def task_state(args):
    """
    Returns the state of a TaskInstance at the command line.
    >>> airflow tasks state tutorial sleep 2015-01-01
    success
    """
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    print(ti.current_state())


@cli_utils.action_logging
@suppress_logs_and_warning
def task_list(args, dag=None):
    """Lists the tasks within a DAG at the command line"""
    dag = dag or get_dag(args.subdir, args.dag_id)
    if args.tree:
        dag.tree_view()
    else:
        tasks = sorted([t.task_id for t in dag.tasks])
        print("\n".join(tasks))


SUPPORTED_DEBUGGER_MODULES: List[str] = [
    "pudb",
    "web_pdb",
    "ipdb",
    "pdb",
]


def _guess_debugger():
    """
    Trying to guess the debugger used by the user. When it doesn't find any user-installed debugger,
    returns ``pdb``.

    List of supported debuggers:

    * `pudb <https://github.com/inducer/pudb>`__
    * `web_pdb <https://github.com/romanvm/python-web-pdb>`__
    * `ipdb <https://github.com/gotcha/ipdb>`__
    * `pdb <https://docs.python.org/3/library/pdb.html>`__
    """
    for mod in SUPPORTED_DEBUGGER_MODULES:
        try:
            return importlib.import_module(mod)
        except ImportError:
            continue
    return importlib.import_module("pdb")


@cli_utils.action_logging
@suppress_logs_and_warning
def task_states_for_dag_run(args):
    """Get the status of all task instances in a DagRun"""
    with create_session() as session:
        tis = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.execution_date,
                TaskInstance.task_id,
                TaskInstance.state,
                TaskInstance.start_date,
                TaskInstance.end_date,
            )
            .filter(TaskInstance.dag_id == args.dag_id, TaskInstance.execution_date == args.execution_date)
            .all()
        )

        if len(tis) == 0:
            raise AirflowException("DagRun does not exist.")

        AirflowConsole().print_as(
            data=tis,
            output=args.output,
            mapper=lambda ti: {
                "dag_id": ti.dag_id,
                "execution_date": ti.execution_date.isoformat(),
                "task_id": ti.task_id,
                "state": ti.state,
                "start_date": ti.start_date.isoformat() if ti.start_date else "",
                "end_date": ti.end_date.isoformat() if ti.end_date else "",
            },
        )


@cli_utils.action_logging
def task_test(args, dag=None):
    """Tests task for a given dag_id"""
    # We want to log output from operators etc to show up here. Normally
    # airflow.task would redirect to a file, but here we want it to propagate
    # up to the normal airflow handler.
    handlers = logging.getLogger('airflow.task').handlers
    already_has_stream_handler = False
    for handler in handlers:
        already_has_stream_handler = isinstance(handler, logging.StreamHandler)
        if already_has_stream_handler:
            break
    if not already_has_stream_handler:
        logging.getLogger('airflow.task').propagate = True

    env_vars = {'AIRFLOW_TEST_MODE': 'True'}
    if args.env_vars:
        env_vars.update(args.env_vars)
        os.environ.update(env_vars)

    dag = dag or get_dag(args.subdir, args.dag_id)

    task = dag.get_task(task_id=args.task_id)
    # Add CLI provided task_params to task.params
    if args.task_params:
        passed_in_params = json.loads(args.task_params)
        task.params.update(passed_in_params)
    ti = TaskInstance(task, args.execution_date)

    try:
        if args.dry_run:
            ti.dry_run()
        else:
            ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    except Exception:  # pylint: disable=broad-except
        if args.post_mortem:
            debugger = _guess_debugger()
            debugger.post_mortem()
        else:
            raise
    finally:
        if not already_has_stream_handler:
            # Make sure to reset back to normal. When run for CLI this doesn't
            # matter, but it does for test suite
            logging.getLogger('airflow.task').propagate = False


@cli_utils.action_logging
@suppress_logs_and_warning
def task_render(args):
    """Renders and displays templated fields for a given task"""
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.render_templates()
    for attr in task.__class__.template_fields:
        print(
            textwrap.dedent(
                f"""        # ----------------------------------------------------------
        # property: {attr}
        # ----------------------------------------------------------
        {getattr(task, attr)}
        """
            )
        )


@cli_utils.action_logging
def task_clear(args):
    """Clears all task instances or only those matched by regex for a DAG(s)"""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)

    if args.dag_id and not args.subdir and not args.dag_regex and not args.task_regex:
        dags = get_dag_by_file_location(args.dag_id)
    else:
        # todo clear command only accepts a single dag_id. no reason for get_dags with 's' except regex?
        dags = get_dags(args.subdir, args.dag_id, use_regex=args.dag_regex)

        if args.task_regex:
            for idx, dag in enumerate(dags):
                dags[idx] = dag.partial_subset(
                    task_ids_or_regex=args.task_regex,
                    include_downstream=args.downstream,
                    include_upstream=args.upstream,
                )

    DAG.clear_dags(
        dags,
        start_date=args.start_date,
        end_date=args.end_date,
        only_failed=args.only_failed,
        only_running=args.only_running,
        confirm_prompt=not args.yes,
        include_subdags=not args.exclude_subdags,
        include_parentdag=not args.exclude_parentdag,
    )
