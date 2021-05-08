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

"""Dag sub-commands"""
import ast
import errno
import json
import logging
import signal
import subprocess
import sys

from graphviz.dot import Dot

from airflow import settings
from airflow.api.client import get_current_api_client
from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.exceptions import AirflowException, BackfillUnfinished
from airflow.executors.debug_executor import DebugExecutor
from airflow.jobs.base_job import BaseJob
from airflow.models import DagBag, DagModel, DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.utils import cli as cli_utils
from airflow.utils.cli import (
    get_dag,
    get_dag_by_file_location,
    process_subdir,
    sigint_handler,
    suppress_logs_and_warning,
)
from airflow.utils.dot_renderer import render_dag
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State


@cli_utils.action_logging
def dag_backfill(args, dag=None):
    """Creates backfill job or dry run for a DAG"""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)

    signal.signal(signal.SIGTERM, sigint_handler)

    import warnings

    warnings.warn(
        '--ignore-first-depends-on-past is deprecated as the value is always set to True',
        category=PendingDeprecationWarning,
    )

    if args.ignore_first_depends_on_past is False:
        args.ignore_first_depends_on_past = True

    dag = dag or get_dag(args.subdir, args.dag_id)

    if not args.start_date and not args.end_date:
        raise AirflowException("Provide a start_date and/or end_date")

    # If only one date is passed, using same as start and end
    args.end_date = args.end_date or args.start_date
    args.start_date = args.start_date or args.end_date

    if args.task_regex:
        dag = dag.partial_subset(
            task_ids_or_regex=args.task_regex, include_upstream=not args.ignore_dependencies
        )

    run_conf = None
    if args.conf:
        run_conf = json.loads(args.conf)

    if args.dry_run:
        print(f"Dry run of DAG {args.dag_id} on {args.start_date}")
        for task in dag.tasks:
            print(f"Task {task.task_id}")
            ti = TaskInstance(task, args.start_date)
            ti.dry_run()
    else:
        if args.reset_dagruns:
            DAG.clear_dags(
                [dag],
                start_date=args.start_date,
                end_date=args.end_date,
                confirm_prompt=not args.yes,
                include_subdags=True,
                dag_run_state=State.NONE,
            )

        dag.run(
            start_date=args.start_date,
            end_date=args.end_date,
            mark_success=args.mark_success,
            local=args.local,
            donot_pickle=(args.donot_pickle or conf.getboolean('core', 'donot_pickle')),
            ignore_first_depends_on_past=args.ignore_first_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            pool=args.pool,
            delay_on_limit_secs=args.delay_on_limit,
            verbose=args.verbose,
            conf=run_conf,
            rerun_failed_tasks=args.rerun_failed_tasks,
            run_backwards=args.run_backwards,
        )


@cli_utils.action_logging
def dag_trigger(args):
    """Creates a dag run for the specified dag"""
    api_client = get_current_api_client()
    try:
        message = api_client.trigger_dag(
            dag_id=args.dag_id, run_id=args.run_id, conf=args.conf, execution_date=args.exec_date
        )
        print(message)
    except OSError as err:
        raise AirflowException(err)


@cli_utils.action_logging
def dag_delete(args):
    """Deletes all DB records related to the specified dag"""
    api_client = get_current_api_client()
    if (
        args.yes
        or input("This will drop all existing records related to the specified DAG. Proceed? (y/n)").upper()
        == "Y"
    ):
        try:
            message = api_client.delete_dag(dag_id=args.dag_id)
            print(message)
        except OSError as err:
            raise AirflowException(err)
    else:
        print("Cancelled")


@cli_utils.action_logging
def dag_pause(args):
    """Pauses a DAG"""
    set_is_paused(True, args)


@cli_utils.action_logging
def dag_unpause(args):
    """Unpauses a DAG"""
    set_is_paused(False, args)


def set_is_paused(is_paused, args):
    """Sets is_paused for DAG by a given dag_id"""
    dag = DagModel.get_dagmodel(args.dag_id)

    if not dag:
        raise SystemExit(f"DAG: {args.dag_id} does not exist in 'dag' table")

    dag.set_is_paused(is_paused=is_paused)

    print(f"Dag: {args.dag_id}, paused: {is_paused}")


def dag_show(args):
    """Displays DAG or saves it's graphic representation to the file"""
    dag = get_dag(args.subdir, args.dag_id)
    dot = render_dag(dag)
    filename = args.save
    imgcat = args.imgcat

    if filename and imgcat:
        raise SystemExit(
            "Option --save and --imgcat are mutually exclusive. "
            "Please remove one option to execute the command.",
        )
    elif filename:
        _save_dot_to_file(dot, filename)
    elif imgcat:
        _display_dot_via_imgcat(dot)
    else:
        print(dot.source)


def _display_dot_via_imgcat(dot: Dot):
    data = dot.pipe(format='png')
    try:
        with subprocess.Popen("imgcat", stdout=subprocess.PIPE, stdin=subprocess.PIPE) as proc:
            out, err = proc.communicate(data)
            if out:
                print(out.decode('utf-8'))
            if err:
                print(err.decode('utf-8'))
    except OSError as e:
        if e.errno == errno.ENOENT:
            raise SystemExit(
                "Failed to execute. Make sure the imgcat executables are on your systems \'PATH\'"
            )
        else:
            raise


def _save_dot_to_file(dot: Dot, filename: str):
    filename_without_ext, _, ext = filename.rpartition('.')
    dot.render(filename=filename_without_ext, format=ext, cleanup=True)
    print(f"File {filename} saved")


@cli_utils.action_logging
def dag_state(args):
    """
    Returns the state (and conf if exists) of a DagRun at the command line.
    >>> airflow dags state tutorial 2015-01-01T00:00:00.000000
    running
    >>> airflow dags state a_dag_with_conf_passed 2015-01-01T00:00:00.000000
    failed, {"name": "bob", "age": "42"}
    """
    if args.subdir:
        dag = get_dag(args.subdir, args.dag_id)
    else:
        dag = get_dag_by_file_location(args.dag_id)
    dr = DagRun.find(dag.dag_id, execution_date=args.execution_date)
    out = dr[0].state if dr else None
    conf_out = ''
    if out and dr[0].conf:
        conf_out = ', ' + json.dumps(dr[0].conf)
    print(str(out) + conf_out)


@cli_utils.action_logging
def dag_next_execution(args):
    """
    Returns the next execution datetime of a DAG at the command line.
    >>> airflow dags next-execution tutorial
    2018-08-31 10:38:00
    """
    dag = get_dag(args.subdir, args.dag_id)

    if dag.get_is_paused():
        print("[INFO] Please be reminded this DAG is PAUSED now.", file=sys.stderr)

    latest_execution_date = dag.get_latest_execution_date()
    if latest_execution_date:
        next_execution_dttm = dag.following_schedule(latest_execution_date)

        if next_execution_dttm is None:
            print(
                "[WARN] No following schedule can be found. "
                + "This DAG may have schedule interval '@once' or `None`.",
                file=sys.stderr,
            )
            print(None)
        else:
            print(next_execution_dttm)

            for _ in range(1, args.num_executions):
                next_execution_dttm = dag.following_schedule(next_execution_dttm)
                print(next_execution_dttm)
    else:
        print("[WARN] Only applicable when there is execution record found for the DAG.", file=sys.stderr)
        print(None)


@cli_utils.action_logging
@suppress_logs_and_warning
def dag_list_dags(args):
    """Displays dags with or without stats at the command line"""
    dagbag = DagBag(process_subdir(args.subdir))
    data = []
    for dag in dagbag.dags.values():
        data.append(
            {
                "dag_id": dag.dag_id,
                "filepath": dag.filepath,
                "owner": dag.owner,
                "paused": dag.get_is_paused(),
                "error": "",
            }
        )

    # Add errors
    for filepath, err in dagbag.import_errors.items():
        data.append({"dag_id": "", "filepath": filepath, "owner": "", "paused": "", "error": f"{err}"})

    AirflowConsole().print_as(
        data=sorted(data, key=lambda d: d["dag_id"]),
        output=args.output,
    )


@cli_utils.action_logging
@suppress_logs_and_warning
def dag_report(args):
    """Displays dagbag stats at the command line"""
    dagbag = DagBag(process_subdir(args.subdir))
    AirflowConsole().print_as(
        data=dagbag.dagbag_stats,
        output=args.output,
        mapper=lambda x: {
            "file": x.file,
            "duration": x.duration,
            "dag_num": x.dag_num,
            "task_num": x.task_num,
            "dags": sorted(ast.literal_eval(x.dags)),
        },
    )


@cli_utils.action_logging
@suppress_logs_and_warning
def dag_list_jobs(args, dag=None):
    """Lists latest n jobs"""
    queries = []
    if dag:
        args.dag_id = dag.dag_id
    if args.dag_id:
        dagbag = DagBag()

        if args.dag_id not in dagbag.dags:
            error_message = f"Dag id {args.dag_id} not found"
            raise AirflowException(error_message)
        queries.append(BaseJob.dag_id == args.dag_id)

    if args.state:
        queries.append(BaseJob.state == args.state)

    fields = ['dag_id', 'state', 'job_type', 'start_date', 'end_date']
    with create_session() as session:
        all_jobs = (
            session.query(BaseJob)
            .filter(*queries)
            .order_by(BaseJob.start_date.desc())
            .limit(args.limit)
            .all()
        )
        all_jobs = [{f: str(job.__getattribute__(f)) for f in fields} for job in all_jobs]

    AirflowConsole().print_as(
        data=all_jobs,
        output=args.output,
    )


@cli_utils.action_logging
@suppress_logs_and_warning
def dag_list_dag_runs(args, dag=None):
    """Lists dag runs for a given DAG"""
    if dag:
        args.dag_id = dag.dag_id

    dagbag = DagBag()

    if args.dag_id is not None and args.dag_id not in dagbag.dags:
        error_message = f"Dag id {args.dag_id} not found"
        raise AirflowException(error_message)

    state = args.state.lower() if args.state else None
    dag_runs = DagRun.find(
        dag_id=args.dag_id,
        state=state,
        no_backfills=args.no_backfill,
        execution_start_date=args.start_date,
        execution_end_date=args.end_date,
    )

    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    AirflowConsole().print_as(
        data=dag_runs,
        output=args.output,
        mapper=lambda dr: {
            "dag_id": dr.dag_id,
            "run_id": dr.run_id,
            "state": dr.state,
            "execution_date": dr.execution_date.isoformat(),
            "start_date": dr.start_date.isoformat() if dr.start_date else '',
            "end_date": dr.end_date.isoformat() if dr.end_date else '',
        },
    )


@provide_session
@cli_utils.action_logging
def dag_test(args, session=None):
    """Execute one single DagRun for a given DAG and execution date, using the DebugExecutor."""
    dag = get_dag(subdir=args.subdir, dag_id=args.dag_id)
    dag.clear(start_date=args.execution_date, end_date=args.execution_date, dag_run_state=State.NONE)
    try:
        dag.run(executor=DebugExecutor(), start_date=args.execution_date, end_date=args.execution_date)
    except BackfillUnfinished as e:
        print(str(e))

    show_dagrun = args.show_dagrun
    imgcat = args.imgcat_dagrun
    filename = args.save_dagrun
    if show_dagrun or imgcat or filename:
        tis = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == args.dag_id,
                TaskInstance.execution_date == args.execution_date,
            )
            .all()
        )

        dot_graph = render_dag(dag, tis=tis)
        print()
        if filename:
            _save_dot_to_file(dot_graph, filename)
        if imgcat:
            _display_dot_via_imgcat(dot_graph)
        if show_dagrun:
            print(dot_graph.source)
