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
"""Utilities module for cli"""
import functools
import json
import logging
import os
import re
import socket
import sys
import threading
import traceback
import warnings
from argparse import Namespace
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional, TypeVar, cast

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.utils import cli_action_loggers
from airflow.utils.platform import getuser, is_terminal_support_colors
from airflow.utils.session import provide_session

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name

if TYPE_CHECKING:
    from airflow.models import DAG


def _check_cli_args(args):
    if not args:
        raise ValueError("Args should be set")
    if not isinstance(args[0], Namespace):
        raise ValueError(
            "1st positional argument should be argparse.Namespace instance," f"but is {type(args[0])}"
        )


def action_logging(f: T) -> T:
    """
    Decorates function to execute function at the same time submitting action_logging
    but in CLI context. It will call action logger callbacks twice,
    one for pre-execution and the other one for post-execution.

    Action logger will be called with below keyword parameters:
        sub_command : name of sub-command
        start_datetime : start datetime instance by utc
        end_datetime : end datetime instance by utc
        full_command : full command line arguments
        user : current user
        log : airflow.models.log.Log ORM instance
        dag_id : dag id (optional)
        task_id : task_id (optional)
        execution_date : execution date (optional)
        error : exception instance if there's an exception

    :param f: function instance
    :return: wrapped function
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """
        An wrapper for cli functions. It assumes to have Namespace instance
        at 1st positional argument

        :param args: Positional argument. It assumes to have Namespace instance
            at 1st positional argument
        :param kwargs: A passthrough keyword argument
        """
        _check_cli_args(args)
        metrics = _build_metrics(f.__name__, args[0])
        cli_action_loggers.on_pre_execution(**metrics)
        try:
            return f(*args, **kwargs)
        except Exception as e:
            metrics['error'] = e
            raise
        finally:
            metrics['end_datetime'] = datetime.utcnow()
            cli_action_loggers.on_post_execution(**metrics)

    return cast(T, wrapper)


def _build_metrics(func_name, namespace):
    """
    Builds metrics dict from function args
    It assumes that function arguments is from airflow.bin.cli module's function
    and has Namespace instance where it optionally contains "dag_id", "task_id",
    and "execution_date".

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """
    from airflow.models import Log

    sub_commands_to_check = {'users', 'connections'}
    sensitive_fields = {'-p', '--password', '--conn-password'}
    full_command = list(sys.argv)
    if full_command[1] in sub_commands_to_check:  # pylint: disable=too-many-nested-blocks
        for idx, command in enumerate(full_command):
            if command in sensitive_fields:
                # For cases when password is passed as "--password xyz" (with space between key and value)
                full_command[idx + 1] = "*" * 8
            else:
                # For cases when password is passed as "--password=xyz" (with '=' between key and value)
                for sensitive_field in sensitive_fields:
                    if command.startswith(f'{sensitive_field}='):
                        full_command[idx] = f'{sensitive_field}={"*" * 8}'

    metrics = {
        'sub_command': func_name,
        'start_datetime': datetime.utcnow(),
        'full_command': f'{full_command}',
        'user': getuser(),
    }

    if not isinstance(namespace, Namespace):
        raise ValueError(
            "namespace argument should be argparse.Namespace instance," f"but is {type(namespace)}"
        )
    tmp_dic = vars(namespace)
    metrics['dag_id'] = tmp_dic.get('dag_id')
    metrics['task_id'] = tmp_dic.get('task_id')
    metrics['execution_date'] = tmp_dic.get('execution_date')
    metrics['host_name'] = socket.gethostname()

    extra = json.dumps({k: metrics[k] for k in ('host_name', 'full_command')})
    log = Log(
        event=f'cli_{func_name}',
        task_instance=None,
        owner=metrics['user'],
        extra=extra,
        task_id=metrics.get('task_id'),
        dag_id=metrics.get('dag_id'),
        execution_date=metrics.get('execution_date'),
    )
    metrics['log'] = log
    return metrics


def process_subdir(subdir: Optional[str]):
    """Expands path to absolute by replacing 'DAGS_FOLDER', '~', '.', etc."""
    if subdir:
        if not settings.DAGS_FOLDER:
            raise ValueError("DAGS_FOLDER variable in settings should be filled.")
        subdir = subdir.replace('DAGS_FOLDER', settings.DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
    return subdir


def get_dag_by_file_location(dag_id: str):
    """Returns DAG of a given dag_id by looking up file location"""
    from airflow.models import DagBag, DagModel

    # Benefit is that logging from other dags in dagbag will not appear
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(dag_id)
        )
    dagbag = DagBag(dag_folder=dag_model.fileloc)
    return dagbag.dags[dag_id]


def get_dag(subdir: Optional[str], dag_id: str) -> "DAG":
    """Returns DAG of a given dag_id"""
    from airflow.models import DagBag

    dagbag = DagBag(process_subdir(subdir))
    if dag_id not in dagbag.dags:
        raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(dag_id)
        )
    return dagbag.dags[dag_id]


def get_dags(subdir: Optional[str], dag_id: str, use_regex: bool = False):
    """Returns DAG(s) matching a given regex or dag_id"""
    from airflow.models import DagBag

    if not use_regex:
        return [get_dag(subdir, dag_id)]
    dagbag = DagBag(process_subdir(subdir))
    matched_dags = [dag for dag in dagbag.dags.values() if re.search(dag_id, dag.dag_id)]
    if not matched_dags:
        raise AirflowException(
            'dag_id could not be found with regex: {}. Either the dag did not exist '
            'or it failed to parse.'.format(dag_id)
        )
    return matched_dags


@provide_session
def get_dag_by_pickle(pickle_id, session=None):
    """Fetch DAG from the database using pickling"""
    from airflow.models import DagPickle

    dag_pickle = session.query(DagPickle).filter(DagPickle.id == pickle_id).first()
    if not dag_pickle:
        raise AirflowException("Who hid the pickle!? [missing pickle]")
    pickle_dag = dag_pickle.pickle
    return pickle_dag


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    """Creates logging paths"""
    if not stderr:
        stderr = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.err')
    if not stdout:
        stdout = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.out')
    if not log:
        log = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.log')

    if not pid:
        pid = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.pid')
    else:
        pid = os.path.abspath(pid)

    return pid, stdout, stderr, log


def setup_logging(filename):
    """Creates log file handler for daemon process"""
    root = logging.getLogger()
    handler = logging.FileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(settings.LOGGING_LEVEL)

    return handler.stream


def sigint_handler(sig, frame):  # pylint: disable=unused-argument
    """
    Returns without error on SIGINT or SIGTERM signals in interactive command mode
    e.g. CTRL+C or kill <PID>
    """
    sys.exit(0)


def sigquit_handler(sig, frame):  # pylint: disable=unused-argument
    """
    Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    print(f"Dumping stack traces for all threads in PID {os.getpid()}")
    id_to_name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    for thread_id, stack in sys._current_frames().items():  # pylint: disable=protected-access
        code.append(f"\n# Thread: {id_to_name.get(thread_id, '')}({thread_id})")
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append(f'File: "{filename}", line {line_number}, in {name}')
            if line:
                code.append(f"  {line.strip()}")
    print("\n".join(code))


class ColorMode:
    """Coloring modes. If `auto` is then automatically detected."""

    ON = "on"
    OFF = "off"
    AUTO = "auto"


def should_use_colors(args) -> bool:
    """Processes arguments and decides whether to enable color in output"""
    if args.color == ColorMode.ON:
        return True
    if args.color == ColorMode.OFF:
        return False
    return is_terminal_support_colors()


def suppress_logs_and_warning(f: T) -> T:
    """
    Decorator to suppress logging and warning messages
    in cli functions.
    """

    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        _check_cli_args(args)
        if args[0].verbose:
            f(*args, **kwargs)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                logging.disable(logging.CRITICAL)
                try:
                    f(*args, **kwargs)
                finally:
                    # logging output again depends on the effective
                    # levels of individual loggers
                    logging.disable(logging.NOTSET)

    return cast(T, _wrapper)
