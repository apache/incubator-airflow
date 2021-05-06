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
"""Processes DAGs."""
import enum
import importlib
import inspect
import logging
import multiprocessing
import os
import random
import signal
import sys
import time
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from importlib import import_module
from multiprocessing.connection import Connection as MultiprocessingConnection
from typing import TYPE_CHECKING, Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Union, cast

from setproctitle import setproctitle  # pylint: disable=no-name-in-module
from sqlalchemy import or_
from tabulate import tabulate

import airflow.models
from airflow.configuration import conf
from airflow.models import DagModel, errors
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.settings import STORE_DAG_CODE
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.callback_requests import CallbackRequest, SlaCallbackRequest, TaskCallbackRequest
from airflow.utils.file import list_py_file_paths
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.mixins import MultiprocessingStartMethodMixin
from airflow.utils.net import get_hostname
from airflow.utils.process_utils import kill_child_processes_by_pids, reap_process_group
from airflow.utils.session import provide_session
from airflow.utils.state import State

if TYPE_CHECKING:
    import pathlib


class AbstractDagFileProcessorProcess(metaclass=ABCMeta):
    """Processes a DAG file. See SchedulerJob.process_file() for more details."""

    @abstractmethod
    def start(self) -> None:
        """Launch the process to process the file"""
        raise NotImplementedError()

    @abstractmethod
    def terminate(self, sigkill: bool = False):
        """Terminate (and then kill) the process launched to process the file"""
        raise NotImplementedError()

    @abstractmethod
    def kill(self) -> None:
        """Kill the process launched to process the file, and ensure consistent state."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def pid(self) -> int:
        """:return: the PID of the process launched to process the given file"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def exit_code(self) -> Optional[int]:
        """
        After the process is finished, this can be called to get the return code
        :return: the exit code of the process
        :rtype: int
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def done(self) -> bool:
        """
        Check if the process launched to process this file is done.
        :return: whether the process is finished running
        :rtype: bool
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def result(self) -> Optional[Tuple[int, int]]:
        """
        A list of simple dags found, and the number of import errors

        :return: result of running SchedulerJob.process_file() if available. Otherwise, none
        :rtype: Optional[Tuple[int, int]]
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def start_time(self) -> datetime:
        """
        :return: When this started to process the file
        :rtype: datetime
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def file_path(self) -> str:
        """
        :return: the path to the file that this is processing
        :rtype: unicode
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def waitable_handle(self):
        """A "waitable" handle that can be passed to ``multiprocessing.connection.wait()``"""
        raise NotImplementedError()


class DagParsingStat(NamedTuple):
    """Information on processing progress"""

    done: bool
    all_files_processed: bool


class DagFileStat(NamedTuple):
    """Information about single processing of one file"""

    num_dags: int
    import_errors: int
    last_finish_time: Optional[datetime]
    last_duration: Optional[float]
    run_count: int


class DagParsingSignal(enum.Enum):
    """All signals sent to parser."""

    AGENT_RUN_ONCE = 'agent_run_once'
    TERMINATE_MANAGER = 'terminate_manager'
    END_MANAGER = 'end_manager'


class DagFileProcessorAgent(LoggingMixin, MultiprocessingStartMethodMixin):
    """
    Agent for DAG file processing. It is responsible for all DAG parsing
    related jobs in scheduler process. Mainly it can spin up DagFileProcessorManager
    in a subprocess, collect DAG parsing results from it and communicate
    signal/DAG parsing stat with it.

    This class runs in the main `airflow scheduler` process.

    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :type dag_directory: str
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :type max_runs: int
    :param processor_factory: function that creates processors for DAG
        definition files. Arguments are (dag_definition_path, log_file_path)
    :type processor_factory: ([str, List[CallbackRequest], Optional[List[str]], bool]) -> (
        AbstractDagFileProcessorProcess
    )
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :type processor_timeout: timedelta
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :type dag_ids: list[str]
    :param pickle_dags: whether to pickle DAGs.
    :type: pickle_dags: bool
    :param async_mode: Whether to start agent in async mode
    :type async_mode: bool
    """

    def __init__(
        self,
        dag_directory: str,
        max_runs: int,
        processor_factory: Callable[
            [str, List[CallbackRequest], Optional[List[str]], bool], AbstractDagFileProcessorProcess
        ],
        processor_timeout: timedelta,
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
        async_mode: bool,
    ):
        super().__init__()
        self._file_path_queue: List[str] = []
        self._dag_directory: str = dag_directory
        self._max_runs = max_runs
        self._processor_factory = processor_factory
        self._processor_timeout = processor_timeout
        self._dag_ids = dag_ids
        self._pickle_dags = pickle_dags
        self._async_mode = async_mode
        # Map from file path to the processor
        self._processors: Dict[str, AbstractDagFileProcessorProcess] = {}
        # Pipe for communicating signals
        self._process: Optional[multiprocessing.process.BaseProcess] = None
        self._done: bool = False
        # Initialized as true so we do not deactivate w/o any actual DAG parsing.
        self._all_files_processed = True

        self._parent_signal_conn: Optional[MultiprocessingConnection] = None

        self._last_parsing_stat_received_at: float = time.monotonic()

    def start(self) -> None:
        """Launch DagFileProcessorManager processor and start DAG parsing loop in manager."""
        mp_start_method = self._get_multiprocessing_start_method()
        context = multiprocessing.get_context(mp_start_method)
        self._last_parsing_stat_received_at = time.monotonic()

        self._parent_signal_conn, child_signal_conn = context.Pipe()
        process = context.Process(
            target=type(self)._run_processor_manager,
            args=(
                self._dag_directory,
                self._max_runs,
                # getattr prevents error while pickling an instance method.
                getattr(self, "_processor_factory"),
                self._processor_timeout,
                child_signal_conn,
                self._dag_ids,
                self._pickle_dags,
                self._async_mode,
            ),
        )
        self._process = process

        process.start()

        self.log.info("Launched DagFileProcessorManager with pid: %s", process.pid)

    def run_single_parsing_loop(self) -> None:
        """
        Should only be used when launched DAG file processor manager in sync mode.
        Send agent heartbeat signal to the manager, requesting that it runs one
        processing "loop".

        Call wait_until_finished to ensure that any launched processors have
        finished before continuing
        """
        if not self._parent_signal_conn or not self._process:
            raise ValueError("Process not started.")
        if not self._process.is_alive():
            return

        try:
            self._parent_signal_conn.send(DagParsingSignal.AGENT_RUN_ONCE)
        except ConnectionError:
            # If this died cos of an error then we will noticed and restarted
            # when harvest_serialized_dags calls _heartbeat_manager.
            pass

    def send_callback_to_execute(self, request: CallbackRequest) -> None:
        """
        Sends information about the callback to be executed by DagFileProcessor.

        :param request: Callback request to be executed.
        :type request: CallbackRequest
        """
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        try:
            self._parent_signal_conn.send(request)
        except ConnectionError:
            # If this died cos of an error then we will noticed and restarted
            # when harvest_serialized_dags calls _heartbeat_manager.
            pass

    def send_sla_callback_request_to_execute(self, full_filepath: str, dag_id: str) -> None:
        """
        Sends information about the SLA callback to be executed by DagFileProcessor.

        :param full_filepath: DAG File path
        :type full_filepath: str
        :param dag_id: DAG ID
        :type dag_id: str
        """
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        try:
            request = SlaCallbackRequest(full_filepath=full_filepath, dag_id=dag_id)
            self._parent_signal_conn.send(request)
        except ConnectionError:
            # If this died cos of an error then we will noticed and restarted
            # when harvest_serialized_dags calls _heartbeat_manager.
            pass

    def wait_until_finished(self) -> None:
        """Waits until DAG parsing is finished."""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        if self._async_mode:
            raise RuntimeError("wait_until_finished should only be called in sync_mode")
        while self._parent_signal_conn.poll(timeout=None):
            try:
                result = self._parent_signal_conn.recv()
            except EOFError:
                return
            self._process_message(result)
            if isinstance(result, DagParsingStat):
                # In sync mode (which is the only time we call this function) we don't send this message from
                # the Manager until all the running processors have finished
                return

    @staticmethod
    def _run_processor_manager(
        dag_directory: str,
        max_runs: int,
        processor_factory: Callable[[str, List[CallbackRequest]], AbstractDagFileProcessorProcess],
        processor_timeout: timedelta,
        signal_conn: MultiprocessingConnection,
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
        async_mode: bool,
    ) -> None:

        # Make this process start as a new process group - that makes it easy
        # to kill all sub-process of this at the OS-level, rather than having
        # to iterate the child processes
        os.setpgid(0, 0)

        setproctitle("airflow scheduler -- DagFileProcessorManager")
        # Reload configurations and settings to avoid collision with parent process.
        # Because this process may need custom configurations that cannot be shared,
        # e.g. RotatingFileHandler. And it can cause connection corruption if we
        # do not recreate the SQLA connection pool.
        os.environ['CONFIG_PROCESSOR_MANAGER_LOGGER'] = 'True'
        os.environ['AIRFLOW__LOGGING__COLORED_CONSOLE_LOG'] = 'False'
        # Replicating the behavior of how logging module was loaded
        # in logging_config.py
        importlib.reload(import_module(airflow.settings.LOGGING_CLASS_PATH.rsplit('.', 1)[0]))  # type: ignore
        importlib.reload(airflow.settings)
        airflow.settings.initialize()
        del os.environ['CONFIG_PROCESSOR_MANAGER_LOGGER']
        processor_manager = DagFileProcessorManager(
            dag_directory,
            max_runs,
            processor_factory,
            processor_timeout,
            signal_conn,
            dag_ids,
            pickle_dags,
            async_mode,
        )

        processor_manager.start()

    def heartbeat(self) -> None:
        """Check if the DagFileProcessorManager process is alive, and process any pending messages"""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        # Receive any pending messages before checking if the process has exited.
        while self._parent_signal_conn.poll(timeout=0.01):
            try:
                result = self._parent_signal_conn.recv()
            except (EOFError, ConnectionError):
                break
            self._process_message(result)

        # If it died unexpectedly restart the manager process
        self._heartbeat_manager()

    def _process_message(self, message):
        self.log.debug("Received message of type %s", type(message).__name__)
        if isinstance(message, DagParsingStat):
            self._sync_metadata(message)
        else:
            raise RuntimeError(f"Unexpected message received of type {type(message).__name__}")

    def _heartbeat_manager(self):
        """Heartbeat DAG file processor and restart it if we are not done."""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        if self._process and not self._process.is_alive():
            self._process.join(timeout=0)
            if not self.done:
                self.log.warning(
                    "DagFileProcessorManager (PID=%d) exited with exit code %d - re-launching",
                    self._process.pid,
                    self._process.exitcode,
                )
                self.start()

        if self.done:
            return

        parsing_stat_age = time.monotonic() - self._last_parsing_stat_received_at
        if parsing_stat_age > self._processor_timeout.total_seconds():
            Stats.incr('dag_processing.manager_stalls')
            self.log.error(
                "DagFileProcessorManager (PID=%d) last sent a heartbeat %.2f seconds ago! Restarting it",
                self._process.pid,
                parsing_stat_age,
            )
            reap_process_group(self._process.pid, logger=self.log)
            self.start()

    def _sync_metadata(self, stat):
        """Sync metadata from stat queue and only keep the latest stat."""
        self._done = stat.done
        self._all_files_processed = stat.all_files_processed
        self._last_parsing_stat_received_at = time.monotonic()

    @property
    def done(self) -> bool:
        """Has DagFileProcessorManager ended?"""
        return self._done

    @property
    def all_files_processed(self):
        """Have all files been processed at least once?"""
        return self._all_files_processed

    def terminate(self):
        """
        Send termination signal to DAG parsing processor manager
        and expect it to terminate all DAG file processors.
        """
        if self._process and self._process.is_alive():
            self.log.info("Sending termination message to manager.")
            try:
                self._parent_signal_conn.send(DagParsingSignal.TERMINATE_MANAGER)
            except ConnectionError:
                pass

    def end(self):
        """
        Terminate (and then kill) the manager process launched.
        :return:
        """
        if not self._process:
            self.log.warning('Ending without manager process.')
            return
        # Give the Manager some time to cleanly shut down, but not too long, as
        # it's better to finish sooner than wait for (non-critical) work to
        # finish
        self._process.join(timeout=1.0)
        reap_process_group(self._process.pid, logger=self.log)
        self._parent_signal_conn.close()


class DagFileProcessorManager(LoggingMixin):  # pylint: disable=too-many-instance-attributes
    """
    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them and put the results to a multiprocessing.Queue
    for DagFileProcessorAgent to harvest. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :type dag_directory: unicode
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :type max_runs: int
    :param processor_factory: function that creates processors for DAG
        definition files. Arguments are (dag_definition_path)
    :type processor_factory: (unicode, unicode, list) -> (AbstractDagFileProcessorProcess)
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :type processor_timeout: timedelta
    :param signal_conn: connection to communicate signal with processor agent.
    :type signal_conn: MultiprocessingConnection
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :type dag_ids: list[str]
    :param pickle_dags: whether to pickle DAGs.
    :type pickle_dags: bool
    :param async_mode: whether to start the manager in async mode
    :type async_mode: bool
    """

    def __init__(
        self,
        dag_directory: Union[str, "pathlib.Path"],
        max_runs: int,
        processor_factory: Callable[[str, List[CallbackRequest]], AbstractDagFileProcessorProcess],
        processor_timeout: timedelta,
        signal_conn: MultiprocessingConnection,
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
        async_mode: bool = True,
    ):
        super().__init__()
        self._file_paths: List[str] = []
        self._file_path_queue: List[str] = []
        self._dag_directory = dag_directory
        self._max_runs = max_runs
        self._processor_factory = processor_factory
        self._signal_conn = signal_conn
        self._pickle_dags = pickle_dags
        self._dag_ids = dag_ids
        self._async_mode = async_mode
        self._parsing_start_time: Optional[int] = None

        # Set the signal conn in to non-blocking mode, so that attempting to
        # send when the buffer is full errors, rather than hangs for-ever
        # attempting to send (this is to avoid deadlocks!)
        #
        # Don't do this in sync_mode, as we _need_ the DagParsingStat sent to
        # continue the scheduler
        if self._async_mode:
            os.set_blocking(self._signal_conn.fileno(), False)

        self._parallelism = conf.getint('scheduler', 'parsing_processes')
        if conf.get('core', 'sql_alchemy_conn').startswith('sqlite') and self._parallelism > 1:
            self.log.warning(
                "Because we cannot use more than 1 thread (parsing_processes = "
                "%d ) when using sqlite. So we set parallelism to 1.",
                self._parallelism,
            )
            self._parallelism = 1

        # Parse and schedule each file no faster than this interval.
        self._file_process_interval = conf.getint('scheduler', 'min_file_process_interval')
        # How often to print out DAG file processing stats to the log. Default to
        # 30 seconds.
        self.print_stats_interval = conf.getint('scheduler', 'print_stats_interval')
        # How many seconds do we wait for tasks to heartbeat before mark them as zombies.
        self._zombie_threshold_secs = conf.getint('scheduler', 'scheduler_zombie_task_threshold')

        # Should store dag file source in a database?
        self.store_dag_code = STORE_DAG_CODE
        # Map from file path to the processor
        self._processors: Dict[str, AbstractDagFileProcessorProcess] = {}

        self._num_run = 0

        # Map from file path to stats about the file
        self._file_stats: Dict[str, DagFileStat] = {}

        self._last_zombie_query_time = None
        # Last time that the DAG dir was traversed to look for files
        self.last_dag_dir_refresh_time = timezone.make_aware(datetime.fromtimestamp(0))
        # Last time stats were printed
        self.last_stat_print_time = 0
        # TODO: Remove magic number
        self._zombie_query_interval = 10
        # How long to wait before timing out a process to parse a DAG file
        self._processor_timeout = processor_timeout

        # How often to scan the DAGs directory for new files. Default to 5 minutes.
        self.dag_dir_list_interval = conf.getint('scheduler', 'dag_dir_list_interval')

        # Mapping file name and callbacks requests
        self._callback_to_execute: Dict[str, List[CallbackRequest]] = defaultdict(list)

        self._log = logging.getLogger('airflow.processor_manager')

        self.waitables: Dict[Any, Union[MultiprocessingConnection, AbstractDagFileProcessorProcess]] = {
            self._signal_conn: self._signal_conn,
        }

    def register_exit_signals(self):
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        # So that we ignore the debug dump signal, making it easier to send
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)

    def _exit_gracefully(self, signum, frame):  # pylint: disable=unused-argument
        """Helper method to clean up DAG file processors to avoid leaving orphan processes."""
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.log.debug("Current Stacktrace is: %s", '\n'.join(map(str, inspect.stack())))
        self.terminate()
        self.end()
        self.log.debug("Finished terminating DAG processors.")
        sys.exit(os.EX_OK)

    def start(self):
        """
        Use multiple processes to parse and generate tasks for the
        DAGs in parallel. By processing them in separate processes,
        we can get parallelism and isolation from potentially harmful
        user code.
        """
        self.register_exit_signals()

        # Start a new process group
        os.setpgid(0, 0)

        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)
        self.log.info(
            "Checking for new files in %s every %s seconds", self._dag_directory, self.dag_dir_list_interval
        )

        return self._run_parsing_loop()

    def _run_parsing_loop(self):

        # In sync mode we want timeout=None -- wait forever until a message is received
        if self._async_mode:
            poll_time = 0.0
        else:
            poll_time = None

        self._refresh_dag_dir()
        self.prepare_file_path_queue()

        if self._async_mode:
            # If we're in async mode, we can start up straight away. If we're
            # in sync mode we need to be told to start a "loop"
            self.start_new_processes()

        while True:
            loop_start_time = time.monotonic()

            # pylint: disable=no-else-break
            ready = multiprocessing.connection.wait(self.waitables.keys(), timeout=poll_time)
            if self._signal_conn in ready:
                agent_signal = self._signal_conn.recv()

                self.log.debug("Received %s signal from DagFileProcessorAgent", agent_signal)
                if agent_signal == DagParsingSignal.TERMINATE_MANAGER:
                    self.terminate()
                    break
                elif agent_signal == DagParsingSignal.END_MANAGER:
                    self.end()
                    sys.exit(os.EX_OK)
                elif agent_signal == DagParsingSignal.AGENT_RUN_ONCE:
                    # continue the loop to parse dags
                    pass
                elif isinstance(agent_signal, CallbackRequest):
                    self._add_callback_to_queue(agent_signal)
                else:
                    raise ValueError(f"Invalid message {type(agent_signal)}")

            if not ready and not self._async_mode:
                # In "sync" mode we don't want to parse the DAGs until we
                # are told to (as that would open another connection to the
                # SQLite DB which isn't a good practice

                # This shouldn't happen, as in sync mode poll should block for
                # ever. Lets be defensive about that.
                self.log.warning(
                    "wait() unexpectedly returned nothing ready after infinite timeout (%r)!", poll_time
                )

                continue

            for sentinel in ready:
                if sentinel is self._signal_conn:
                    continue

                processor = self.waitables.get(sentinel)
                if not processor:
                    continue

                self._collect_results_from_processor(processor)
                self.waitables.pop(sentinel)
                self._processors.pop(processor.file_path)

            self._refresh_dag_dir()
            self._find_zombies()  # pylint: disable=no-value-for-parameter

            self._kill_timed_out_processors()

            # Generate more file paths to process if we processed all the files
            # already.
            if not self._file_path_queue:
                self.emit_metrics()
                self.prepare_file_path_queue()

            self.start_new_processes()

            # Update number of loop iteration.
            self._num_run += 1

            if not self._async_mode:
                self.log.debug("Waiting for processors to finish since we're using sqlite")
                # Wait until the running DAG processors are finished before
                # sending a DagParsingStat message back. This means the Agent
                # can tell we've got to the end of this iteration when it sees
                # this type of message
                self.wait_until_finished()

            # Collect anything else that has finished, but don't kick off any more processors
            self.collect_results()

            self._print_stat()

            all_files_processed = all(self.get_last_finish_time(x) is not None for x in self.file_paths)
            max_runs_reached = self.max_runs_reached()

            try:
                self._signal_conn.send(
                    DagParsingStat(
                        max_runs_reached,
                        all_files_processed,
                    )
                )
            except BlockingIOError:
                # Try again next time around the loop!

                # It is better to fail, than it is deadlock. This should
                # "almost never happen" since the DagParsingStat object is
                # small, and in async mode this stat is not actually _required_
                # for normal operation (It only drives "max runs")
                self.log.debug("BlockingIOError received trying to send DagParsingStat, ignoring")

            if max_runs_reached:
                self.log.info(
                    "Exiting dag parsing loop as all files have been processed %s times", self._max_runs
                )
                break

            if self._async_mode:
                loop_duration = time.monotonic() - loop_start_time
                if loop_duration < 1:
                    poll_time = 1 - loop_duration
                else:
                    poll_time = 0.0

    def _add_callback_to_queue(self, request: CallbackRequest):
        self._callback_to_execute[request.full_filepath].append(request)
        # Callback has a higher priority over DAG Run scheduling
        if request.full_filepath in self._file_path_queue:
            # Remove file paths matching request.full_filepath from self._file_path_queue
            # Since we are already going to use that filepath to run callback,
            # there is no need to have same file path again in the queue
            self._file_path_queue = [
                file_path for file_path in self._file_path_queue if file_path != request.full_filepath
            ]
        self._file_path_queue.insert(0, request.full_filepath)

    def _refresh_dag_dir(self):
        """Refresh file paths from dag dir if we haven't done it for too long."""
        now = timezone.utcnow()
        elapsed_time_since_refresh = (now - self.last_dag_dir_refresh_time).total_seconds()
        if elapsed_time_since_refresh > self.dag_dir_list_interval:
            # Build up a list of Python files that could contain DAGs
            self.log.info("Searching for files in %s", self._dag_directory)
            self._file_paths = list_py_file_paths(self._dag_directory)
            self.last_dag_dir_refresh_time = now
            self.log.info("There are %s files in %s", len(self._file_paths), self._dag_directory)
            self.set_file_paths(self._file_paths)

            try:
                self.log.debug("Removing old import errors")
                self.clear_nonexistent_import_errors()  # pylint: disable=no-value-for-parameter
            except Exception:  # noqa pylint: disable=broad-except
                self.log.exception("Error removing old import errors")

            SerializedDagModel.remove_deleted_dags(self._file_paths)
            DagModel.deactivate_deleted_dags(self._file_paths)

            if self.store_dag_code:
                from airflow.models.dagcode import DagCode

                DagCode.remove_deleted_code(self._file_paths)

    def _print_stat(self):
        """Occasionally print out stats about how fast the files are getting processed"""
        if 0 < self.print_stats_interval < time.monotonic() - self.last_stat_print_time:
            if self._file_paths:
                self._log_file_processing_stats(self._file_paths)
            self.last_stat_print_time = time.monotonic()

    @provide_session
    def clear_nonexistent_import_errors(self, session):
        """
        Clears import errors for files that no longer exist.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        """
        query = session.query(errors.ImportError)
        if self._file_paths:
            query = query.filter(~errors.ImportError.filename.in_(self._file_paths))
        query.delete(synchronize_session='fetch')
        session.commit()

    def _log_file_processing_stats(self, known_file_paths):
        """
        Print out stats about how files are getting processed.

        :param known_file_paths: a list of file paths that may contain Airflow
            DAG definitions
        :type known_file_paths: list[unicode]
        :return: None
        """
        # File Path: Path to the file containing the DAG definition
        # PID: PID associated with the process that's processing the file. May
        # be empty.
        # Runtime: If the process is currently running, how long it's been
        # running for in seconds.
        # Last Runtime: If the process ran before, how long did it take to
        # finish in seconds
        # Last Run: When the file finished processing in the previous run.
        headers = ["File Path", "PID", "Runtime", "# DAGs", "# Errors", "Last Runtime", "Last Run"]

        rows = []
        now = timezone.utcnow()
        for file_path in known_file_paths:
            last_runtime = self.get_last_runtime(file_path)
            num_dags = self.get_last_dag_count(file_path)
            num_errors = self.get_last_error_count(file_path)
            file_name = os.path.basename(file_path)
            file_name = os.path.splitext(file_name)[0].replace(os.sep, '.')

            processor_pid = self.get_pid(file_path)
            processor_start_time = self.get_start_time(file_path)
            runtime = (now - processor_start_time) if processor_start_time else None
            last_run = self.get_last_finish_time(file_path)
            if last_run:
                seconds_ago = (now - last_run).total_seconds()
                Stats.gauge(f'dag_processing.last_run.seconds_ago.{file_name}', seconds_ago)
            if runtime:
                Stats.timing(f'dag_processing.last_duration.{file_name}', runtime)

            rows.append((file_path, processor_pid, runtime, num_dags, num_errors, last_runtime, last_run))

        # Sort by longest last runtime. (Can't sort None values in python3)
        rows = sorted(rows, key=lambda x: x[3] or 0.0)

        formatted_rows = []
        for file_path, pid, runtime, num_dags, num_errors, last_runtime, last_run in rows:
            formatted_rows.append(
                (
                    file_path,
                    pid,
                    f"{runtime.total_seconds():.2f}s" if runtime else None,
                    num_dags,
                    num_errors,
                    f"{last_runtime:.2f}s" if last_runtime else None,
                    last_run.strftime("%Y-%m-%dT%H:%M:%S") if last_run else None,
                )
            )
        log_str = (
            "\n"
            + "=" * 80
            + "\n"
            + "DAG File Processing Stats\n\n"
            + tabulate(formatted_rows, headers=headers)
            + "\n"
            + "=" * 80
        )

        self.log.info(log_str)

    def get_pid(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the PID of the process processing the given file or None if
            the specified file is not being processed
        :rtype: int
        """
        if file_path in self._processors:
            return self._processors[file_path].pid
        return None

    def get_all_pids(self):
        """
        :return: a list of the PIDs for the processors that are running
        :rtype: List[int]
        """
        return [x.pid for x in self._processors.values()]

    def get_last_runtime(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the runtime (in seconds) of the process of the last run, or
            None if the file was never processed.
        :rtype: float
        """
        stat = self._file_stats.get(file_path)
        return stat.last_duration if stat else None

    def get_last_dag_count(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the number of dags loaded from that file, or None if the file
            was never processed.
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.num_dags if stat else None

    def get_last_error_count(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the number of import errors from processing, or None if the file
            was never processed.
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.import_errors if stat else None

    def get_last_finish_time(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the finish time of the process of the last run, or None if the
            file was never processed.
        :rtype: datetime
        """
        stat = self._file_stats.get(file_path)
        return stat.last_finish_time if stat else None

    def get_start_time(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the start time of the process that's processing the
            specified file or None if the file is not currently being processed
        :rtype: datetime
        """
        if file_path in self._processors:
            return self._processors[file_path].start_time
        return None

    def get_run_count(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the number of times the given file has been parsed
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.run_count if stat else 0

    def set_file_paths(self, new_file_paths):
        """
        Update this with a new set of paths to DAG definition files.

        :param new_file_paths: list of paths to DAG definition files
        :type new_file_paths: list[unicode]
        :return: None
        """
        self._file_paths = new_file_paths
        self._file_path_queue = [x for x in self._file_path_queue if x in new_file_paths]
        # Stop processors that are working on deleted files
        filtered_processors = {}
        for file_path, processor in self._processors.items():
            if file_path in new_file_paths:
                filtered_processors[file_path] = processor
            else:
                self.log.warning("Stopping processor for %s", file_path)
                Stats.decr('dag_processing.processes')
                processor.terminate()
                self._file_stats.pop(file_path)
        self._processors = filtered_processors

    def wait_until_finished(self):
        """Sleeps until all the processors are done."""
        for processor in self._processors.values():
            while not processor.done:
                time.sleep(0.1)

    def _collect_results_from_processor(self, processor) -> None:
        self.log.debug("Processor for %s finished", processor.file_path)
        Stats.decr('dag_processing.processes')
        last_finish_time = timezone.utcnow()

        if processor.result is not None:
            num_dags, count_import_errors = processor.result
        else:
            self.log.error(
                "Processor for %s exited with return code %s.", processor.file_path, processor.exit_code
            )
            count_import_errors = -1
            num_dags = 0

        stat = DagFileStat(
            num_dags=num_dags,
            import_errors=count_import_errors,
            last_finish_time=last_finish_time,
            last_duration=(last_finish_time - processor.start_time).total_seconds(),
            run_count=self.get_run_count(processor.file_path) + 1,
        )
        self._file_stats[processor.file_path] = stat

    def collect_results(self) -> None:
        """Collect the result from any finished DAG processors"""
        ready = multiprocessing.connection.wait(self.waitables.keys() - [self._signal_conn], timeout=0)

        for sentinel in ready:
            if sentinel is self._signal_conn:
                continue
            processor = cast(AbstractDagFileProcessorProcess, self.waitables[sentinel])
            self.waitables.pop(processor.waitable_handle)
            self._processors.pop(processor.file_path)
            self._collect_results_from_processor(processor)

        self.log.debug("%s/%s DAG parsing processes running", len(self._processors), self._parallelism)

        self.log.debug("%s file paths queued for processing", len(self._file_path_queue))

    def start_new_processes(self):
        """Start more processors if we have enough slots and files to process"""
        while self._parallelism - len(self._processors) > 0 and self._file_path_queue:
            file_path = self._file_path_queue.pop(0)
            # Stop creating duplicate processor i.e. processor with the same filepath
            if file_path in self._processors.keys():
                continue

            callback_to_execute_for_file = self._callback_to_execute[file_path]
            processor = self._processor_factory(
                file_path, callback_to_execute_for_file, self._dag_ids, self._pickle_dags
            )

            del self._callback_to_execute[file_path]
            Stats.incr('dag_processing.processes')

            processor.start()
            self.log.debug("Started a process (PID: %s) to generate tasks for %s", processor.pid, file_path)
            self._processors[file_path] = processor
            self.waitables[processor.waitable_handle] = processor

    def prepare_file_path_queue(self):
        """Generate more file paths to process. Result are saved in _file_path_queue."""
        self._parsing_start_time = time.perf_counter()
        # If the file path is already being processed, or if a file was
        # processed recently, wait until the next batch
        file_paths_in_progress = self._processors.keys()
        now = timezone.utcnow()

        # Sort the file paths by the parsing order mode
        list_mode = conf.get("scheduler", "file_parsing_sort_mode")

        files_with_mtime = {}
        file_paths = []
        is_mtime_mode = list_mode == "modified_time"

        file_paths_recently_processed = []
        for file_path in self._file_paths:

            if is_mtime_mode:
                files_with_mtime[file_path] = os.path.getmtime(file_path)
            else:
                file_paths.append(file_path)

            # Find file paths that were recently processed
            last_finish_time = self.get_last_finish_time(file_path)
            if (
                last_finish_time is not None
                and (now - last_finish_time).total_seconds() < self._file_process_interval
            ):
                file_paths_recently_processed.append(file_path)

        # Sort file paths via last modified time
        if is_mtime_mode:
            file_paths = sorted(files_with_mtime, key=files_with_mtime.get, reverse=True)
        elif list_mode == "alphabetical":
            file_paths = sorted(file_paths)
        elif list_mode == "random_seeded_by_host":
            # Shuffle the list seeded by hostname so multiple schedulers can work on different
            # set of files. Since we set the seed, the sort order will remain same per host
            random.Random(get_hostname()).shuffle(file_paths)

        files_paths_at_run_limit = [
            file_path for file_path, stat in self._file_stats.items() if stat.run_count == self._max_runs
        ]

        file_paths_to_exclude = set(file_paths_in_progress).union(
            file_paths_recently_processed, files_paths_at_run_limit
        )

        # Do not convert the following list to set as set does not preserve the order
        # and we need to maintain the order of file_paths for `[scheduler] file_parsing_sort_mode`
        files_paths_to_queue = [
            file_path for file_path in file_paths if file_path not in file_paths_to_exclude
        ]

        for file_path, processor in self._processors.items():
            self.log.debug(
                "File path %s is still being processed (started: %s)",
                processor.file_path,
                processor.start_time.isoformat(),
            )

        self.log.debug("Queuing the following files for processing:\n\t%s", "\n\t".join(files_paths_to_queue))

        for file_path in files_paths_to_queue:
            if file_path not in self._file_stats:
                self._file_stats[file_path] = DagFileStat(
                    num_dags=0, import_errors=0, last_finish_time=None, last_duration=None, run_count=0
                )

        self._file_path_queue.extend(files_paths_to_queue)

    @provide_session
    def _find_zombies(self, session):
        """
        Find zombie task instances, which are tasks haven't heartbeated for too long
        and update the current zombie list.
        """
        now = timezone.utcnow()
        if (
            not self._last_zombie_query_time
            or (now - self._last_zombie_query_time).total_seconds() > self._zombie_query_interval
        ):
            # to avoid circular imports
            from airflow.jobs.local_task_job import LocalTaskJob as LJ

            self.log.info("Finding 'running' jobs without a recent heartbeat")
            TI = airflow.models.TaskInstance
            DM = airflow.models.DagModel
            limit_dttm = timezone.utcnow() - timedelta(seconds=self._zombie_threshold_secs)
            self.log.info("Failing jobs without heartbeat after %s", limit_dttm)

            zombies = (
                session.query(TI, DM.fileloc)
                .join(LJ, TI.job_id == LJ.id)
                .join(DM, TI.dag_id == DM.dag_id)
                .filter(TI.state == State.RUNNING)
                .filter(
                    or_(
                        LJ.state != State.RUNNING,
                        LJ.latest_heartbeat < limit_dttm,
                    )
                )
                .all()
            )

            self._last_zombie_query_time = timezone.utcnow()
            for ti, file_loc in zombies:
                request = TaskCallbackRequest(
                    full_filepath=file_loc,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Detected as zombie",
                )
                self.log.info("Detected zombie job: %s", request)
                self._add_callback_to_queue(request)
                Stats.incr('zombies_killed')

    def _kill_timed_out_processors(self):
        """Kill any file processors that timeout to defend against process hangs."""
        now = timezone.utcnow()
        for file_path, processor in self._processors.items():
            duration = now - processor.start_time
            if duration > self._processor_timeout:
                self.log.error(
                    "Processor for %s with PID %s started at %s has timed out, killing it.",
                    file_path,
                    processor.pid,
                    processor.start_time.isoformat(),
                )
                Stats.decr('dag_processing.processes')
                Stats.incr('dag_processing.processor_timeouts')
                # TODO: Remove after Airflow 2.0
                Stats.incr('dag_file_processor_timeouts')
                processor.kill()

    def max_runs_reached(self):
        """:return: whether all file paths have been processed max_runs times"""
        if self._max_runs == -1:  # Unlimited runs.
            return False
        for stat in self._file_stats.values():
            if stat.run_count < self._max_runs:
                return False
        if self._num_run < self._max_runs:
            return False
        return True

    def terminate(self):
        """
        Stops all running processors
        :return: None
        """
        for processor in self._processors.values():
            Stats.decr('dag_processing.processes')
            processor.terminate()

    def end(self):
        """
        Kill all child processes on exit since we don't want to leave
        them as orphaned.
        """
        pids_to_kill = self.get_all_pids()
        if pids_to_kill:
            kill_child_processes_by_pids(pids_to_kill)

    def emit_metrics(self):
        """
        Emit metrics about dag parsing summary

        This is called once every time around the parsing "loop" - i.e. after
        all files have been parsed.
        """
        parse_time = time.perf_counter() - self._parsing_start_time
        Stats.gauge('dag_processing.total_parse_time', parse_time)
        Stats.gauge('dagbag_size', sum(stat.num_dags for stat in self._file_stats.values()))
        Stats.gauge(
            'dag_processing.import_errors', sum(stat.import_errors for stat in self._file_stats.values())
        )

    # pylint: disable=missing-docstring
    @property
    def file_paths(self):
        return self._file_paths
