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
"""
LocalExecutor

.. seealso::
    For more information on how the LocalExecutor works, take a look at the guide:
    :ref:`executor:LocalExecutor`
"""
import logging
import os
import subprocess
from abc import abstractmethod
from multiprocessing import Manager, Process
from multiprocessing.managers import SyncManager
from queue import Empty, Queue  # pylint: disable=unused-import  # noqa: F401
from typing import Any, List, Optional, Tuple, Union  # pylint: disable=unused-import # noqa: F401

from setproctitle import setproctitle  # pylint: disable=no-name-in-module

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, PARALLELISM, BaseExecutor, CommandType
from airflow.models.taskinstance import (  # pylint: disable=unused-import # noqa: F401
    TaskInstanceKey,
    TaskInstanceStateType,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

# This is a work to be executed by a worker.
# It can Key and Command - but it can also be None, None which is actually a
# "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
ExecutorWorkType = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]


class LocalWorkerBase(Process, LoggingMixin):
    """
    LocalWorkerBase implementation to run airflow commands. Executes the given
    command and puts the result into a result queue when done, terminating execution.

    :param result_queue: the queue to store result state
    """

    def __init__(self, result_queue: 'Queue[TaskInstanceStateType]'):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.result_queue: 'Queue[TaskInstanceStateType]' = result_queue

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        return super().run()

    def execute_work(self, key: TaskInstanceKey, command: CommandType) -> None:
        """
        Executes command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        if settings.EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
            state = self._execute_work_in_subprocess(command)
        else:
            state = self._execute_work_in_fork(command)

        self.result_queue.put((key, state))

    def _execute_work_in_subprocess(self, command: CommandType) -> str:
        try:
            subprocess.check_call(command, close_fds=True)
            return State.SUCCESS
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to execute task %s.", str(e))
            return State.FAILED

    def _execute_work_in_fork(self, command: CommandType) -> str:
        pid = os.fork()
        if pid:
            # In parent, wait for the child
            pid, ret = os.waitpid(pid, 0)
            return State.SUCCESS if ret == 0 else State.FAILED

        from airflow.sentry import Sentry

        ret = 1
        try:
            import signal

            from airflow.cli.cli_parser import get_parser

            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGUSR2, signal.SIG_DFL)

            parser = get_parser()
            # [1:] - remove "airflow" from the start of the command
            args = parser.parse_args(command[1:])
            args.shut_down_logging = False

            setproctitle(f"airflow task supervisor: {command}")

            args.func(args)
            ret = 0
            return State.SUCCESS
        except Exception as e:  # pylint: disable=broad-except
            self.log.error("Failed to execute task %s.", str(e))
        finally:
            Sentry.flush()
            logging.shutdown()
            os._exit(ret)  # pylint: disable=protected-access
            raise RuntimeError('unreachable -- keep mypy happy')

    @abstractmethod
    def do_work(self):
        """Called in the subprocess and should then execute tasks"""
        raise NotImplementedError()


class LocalWorker(LocalWorkerBase):
    """
    Local worker that executes the task.

    :param result_queue: queue where results of the tasks are put.
    :param key: key identifying task instance
    :param command: Command to execute
    """

    def __init__(
        self, result_queue: 'Queue[TaskInstanceStateType]', key: TaskInstanceKey, command: CommandType
    ):
        super().__init__(result_queue)
        self.key: TaskInstanceKey = key
        self.command: CommandType = command

    def do_work(self) -> None:
        self.execute_work(key=self.key, command=self.command)


class QueuedLocalWorker(LocalWorkerBase):
    """
    LocalWorker implementation that is waiting for tasks from a queue and will
    continue executing commands as they become available in the queue.
    It will terminate execution once the poison token is found.

    :param task_queue: queue from which worker reads tasks
    :param result_queue: queue where worker puts results after finishing tasks
    """

    def __init__(self, task_queue: 'Queue[ExecutorWorkType]', result_queue: 'Queue[TaskInstanceStateType]'):
        super().__init__(result_queue=result_queue)
        self.task_queue = task_queue

    def do_work(self) -> None:
        while True:
            try:
                key, command = self.task_queue.get()
            except EOFError:
                self.log.info(
                    "Failed to read tasks from the task queue because the other "
                    "end has closed the connection. Terminating worker %s.",
                    self.name,
                )
                break
            try:
                if key is None or command is None:
                    # Received poison pill, no more tasks to run
                    break
                self.execute_work(key=key, command=command)
            finally:
                self.task_queue.task_done()


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel.
    It uses the multiprocessing Python library and queues to parallelize the execution
    of tasks.

    :param parallelism: how many parallel processes are run in the executor
    """

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        self.manager: Optional[SyncManager] = None
        self.result_queue: Optional['Queue[TaskInstanceStateType]'] = None
        self.workers: List[QueuedLocalWorker] = []
        self.workers_used: int = 0
        self.workers_active: int = 0
        self.impl: Optional[
            Union['LocalExecutor.UnlimitedParallelism', 'LocalExecutor.LimitedParallelism']
        ] = None

    class UnlimitedParallelism:
        """
        Implements LocalExecutor with unlimited parallelism, starting one process
        per each command to execute.

        :param executor: the executor instance to implement.
        """

        def __init__(self, executor: 'LocalExecutor'):
            self.executor: 'LocalExecutor' = executor

        def start(self) -> None:
            """Starts the executor."""
            self.executor.workers_used = 0
            self.executor.workers_active = 0

        # pylint: disable=unused-argument # pragma: no cover
        def execute_async(
            self,
            key: TaskInstanceKey,
            command: CommandType,
            queue: Optional[str] = None,
            executor_config: Optional[Any] = None,
        ) -> None:
            """
            Executes task asynchronously.

            :param key: the key to identify the task instance
            :param command: the command to execute
            :param queue: Name of the queue
            :param executor_config: configuration for the executor
            """
            if not self.executor.result_queue:
                raise AirflowException(NOT_STARTED_MESSAGE)
            local_worker = LocalWorker(self.executor.result_queue, key=key, command=command)
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

        # pylint: enable=unused-argument # pragma: no cover
        def sync(self) -> None:
            """Sync will get called periodically by the heartbeat method."""
            if not self.executor.result_queue:
                raise AirflowException("Executor should be started first")
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

        def end(self) -> None:
            """
            This method is called when the caller is done submitting job and
            wants to wait synchronously for the job submitted previously to be
            all done.
            """
            while self.executor.workers_active > 0:
                self.executor.sync()

    class LimitedParallelism:
        """
        Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution.

        :param executor: the executor instance to implement.
        """

        def __init__(self, executor: 'LocalExecutor'):
            self.executor: 'LocalExecutor' = executor
            self.queue: Optional['Queue[ExecutorWorkType]'] = None

        def start(self) -> None:
            """Starts limited parallelism implementation."""
            if not self.executor.manager:
                raise AirflowException(NOT_STARTED_MESSAGE)
            self.queue = self.executor.manager.Queue()
            if not self.executor.result_queue:
                raise AirflowException(NOT_STARTED_MESSAGE)
            self.executor.workers = [
                QueuedLocalWorker(self.queue, self.executor.result_queue)
                for _ in range(self.executor.parallelism)
            ]

            self.executor.workers_used = len(self.executor.workers)

            for worker in self.executor.workers:
                worker.start()

        def execute_async(
            self,
            key: TaskInstanceKey,
            command: CommandType,
            queue: Optional[str] = None,  # pylint: disable=unused-argument
            executor_config: Optional[Any] = None,  # pylint: disable=unused-argument
        ) -> None:
            """
            Executes task asynchronously.

            :param key: the key to identify the task instance
            :param command: the command to execute
            :param queue: name of the queue
            :param executor_config: configuration for the executor
            """
            if not self.queue:
                raise AirflowException(NOT_STARTED_MESSAGE)
            self.queue.put((key, command))

        def sync(self):
            """Sync will get called periodically by the heartbeat method."""
            while True:
                try:
                    results = self.executor.result_queue.get_nowait()
                    try:
                        self.executor.change_state(*results)
                    finally:
                        self.executor.result_queue.task_done()
                except Empty:
                    break

        def end(self):
            """Ends the executor. Sends the poison pill to all workers."""
            for _ in self.executor.workers:
                self.queue.put((None, None))

            # Wait for commands to finish
            self.queue.join()
            self.executor.sync()

    def start(self) -> None:
        """Starts the executor"""
        self.manager = Manager()
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = (
            LocalExecutor.UnlimitedParallelism(self)
            if self.parallelism == 0
            else LocalExecutor.LimitedParallelism(self)
        )

        self.impl.start()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:
        """Execute asynchronously."""
        if not self.impl:
            raise AirflowException(NOT_STARTED_MESSAGE)

        self.validate_command(command)

        self.impl.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        if not self.impl:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.impl.sync()

    def end(self) -> None:
        """
        Ends the executor.
        :return:
        """
        if not self.impl:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.manager:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.info(
            "Shutting down LocalExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )
        self.impl.end()
        self.manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
