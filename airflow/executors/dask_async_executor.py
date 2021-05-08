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
"""Dask asynchronous executor."""
import asyncio
import random
from asyncio import Task
from typing import Any, Dict, Optional

from distributed import Client, Future, LocalCluster
from distributed.security import Security

from airflow import AirflowException
from airflow.configuration import conf
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKeyType


class DaskAsyncExecutor(BaseExecutor):
    """
    DaskAsyncExecutor runs an async event loop to manage async tasks submitted to a Dask
    Distributed cluster. The Dask client is an asynchronous client, based on Tornado
    coroutines with compatibility for asyncio.  Tasks sent to this executor are assumed to
    support async/await behavior to yield for blocking IO. This executor is not suited
    for CPU-bounds tasks.

    .. seealso::
        - https://distributed.dask.org/en/latest/asynchronous.html
    """

    def __init__(self, cluster_address=None):
        super().__init__(parallelism=0)  # TODO: what does 'parallelism=0' mean?
        if cluster_address is None:
            cluster_address = conf.get("dask", "cluster_address")
        if not cluster_address:
            cluster_address = LocalCluster()
        self.cluster_address = cluster_address

        self.use_async = True

        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._security: Optional[Security] = None
        self._client: Optional[Client] = None
        self._futures: Optional[Dict[Future, TaskInstanceKeyType]] = None

    @property
    def security(self) -> Optional[Security]:
        """
        An optional security object for the ``.client``; it depends on
        airflow.cfg settings for dask security.
        """
        if self._security:
            return self._security
        # ssl / tls parameters
        tls_ca = conf.get("dask", "tls_ca")
        tls_key = conf.get("dask", "tls_key")
        tls_cert = conf.get("dask", "tls_cert")
        if tls_ca or tls_key or tls_cert:
            self._security = Security(
                tls_client_key=tls_key,
                tls_client_cert=tls_cert,
                tls_ca_file=tls_ca,
                require_encryption=True,
            )
            self.log.info("The %s security is initialized.", self.__class__.__name__)
            return self._security
        return None

    @property
    def client(self) -> Client:
        """
        A dask distributed client, with asynchronous futures
        """
        if self._client is None:
            self._client = Client(
                self.cluster_address, security=self.security, asynchronous=self.use_async
            )
            self.log.info("The %s client is initialized.", self.__class__.__name__)
        return self._client

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        """
        An asyncio event loop
        """
        if self._event_loop is None:
            self._event_loop = asyncio.get_event_loop()
            self._event_loop.run_forever()
            self.log.info("The %s event_loop is initialized.", self.__class__.__name__)
        return self._event_loop

    @property
    def futures(self) -> Dict[Future, TaskInstanceKeyType]:
        """
        A collection of asynchronous futures
        """
        if self._futures is None:
            self._futures = {}
            self.log.info("The %s futures are initialized.", self.__class__.__name__)
        return self._futures

    def start(self) -> None:
        if self.client is None:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if self.event_loop is None:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if self.futures is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

    def execute_async(
        self,
        key: TaskInstanceKeyType,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:
        self.start()

        # TODO: only submit awaitable tasks to the event loop;
        #       all other blocking tasks should raise an exception or something?

        async def submit_coro_func():
            func = command.pop()
            args = command
            command_future = await self.client.submit(func, args)
            self.futures[command_future] = key
            # it's possible to submit a function on a future to the dask client, e.g.
            # self.client.submit(self._update_task_status, command_future)
            # but this might introduce complex async behavior into the task updates, so
            # it's avoided here (for now).

        # self.event_loop.call_soon_threadsafe(submit_coro_func)
        self.event_loop.call_soon(submit_coro_func)

    def sync(self) -> None:
        for command_future, _ in self.futures.copy():
            if command_future.done():
                self._update_task_status(command_future)
                self.futures.pop(command_future)

    def end(self) -> None:
        """
        This method is called when the caller is done submitting job and
        wants to wait for the jobs submitted previously to be complete.
        """

        async def wait_coro_func():
            while self.futures:
                for command_future in self.futures.copy():
                    if command_future.done():
                        self._update_task_status(command_future)
                        self.futures.pop(command_future)
                pause = random.uniform(1, 10)
                await asyncio.sleep(pause)

        # self.event_loop.call_soon_threadsafe(wait_coro_func)
        self.event_loop.call_soon(wait_coro_func)
        # wait_coro_obj = wait_coro_func()
        # self.event_loop.create_task(wait_coro_obj)

        self._ensure_stopped()

    def terminate(self) -> None:
        """
        This method is called when the daemon receives a SIGTERM
        """
        self.start()

        async def kill_coro_func():
            await self.client.cancel(list(self.futures.keys()))
            while self.futures:
                for command_future in self.futures.copy():
                    if command_future.done():
                        self._update_task_status(command_future)
                        self.futures.pop(command_future)
                pause = random.uniform(1, 10)
                await asyncio.sleep(pause)
            await self._ensure_stopped()

        # self.event_loop.call_soon_threadsafe(kill_coro_func)
        self.event_loop.call_soon(kill_coro_func)

    async def _ensure_stopped(self):
        await self.client.cancel(list(self.futures.keys()))
        for command_future in self.futures.copy():
            command_future.cancel()
        for task in Task.all_tasks():
            task.cancel()
        await self.client.close()
        self._client = None
        self._futures = None
        await self.event_loop.stop()
        await self.event_loop.close()
        self._event_loop = None

    def _update_task_status(self, future: Future) -> None:
        # TODO: figure out if this is a blocking operation or not
        #       - does it require any db-connection or db-operation
        #         to update the task status?
        if future.done():
            key = self.futures[future]
            if future.cancelled():
                self.log.error("Failed to execute task: task cancelled")
                self.fail(key)
            elif future.exception():
                self.log.error("Failed to execute task: %s", repr(future.exception()))
                self.fail(key)
            else:
                self.success(key)
