#! /usr/bin/env python3
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

# pylint: disable=bad-continuation
"""
Asyncio Executor
----------------

This module creates AsyncioExecutor, which follows the
execution API of ``concurrent.futures.Executor``.

.. note::
    Code in this module is partly copied and adapted from:

    - https://gist.github.com/seglberg/0b4487b57b4fd425c56ad72aba9971be
    - public-domain license (as of Jan 2020)

"""

import asyncio
import concurrent.futures
import inspect
import logging
import random
import threading
from asyncio import Future
from typing import Any, Awaitable, Callable, Coroutine, Iterable, Iterator, Optional, T, Union

LOGGER = logging.getLogger("async_executor")
LOGGER.setLevel(logging.INFO)


def loop_mgr(loop: asyncio.AbstractEventLoop):
    """
    An asyncio loop manager, used by :py:class:`.AsyncioExecutor`
    to run the loop forever in a thread and clean up after the
    loop stops.

    :param loop:
    """
    try:
        # loop manager will run this in it's own thread
        loop.run_forever()

        # the loop was stopped and concurrent.futures.Executor
        # promises to complete tasks on shutdown.
        while True:
            tasks = asyncio.Task.all_tasks(loop=loop)
            pending = [t for t in tasks if not t.done()]
            loop.run_until_complete(asyncio.gather(*pending))

            # ensure the task collection is updated
            # (this is _not_ redundant)
            tasks = asyncio.Task.all_tasks(loop=loop)
            if all([t.done() for t in tasks]):
                break

    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        # AsyncioExecutor might restart the loop, so skip:
        # loop.close()  # irreversible


class AsyncioExecutor(concurrent.futures.Executor):
    """
    AsyncioExecutor follows the ``concurrent.futures.Executor`` API

    It wraps an ``asyncio.AbstractEventLoop`` in a thread to manage
    submission of coroutines and blocking tasks.

    Examples:

    .. code-block::

        async def delay(pause: int) -> float:
            try:
                await asyncio.sleep(pause)
                return pause
            except asyncio.CancelledError:
                raise

        loop_executor = AsyncioExecutor()
        task = loop_executor.submit(delay, 1)
        loop_executor.shutdown()  # waits for tasks to complete
        assert task.done()
        pause = task.result()
        assert pause == 1

    .. seealso::
        - https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor

    """

    def __init__(self):

        super().__init__()
        # getting an event loop should only be done once, here, but the loop property
        # exposes the loop and must check if something has closed the loop to get a new one.
        self._loop = asyncio.new_event_loop()
        self._shutdown = False
        self._thread = None
        self.start()

        # Consider managing blocking functions with a process executor, although this
        # might really confuse the purpose and responsibility of this class; so PUNT.
        # self._process_executor = concurrent.futures.ProcessPoolExecutor()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop and self._loop.is_closed():
            # Note: not exactly sure how this interacts with start() and shutdown().
            self._loop = asyncio.new_event_loop()
        return self._loop

    def is_alive(self) -> bool:
        if self._shutdown:
            return False
        return self._thread and self._thread.is_alive() and self.loop.is_running()

    def start(self) -> bool:
        # Use a loop manager to run tasks in it's own thread

        # executors own a new event loop and run it
        # in their own thread; not sure what the limits are to
        # how many executor-loops can be spawned, but the class
        # does not use any kind of thread pool (yet).

        if self.is_alive():
            return True

        self._shutdown = False
        self._thread = threading.Thread(target=loop_mgr, args=(self.loop,), daemon=True)
        self._thread.start()
        return self.is_alive()

    def map(
        self,
        func: Callable,
        *iterables: Iterable[Any],
        timeout: Optional[float] = 120,
        chunksize: int = 100,
    ) -> Iterator[Future]:
        """TODO docs"""

        self._check_loop()

        # TODO: apply chunksize to iteration?  This might be done with a
        #       asyncio.Semaphore but that requires an async coroutine.
        async_tasks = []
        for obj in iterables:
            coro = func(obj)
            # coroutine objects can be wrapped in an async task (future);
            # the event loop creates the task, but does not start it until
            # this creation coroutine is awaited (run by event loop).
            async_task = self.loop.create_task(coro)
            async_tasks.append(async_task)

        return asyncio.as_completed(async_tasks, loop=self.loop, timeout=timeout)

    def submit(
        self, fn: Union[Callable, Coroutine, Awaitable[T]], *args, **kwargs
    ) -> Optional[Future]:
        """
        Schedules the callable, fn, to be executed as ``fn(*args **kwargs)`` and
        returns a Future object representing the execution of the callable.

        Add notes for coroutine-functions, coroutine-objects, and blocking functions;
        apply the right typing annotations.  Maybe split this function up into different
        public (or private) methods to handle different fn types?

        :param fn:
        :param args:
        :param kwargs:
        :return:
        """

        self._check_loop()

        if inspect.iscoroutinefunction(fn):
            coro = fn(*args, **kwargs)
            return asyncio.run_coroutine_threadsafe(coro, self.loop)

        if inspect.iscoroutine(fn):
            return asyncio.run_coroutine_threadsafe(fn, self.loop)

        if inspect.isawaitable(fn) and inspect.isgenerator(fn):
            return asyncio.run_coroutine_threadsafe(fn, self.loop)

        if inspect.isgeneratorfunction(fn):
            gen = fn(*args, **kwargs)
            if inspect.isawaitable(gen) and inspect.isgenerator(gen):
                return asyncio.run_coroutine_threadsafe(gen, self.loop)

        raise RuntimeError("Can only execute awaitable coroutines and generators")

        # Try to submit blocking functions to a process executor, although this
        # might really confuse the purpose and responsibility of this class.
        # func = functools.partial(fn, *args, **kwargs)
        # return self.loop.run_in_executor(self._process_executor, func)

    def shutdown(self, wait=True):
        """
        https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.shutdown

        concurrent.futures.Executor promises to complete
        tasks on shutdown, so gather all pending tasks.

        promises that submitted tasks will run to completion (not cancelled); maybe add
        notes about how the user can first cancel tasks and then shutdown this executor
        :param wait: wait for thread running event loop to join
        """
        if self._shutdown:
            LOGGER.warning("Already shutdown")
            return

        # for some reason, if nothing has been submitted, the shutdown hangs on a thread lock,
        # so first submit a small async sleep to work around this until it can be fixed.
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0.01), self.loop)

        self._shutdown = True
        self.loop.stop()
        if wait:
            while self.loop.is_running():
                pass
            self._thread.join()
        self._thread = None

    def _check_loop(self):
        if self._shutdown:
            raise RuntimeError("Cannot schedule new futures after shutdown")

        if not self.is_alive():
            raise RuntimeError("Loop must be started before any function can be submitted")


#
#  test code
#

#: Minimum task pause
MIN_PAUSE: int = 1

#: Maximum task pause
MAX_PAUSE: int = 10


async def delay(task_id: int) -> float:
    """
    Await a random pause between :py:const:`MIN_PAUSE` and :py:const:`MAX_PAUSE`

    :param task_id: the ID for the asyncio.Task awaiting this pause
    :return: random interval for pause
    """
    pause = random.uniform(MIN_PAUSE, MAX_PAUSE)
    LOGGER.warning("Task %d - await a sleep for %.2f", task_id, pause)
    try:
        await asyncio.sleep(pause)
        LOGGER.warning("Task %d - done with sleep for %.2f", task_id, pause)
        return pause

    except asyncio.CancelledError:
        LOGGER.error("Task %d - cancelled", task_id)
        raise
