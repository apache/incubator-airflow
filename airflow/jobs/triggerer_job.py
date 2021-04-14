# pylint: disable=no-name-in-module
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
import asyncio
import importlib
import os
import signal
import sys
import threading
import time
from collections import deque
from typing import Deque, Dict, Tuple, Type

from airflow.jobs.base_job import BaseJob
from airflow.models.trigger import Trigger
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.asyncio import create_task
from airflow.utils.log.logging_mixin import LoggingMixin


class TriggererJob(BaseJob, LoggingMixin):
    """
    TriggererJob continuously runs active triggers in asyncio, watching
    for them to fire off their events and then dispatching that information
    to their dependent tasks/DAGs.

    It runs as two threads:
     - The main thread does DB calls/checkins
     - A subthread runs all the async code
    """

    __mapper_args__ = {'polymorphic_identity': 'TriggererJob'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runner = TriggerRunner()

    def register_signals(self) -> None:
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """Helper method to clean up processor_agent to avoid leaving orphan processes."""
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        # TODO: Add graceful trigger exit
        sys.exit(os.EX_OK)

    def _execute(self) -> None:
        self.log.info("Starting the triggerer")

        try:
            self.runner.start()
            self._run_trigger_loop()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Exception when executing TriggererJob._run_trigger_loop")
            raise
        finally:
            self.log.info("Waiting for triggers to clean up")
            self.runner.stop = True
            self.runner.join()
            self.log.info("Exited trigger loop")

    def _run_trigger_loop(self) -> None:
        """
        The main-thread trigger loop.

        This runs synchronously and handles all database reads/writes.
        """
        while True:
            # Clean out unused triggers
            Trigger.clean_unused()
            # Load/delete triggers
            self._load_new_triggers()
            # Handle events
            self._handle_events()
            # Idle sleep
            time.sleep(1)

    def _load_new_triggers(self):
        """
        Queries the database to get the triggers we're supposed to be running,
        adds them to our runner, and then removes ones from it we no longer
        need.
        """
        requested_triggers = Trigger.runnable()
        self.runner.update_triggers({x.id: x for x in requested_triggers})

    def _handle_events(self):
        """
        Handles outbound events from triggers - dispatching them into the Trigger
        model where they are then pushed into the relevant task instances.
        """
        while self.runner.events:
            # Get the event and its trigger ID
            trigger_id, event = self.runner.events.popleft()
            # Tell the model to wake up its tasks
            Trigger.submit_event(trigger_id=trigger_id, event=event)


class TriggerRunner(threading.Thread, LoggingMixin):
    """
    Runtime environment for all triggers.

    Mainly runs inside its own thread, where it hands control off to an asyncio
    event loop, but is also sometimes interacted with from the main thread
    (where all the DB queries are done). All communication between threads is
    done via Deques.
    """

    # Maps trigger IDs to their running tasks
    triggers: Dict[int, asyncio.Task]

    # Cache for looking up triggers by classpath
    trigger_cache: Dict[str, Type[BaseTrigger]]

    # Inbound queue of new triggers
    to_create: Deque[Tuple[int, BaseTrigger]]

    # Inbound queue of deleted triggers
    to_delete: Deque[int]

    # Outbound queue of events
    events: Deque[Tuple[int, TriggerEvent]]

    # Should-we-stop flag
    stop: bool = False

    def __init__(self):
        super().__init__()
        self.triggers = {}
        self.trigger_cache = {}
        self.to_create = deque()
        self.to_delete = deque()
        self.events = deque()

    def run(self):
        """Sync entrypoint - just runs arun in an async loop."""
        # Pylint complains about this with a 3.6 base, can remove with 3.7+
        asyncio.run(self.arun())  # # pylint: disable=no-member

    async def arun(self):
        """
        Main (asynchronous) logic loop.

        The loop in here runs trigger addition/deletion, and then coexists
        with the triggers running in separate tasks via its sleep.
        """
        watchdog = create_task(self.block_watchdog())
        while not self.stop:
            # If we have any triggers to create, do so
            while self.to_create:
                trigger_id, trigger_instance = self.to_create.popleft()
                if trigger_id not in self.triggers:
                    self.triggers[trigger_id] = create_task(self.run_trigger(trigger_id, trigger_instance))
                else:
                    print("TRIGGER INSERTED TWICE")
            # If we have any triggers to delete, do it
            while self.to_delete:
                trigger_id = self.to_delete.popleft()
                if trigger_id in self.triggers:
                    # We only delete if it did not exit already
                    self.triggers[trigger_id].cancel()
            # Clean up any exited triggers
            for trigger_id, task in list(self.triggers.items()):
                if task.done():
                    del self.triggers[trigger_id]
            # Sleep for a bit
            await asyncio.sleep(1)
        # Wait for watchdog to complete
        await watchdog

    async def block_watchdog(self):
        """
        Watchdog loop that detects blocking (badly-written) triggers.

        Triggers should be well-behaved async coroutines and await whenever
        they need to wait; this loop tries to run every 100ms to see if
        there are badly-written triggers taking longer than that and blocking
        the event loop.

        Unfortunately, we can't tell what trigger is blocking things, but
        we can at least detect the top-level problem.
        """
        while not self.stop:
            last_run = time.monotonic()
            await asyncio.sleep(0.1)
            # We allow a generous amount of buffer room for now, since it might
            # be a busy event loop.
            time_elapsed = time.monotonic() - last_run
            if time_elapsed > 0.2:
                self.log.error(
                    "Triggerer's async thread was blocked for %.2f seconds, "
                    "likely by a badly-written trigger. Set PYTHONASYNCIODEBUG=1 "
                    "to get more information on overrunning coroutines.",
                    time_elapsed,
                )

    # Async trigger logic

    async def run_trigger(self, trigger_id, trigger):
        """
        Wrapper which runs an actual trigger (they are async generators)
        and pushes their events into our outbound event deque.
        """
        print(f"Running trigger {trigger}")
        try:
            async for event in trigger.run():
                print(f"Trigger {trigger_id} fired: {event}")
                self.events.append((trigger_id, event))
        finally:  # CancelledError will get thrown in when we're stopped
            trigger.cleanup()

    # Main-thread sync API

    def update_triggers(self, requested_triggers: Dict[int, Trigger]):
        """
        Called from the main thread to request that we update what
        triggers we're running.
        """
        requested_trigger_ids = set(requested_triggers.keys())
        current_trigger_ids = set(self.triggers.keys())
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids.difference(current_trigger_ids)
        old_trigger_ids = current_trigger_ids.difference(requested_trigger_ids)
        # Add in new triggers
        for new_id in new_trigger_ids:
            # Resolve trigger record into an actual class instance
            trigger_class = self.get_trigger_by_classpath(requested_triggers[new_id].classpath)
            self.to_create.append((new_id, trigger_class(**requested_triggers[new_id].kwargs)))
            print(f"Requesting to add {new_id}")
        # Remove old triggers
        for old_id in old_trigger_ids:
            self.to_delete.append(old_id)
            print(f"Requesting to delete {old_id}")

    def get_trigger_by_classpath(self, classpath: str) -> Type[BaseTrigger]:
        """
        Gets a trigger class by its classpath ("path.to.module.classname")

        Uses a cache dictionary to speed up lookups after the first time.
        """
        if classpath not in self.trigger_cache:
            module_name, class_name = classpath.rsplit(".", 1)
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                raise ImportError(
                    f"Cannot import trigger module {module_name} (from trigger classpath {classpath})"
                )
            try:
                trigger_class = getattr(module, class_name)
            except AttributeError:
                raise ImportError(f"Cannot import trigger {class_name} from module {module_name}")
            self.trigger_cache[classpath] = trigger_class
        return self.trigger_cache[classpath]
