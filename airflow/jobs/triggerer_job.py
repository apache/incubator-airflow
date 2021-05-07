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

import asyncio
import importlib
import os
import signal
import sys
import threading
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Set, Tuple, Type

from airflow.jobs.base_job import BaseJob
from airflow.models.trigger import Trigger
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.typing_compat import TypedDict
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

    partition_ids: Optional[List[int]] = None
    partition_total: Optional[int] = None

    def __init__(self, partition=None, *args, **kwargs):
        # Call superclass
        super().__init__(*args, **kwargs)
        # Decode partition information
        self.partition_ids, self.partition_total = None, None
        if partition:
            self.partition_ids, self.partition_total = self.decode_partition(partition)
        # Set up runner async thread
        self.runner = TriggerRunner()

    def decode_partition(self, partition: str) -> Tuple[List[int], int]:
        """
        Given a string-format partition specification, returns the list of
        partition IDs it represents and the partition total.
        """
        try:
            # The partition format is "1,2,3/10" where the numbers before
            # the slash are the partitions we represent, and the number
            # after is the total number. Most users will just have a single
            # partition number, e.g. "2/10".
            ids_str, total_str = partition.split("/", 1)
            partition_total = int(total_str)
            partition_ids = []
            for id_str in ids_str.split(","):
                id_number = int(id_str)
                # Bounds checking (they're 1-indexed, which might catch people out)
                if id_number <= 0 or id_number > self.partition_total:
                    raise ValueError(f"Partition number {id_number} is impossible")
                self.partition_ids.append(id_number)
        except (ValueError, TypeError):
            raise ValueError(f"Invalid partition specification: {partition}")
        return partition_ids, partition_total

    def register_signals(self) -> None:
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """Helper method to clean up processor_agent to avoid leaving orphan processes."""
        # The first time, try to exit nicely
        if not self.runner.stop:
            self.log.info("Exiting gracefully upon receiving signal %s", signum)
            self.runner.stop = True
        else:
            self.log.warning("Forcing exit due to second exit signal %s", signum)
            sys.exit(os.EX_SOFTWARE)

    def _execute(self) -> None:
        # Display custom startup ack depending on plurality of partitions
        if self.partition_ids is None:
            self.log.info("Starting the triggerer")
        elif len(self.partition_ids) == 1:
            self.log.info(
                "Starting the triggerer (partition %s of %s)", self.partition_ids[0], self.partition_total
            )
        else:
            self.log.info(
                "Starting the triggerer (partitions %s of %s)", self.partition_ids, self.partition_total
            )

        try:
            # Kick off runner thread
            self.runner.start()
            # Start our own DB loop in the main thread
            self._run_trigger_loop()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Exception when executing TriggererJob._run_trigger_loop")
            raise
        finally:
            self.log.info("Waiting for triggers to clean up")
            # Tell the subthread to stop and then wait for it.
            # If the user interrupts/terms again, _graceful_exit will allow them
            # to force-kill here.
            self.runner.stop = True
            self.runner.join()
            self.log.info("Exited trigger loop")

    def _run_trigger_loop(self) -> None:
        """
        The main-thread trigger loop.

        This runs synchronously and handles all database reads/writes.
        """
        while not self.runner.stop:
            # Clean out unused triggers
            Trigger.clean_unused()
            # Load/delete triggers
            self.load_triggers()
            # Handle events
            self.handle_events()
            # Handle failed triggers
            self.handle_failed_triggers()
            # Idle sleep
            time.sleep(1)

    def load_triggers(self):
        """
        Queries the database to get the triggers we're supposed to be running,
        adds them to our runner, and then removes ones from it we no longer
        need.
        """
        requested_trigger_ids = Trigger.runnable_ids(
            partition_ids=self.partition_ids, partition_total=self.partition_total
        )
        self.runner.update_triggers(set(requested_trigger_ids))

    def handle_events(self):
        """
        Handles outbound events from triggers - dispatching them into the Trigger
        model where they are then pushed into the relevant task instances.
        """
        while self.runner.events:
            # Get the event and its trigger ID
            trigger_id, event = self.runner.events.popleft()
            # Tell the model to wake up its tasks
            Trigger.submit_event(trigger_id=trigger_id, event=event)

    def handle_failed_triggers(self):
        """
        Handles "failed" triggers - ones that errored or exited before they
        sent an event. Task Instances that depend on them need failing.
        """
        while self.runner.failed_triggers:
            # Tell the model to fail this trigger's deps
            trigger_id = self.runner.failed_triggers.popleft()
            Trigger.submit_failure(trigger_id=trigger_id)


class TriggerDetails(TypedDict):
    """Type class for the trigger details dictionary"""

    task: asyncio.Task
    name: str
    events: int


class TriggerRunner(threading.Thread, LoggingMixin):
    """
    Runtime environment for all triggers.

    Mainly runs inside its own thread, where it hands control off to an asyncio
    event loop, but is also sometimes interacted with from the main thread
    (where all the DB queries are done). All communication between threads is
    done via Deques.
    """

    # Maps trigger IDs to their running tasks and other info
    triggers: Dict[int, TriggerDetails]

    # Cache for looking up triggers by classpath
    trigger_cache: Dict[str, Type[BaseTrigger]]

    # Inbound queue of new triggers
    to_create: Deque[Tuple[int, BaseTrigger]]

    # Inbound queue of deleted triggers
    to_delete: Deque[int]

    # Outbound queue of events
    events: Deque[Tuple[int, TriggerEvent]]

    # Outbound queue of failed triggers
    failed_triggers: Deque[int]

    # Should-we-stop flag
    stop: bool = False

    def __init__(self):
        super().__init__()
        self.triggers = {}
        self.trigger_cache = {}
        self.to_create = deque()
        self.to_delete = deque()
        self.events = deque()
        self.failed_triggers = deque()

    def run(self):
        """Sync entrypoint - just runs arun in an async loop."""
        # Pylint complains about this with a 3.6 base, can remove with 3.7+
        asyncio.run(self.arun())  # pylint: disable=no-member

    async def arun(self):
        """
        Main (asynchronous) logic loop.

        The loop in here runs trigger addition/deletion/cleanup. Actual
        triggers run in their own separate coroutines.
        """
        watchdog = create_task(self.block_watchdog())
        last_status = time.time()
        while not self.stop:
            # Run core logic
            await self.create_triggers()
            await self.delete_triggers()
            await self.cleanup_finished_triggers()
            # Sleep for a bit
            await asyncio.sleep(1)
            # Every minute, log status
            if time.time() - last_status >= 60:
                self.log.info("%i triggers currently running", len(self.triggers))
                last_status = time.time()
        # Wait for watchdog to complete
        await watchdog

    async def create_triggers(self):
        """
        Drain the to_create queue and create all triggers that have been
        requested in the DB that we don't yet have.
        """
        while self.to_create:
            trigger_id, trigger_instance = self.to_create.popleft()
            if trigger_id not in self.triggers:
                self.triggers[trigger_id] = {
                    "task": create_task(self.run_trigger(trigger_id, trigger_instance)),
                    "name": f"{trigger_instance!r} (ID {trigger_id})",
                    "events": 0,
                }
            else:
                self.log.warning("Trigger %s had insertion attempted twice", trigger_id)

    async def delete_triggers(self):
        """
        Drain the to_delete queue and ensure all triggers that are not in the
        DB are cancelled, so the cleanup job deletes them.
        """
        while self.to_delete:
            trigger_id = self.to_delete.popleft()
            if trigger_id in self.triggers:
                # We only delete if it did not exit already
                self.triggers[trigger_id]["task"].cancel()

    async def cleanup_finished_triggers(self):
        """
        Go through all trigger tasks (coroutines) and clean up entries for
        ones that have exited, optionally warning users if the exit was
        not normal.
        """
        for trigger_id, details in list(self.triggers.items()):  # pylint: disable=too-many-nested-blocks
            if details["task"].done():
                # Check to see if it exited for good reasons
                try:
                    result = details["task"].result()
                except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
                    # These are "expected" exceptions and we stop processing here
                    # If we don't, then the system requesting a trigger be removed -
                    # which turns into CancelledError - results in a failure.
                    del self.triggers[trigger_id]
                    continue
                except BaseException as e:
                    # This is potentially bad, so log it.
                    self.log.error("Trigger %s exited with error %s", details["name"], e)
                else:
                    # See if they foolishly returned a TriggerEvent
                    if isinstance(result, TriggerEvent):
                        self.log.error(
                            "Trigger %s returned a TriggerEvent rather than yielding it", details["name"]
                        )
                # See if this exited without sending an event, in which case
                # any task instances depending on it need to be failed
                if details["events"] == 0:
                    self.log.error(
                        "Trigger %s exited without sending an event. Dependent tasks will be failed.",
                        details["name"],
                    )
                    self.failed_triggers.append(trigger_id)
                del self.triggers[trigger_id]

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
        self.log.info("Trigger %s starting", self.triggers[trigger_id]['name'])
        try:
            async for event in trigger.run():
                self.log.info("Trigger %s fired: %s", self.triggers[trigger_id]['name'], event)
                self.triggers[trigger_id]["events"] += 1
                self.events.append((trigger_id, event))
        finally:
            # CancelledError will get injected when we're stopped - which is
            # fine, the cleanup process will understand that, but we want to
            # allow triggers a chance to cleanup, either in that case or if
            # they exit cleanly.
            trigger.cleanup()

    # Main-thread sync API

    def update_triggers(self, requested_trigger_ids: Set[int]):
        """
        Called from the main thread to request that we update what
        triggers we're running.

        Works out the differences - ones to add, and ones to remove - then
        adds them to the deques so the subthread can actually mutate the running
        trigger set.
        """
        current_trigger_ids = set(self.triggers.keys())
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids.difference(current_trigger_ids)
        old_trigger_ids = current_trigger_ids.difference(requested_trigger_ids)
        # Bulk-fetch new trigger records
        new_triggers = Trigger.bulk_fetch(new_trigger_ids)
        # Add in new triggers
        for new_id in new_trigger_ids:
            # Check it didn't vanish in the meantime
            if new_id not in new_triggers:
                self.log.warning("Trigger ID %s disappeared before we could start it", new_id)
                continue
            # Resolve trigger record into an actual class instance
            trigger_class = self.get_trigger_by_classpath(new_triggers[new_id].classpath)
            self.to_create.append((new_id, trigger_class(**new_triggers[new_id].kwargs)))
        # Remove old triggers
        for old_id in old_trigger_ids:
            self.to_delete.append(old_id)

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
