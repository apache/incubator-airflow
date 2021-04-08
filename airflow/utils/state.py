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

from enum import Enum
from typing import Dict, FrozenSet, Tuple

from airflow.settings import STATE_COLORS
from airflow.utils.types import Optional


class TaskState(str, Enum):
    """
    Enum that represents all possible states that a Task Instance can be in.

    Note that None is also allowed, so always use this in a type hint with Optional.
    """

    # Set by the scheduler
    # None - Task is created but should not run yet
    REMOVED = "removed"  # Task vanished from DAG before it ran
    SCHEDULED = "scheduled"  # Task should run and will be handed to executor soon

    # Set by the task instance itself
    QUEUED = "queued"  # Executor has enqueued the task
    RUNNING = "running"  # Task is executing
    SUCCESS = "success"  # Task completed
    SHUTDOWN = "shutdown"  # External request to shut down
    FAILED = "failed"  # Task errored out
    UP_FOR_RETRY = "up_for_retry"  # Task failed but has retries left
    UP_FOR_RESCHEDULE = "up_for_reschedule"  # A waiting `reschedule` sensor
    UPSTREAM_FAILED = "upstream_failed"  # One or more upstream deps failed
    SKIPPED = "skipped"  # Skipped by branching or some other mechanism
    SENSING = "sensing"  # Smart sensor offloaded to the sensor DAG


class DagState(str, Enum):
    """
    Enum that represents all possible states that a DagRun can be in.

    These are "shared" with TaskState in some parts of the code, so make
    sure they don't drift.
    """

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class State:
    """
    Static class with task instance states constants and color method to
    avoid hardcoding.
    """

    # Backwards-compat constants for code that does not yet use the enum
    # These first three are shared by DagState and TaskState
    SUCCESS = TaskState.SUCCESS
    RUNNING = TaskState.RUNNING
    FAILED = TaskState.FAILED

    # These are TaskState only
    NONE = None
    REMOVED = TaskState.REMOVED
    SCHEDULED = TaskState.SCHEDULED
    QUEUED = TaskState.QUEUED
    SHUTDOWN = TaskState.SHUTDOWN
    UP_FOR_RETRY = TaskState.UP_FOR_RETRY
    UP_FOR_RESCHEDULE = TaskState.UP_FOR_RESCHEDULE
    UPSTREAM_FAILED = TaskState.UPSTREAM_FAILED
    SKIPPED = TaskState.SKIPPED
    SENSING = TaskState.SENSING

    task_states: Tuple[Optional[TaskState], ...] = (
        None,
        TaskState.SUCCESS,
        TaskState.RUNNING,
        TaskState.FAILED,
        TaskState.UPSTREAM_FAILED,
        TaskState.SKIPPED,
        TaskState.UP_FOR_RETRY,
        TaskState.UP_FOR_RESCHEDULE,
        TaskState.QUEUED,
        TaskState.SCHEDULED,
        TaskState.SENSING,
        TaskState.REMOVED,
    )

    dag_states: Tuple[DagState, ...] = (
        DagState.SUCCESS,
        DagState.RUNNING,
        DagState.FAILED,
    )

    state_color: Dict[Optional[TaskState], str] = {
        None: 'lightblue',
        TaskState.QUEUED: 'gray',
        TaskState.RUNNING: 'lime',
        TaskState.SUCCESS: 'green',
        TaskState.SHUTDOWN: 'blue',
        TaskState.FAILED: 'red',
        TaskState.UP_FOR_RETRY: 'gold',
        TaskState.UP_FOR_RESCHEDULE: 'turquoise',
        TaskState.UPSTREAM_FAILED: 'orange',
        TaskState.SKIPPED: 'pink',
        TaskState.REMOVED: 'lightgrey',
        TaskState.SCHEDULED: 'tan',
        TaskState.SENSING: 'lightseagreen',
    }
    state_color.update(STATE_COLORS)  # type: ignore

    @classmethod
    def color(cls, state):
        """Returns color for a state."""
        return cls.state_color.get(state, 'white')

    @classmethod
    def color_fg(cls, state):
        """Black&white colors for a state."""
        color = cls.color(state)
        if color in ['green', 'red']:
            return 'white'
        return 'black'

    running: FrozenSet[TaskState] = frozenset([TaskState.RUNNING, TaskState.SENSING])
    """
    A list of states indicating that a task is being executed.
    """

    finished: FrozenSet[TaskState] = frozenset(
        [
            TaskState.SUCCESS,
            TaskState.FAILED,
            TaskState.SKIPPED,
            TaskState.UPSTREAM_FAILED,
        ]
    )
    """
    A list of states indicating a task has reached a terminal state (i.e. it has "finished") and needs no
    further action.

    Note that the attempt could have resulted in failure or have been
    interrupted; or perhaps never run at all (skip, or upstream_failed) in any
    case, it is no longer running.
    """

    unfinished: FrozenSet[Optional[TaskState]] = frozenset(
        [
            None,
            TaskState.SCHEDULED,
            TaskState.QUEUED,
            TaskState.RUNNING,
            TaskState.SENSING,
            TaskState.SHUTDOWN,
            TaskState.UP_FOR_RETRY,
            TaskState.UP_FOR_RESCHEDULE,
        ]
    )
    """
    A list of states indicating that a task either has not completed
    a run or has not even started.
    """

    failed_states: FrozenSet[TaskState] = frozenset([TaskState.FAILED, TaskState.UPSTREAM_FAILED])
    """
    A list of states indicating that a task or dag is a failed state.
    """

    success_states: FrozenSet[TaskState] = frozenset([TaskState.SUCCESS, TaskState.SKIPPED])
    """
    A list of states indicating that a task or dag is a success state.
    """


class PokeState:
    """Static class with poke states constants used in smart operator."""

    LANDED = 'landed'
    NOT_LANDED = 'not_landed'
    POKE_EXCEPTION = 'poke_exception'
