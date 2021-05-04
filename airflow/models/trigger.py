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
import datetime
from typing import Any, Dict, Optional

from sqlalchemy import BigInteger, Column, String
from sqlalchemy.orm.exc import NoResultFound

from airflow.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow.triggers.base import BaseTrigger
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime
from airflow.utils.state import State


class Trigger(Base):
    """
    Triggers are a workload that run in an asynchronous event loop shared with
    other Triggers, and fire off events that will unpause deferred Tasks,
    start linked DAGs, etc.

    They are persisted into the database and then re-hydrated into a single
    "triggerer" process, where they're all run at once. We model it so that
    there is a many-to-one relationship between Task and Trigger, for future
    deduplication logic to use.

    Rows will be evicted from the database when the triggerer detects no
    active Tasks/DAGs using them. Events are not stored in the database;
    when an Event is fired, the triggerer will directly push its data to the
    appropriate Task/DAG.
    """

    __tablename__ = "trigger"

    id = Column(BigInteger, primary_key=True)
    classpath = Column(String(1000), nullable=False)
    kwargs = Column(ExtendedJSON, nullable=False)
    created_date = Column(UtcDateTime, nullable=False)

    def __init__(
        self, classpath: str, kwargs: Dict[str, Any], created_date: Optional[datetime.datetime] = None
    ):
        self.classpath = classpath
        self.kwargs = kwargs
        self.created_date = created_date or timezone.utcnow()

    @classmethod
    @provide_session
    def runnable(
        cls, session=None, partition_id=None, partition_total=None
    ):  # pylint: disable=unused-argument
        """
        Returns all "runnable" triggers, optionally filtering down by partition.

        This is a pretty basic partition algorithm for now, but it does the job.
        """
        # Efficient short-circuit for "no partitioning"
        if partition_id is None:
            return session.query(cls).all()
        # Split into modulo-based partitions
        raise NotImplementedError()

    @classmethod
    def from_object(cls, trigger: BaseTrigger):
        """
        Alternative constructor that creates a trigger row based directly
        off of a Trigger object.
        """
        classpath, kwargs = trigger.serialize()
        return cls(classpath=classpath, kwargs=kwargs)

    @classmethod
    @provide_session
    def clean_unused(cls, session=None):
        """
        Deletes all triggers that have no tasks/DAGs dependent on them
        (triggers have a one-to-many relationship to both)
        """
        # TODO: Write this as only two SQL queries once I figure out joins in SQLAlchemy
        for trigger in session.query(cls).all():
            # Get all tasks that refer to us (not necessarily that are waiting on us)
            referencing_tasks = session.query(TaskInstance).filter(TaskInstance.trigger_id == trigger.id)
            # For each task, if it's not waiting on us, remove our reference
            dependent = False
            for task in referencing_tasks:
                if task.state != State.DEFERRED:
                    task.trigger_id = None
                else:
                    dependent = True
            # If there were no dependents, delete us
            if not dependent:
                session.delete(trigger)

    @classmethod
    @provide_session
    def submit_event(cls, trigger_id, event, session=None):
        """
        Takes an event from an instance of itself, and triggers all dependent
        tasks to resume.
        """
        try:
            trigger = session.query(cls).filter(cls.id == trigger_id).one()
        except NoResultFound:
            # The trigger has already been deleted
            # (another process did this, and then cleanup ran, already)
            return
        for task_instance in trigger.dependent_tasks(session=session):
            # Check the task is in the "deferred" state (in case of race conditions)
            if task_instance.state != State.DEFERRED:
                return
            # Add the event's payload into the kwargs for the task
            next_kwargs = task_instance.next_kwargs or {}
            next_kwargs["event"] = event.payload
            task_instance.next_kwargs = next_kwargs
            # Remove ourselves as its trigger
            task_instance.trigger_id = None
            # Finally, mark it as scheduled so it gets re-queued
            task_instance.state = State.SCHEDULED

    # Instance methods

    @provide_session
    def dependent_tasks(self, session):
        """
        Returns all tasks that are currently dependent on this trigger
        (i.e. they are in the DEFERRED state).
        """
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.trigger_id == self.id, TaskInstance.state == State.DEFERRED)
            .all()
        )
