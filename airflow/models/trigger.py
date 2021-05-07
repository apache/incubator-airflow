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
from typing import Any, Dict, List, Optional

import jump
from sqlalchemy import BigInteger, Column, String, func

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
        super().__init__()
        self.classpath = classpath
        self.kwargs = kwargs
        self.created_date = created_date or timezone.utcnow()

    @classmethod
    @provide_session
    def runnable_ids(
        cls, session=None, partition_ids: Optional[List[int]] = None, partition_total: Optional[int] = None
    ):  # pylint: disable=unused-argument
        """
        Returns all "runnable" triggers IDs, optionally filtering down by partition.

        This is a pretty basic partition algorithm for now, but it does the job.
        """
        # NOTE: It's possible in future that we could try and pre-calculate a
        # partition entry in a large virtual ring (e.g. 4096 buckets) and store
        # that in the DB for more direct querying, but for now Jump is fast
        # enough of a hash to do this all locally - about 0.1s per million hashes

        # Retrieve all IDs first
        trigger_ids = [row[0] for row in session.query(cls.id).all()]

        # Short-circuit for "no partitioning"
        if partition_ids is None or partition_total is None:
            return trigger_ids

        # Go through and map each trigger ID to a partition number,
        # using a quick, consistent hash (Jump), keeping only the ones that
        # match one of our partition IDs
        return [x for x in trigger_ids if jump.hash(x, partition_total) + 1 in partition_ids]

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
    def bulk_fetch(cls, ids: List[int], session=None) -> Dict[int, "Trigger"]:
        """
        Fetches all of the Triggers by ID and returns a dict mapping
        ID -> Trigger instance
        """
        return {obj.id: obj for obj in session.query(cls).filter(cls.id.in_(ids)).all()}

    @classmethod
    @provide_session
    def clean_unused(cls, session=None):
        """
        Deletes all triggers that have no tasks/DAGs dependent on them
        (triggers have a one-to-many relationship to both)
        """
        # Update all task instances with trigger IDs that are not DEFERRED to remove them
        session.query(TaskInstance).filter(
            TaskInstance.state != State.DEFERRED, TaskInstance.trigger_id.isnot(None)
        ).update({TaskInstance.trigger_id: None})
        # Get all triggers that have no task instances depending on them, and delete them
        session.query(cls).filter(
            cls.id.in_(
                session.query(cls.id)
                .join(TaskInstance, cls.id == TaskInstance.trigger_id, isouter=True)
                .group_by(cls.id)
                .having(func.count(TaskInstance.trigger_id) == 0)
            )
        ).delete(synchronize_session=False)

    @classmethod
    @provide_session
    def submit_event(cls, trigger_id, event, session=None):
        """
        Takes an event from an instance of itself, and triggers all dependent
        tasks to resume.
        """
        for task_instance in session.query(TaskInstance).filter(
            TaskInstance.trigger_id == trigger_id, TaskInstance.state == State.DEFERRED
        ):
            # Add the event's payload into the kwargs for the task
            next_kwargs = task_instance.next_kwargs or {}
            next_kwargs["event"] = event.payload
            task_instance.next_kwargs = next_kwargs
            # Remove ourselves as its trigger
            task_instance.trigger_id = None
            # Finally, mark it as scheduled so it gets re-queued
            task_instance.state = State.SCHEDULED

    @classmethod
    @provide_session
    def submit_failure(cls, trigger_id, session=None):
        """
        Called when a trigger has failed unexpectedly, and we need to mark
        everything that depended on it as failed.
        """
        session.query(TaskInstance).filter(
            TaskInstance.trigger_id == trigger_id, TaskInstance.state == State.DEFERRED
        ).update({TaskInstance.state: State.FAILED})
