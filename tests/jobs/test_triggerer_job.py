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

import datetime
import time

import pytest

from airflow.jobs.triggerer_job import TriggererJob
from airflow.models import Trigger
from airflow.triggers.base import TriggerEvent
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.db import clear_db_runs


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    yield  # Test runs here
    clear_db_runs()


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


def test_is_alive():
    """Checks the heartbeat logic"""
    # Current time
    triggerer_job = TriggererJob(None, heartrate=10, state=State.RUNNING)
    assert triggerer_job.is_alive()

    # Slightly old, but still fresh
    triggerer_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
    assert triggerer_job.is_alive()

    # Old enough to fail
    triggerer_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
    assert not triggerer_job.is_alive()

    # Completed state should not be alive
    triggerer_job.state = State.SUCCESS
    triggerer_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
    assert not triggerer_job.is_alive(), "Completed jobs even with recent heartbeat should not be alive"


def test_partition_decode():
    """
    Tests that TriggererJob can decode the various different kinds of partition
    arguments that it can be passed.
    """
    # Positive cases
    variants = [
        (None, (None, None)),
        ("2/4", ([2], 4)),
        ("2,3/5", ([2, 3], 5)),
    ]
    for input_str, (partition_ids, partition_total) in variants:
        job = TriggererJob(partition=input_str)
        assert job.partition_ids == partition_ids
        assert job.partition_total == partition_total
    # Negative cases
    variants = ["0/1", "1", "3/2", "1,2,3/2", "one", "/1"]
    for input_str in variants:
        with pytest.raises(ValueError):
            TriggererJob(partition=input_str)


def test_trigger_lifecycle(session):
    """
    Checks that the triggerer will correctly see a new Trigger in the database
    and send it to the trigger runner, and then delete it when it vanishes.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job.runner.to_create] == [1]
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job.runner.daemon = True
    job.runner.start()
    try:
        # Wait for up to 3 seconds for it to appear in the TriggerRunner's storage
        for _ in range(30):
            if job.runner.triggers:
                assert list(job.runner.triggers.keys()) == [1]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never created trigger")
        # OK, now remove it from the DB
        session.delete(trigger_orm)
        session.commit()
        # Re-load the triggers
        job.load_triggers()
        # Wait for up to 3 seconds for it to vanish from the TriggerRunner's storage
        for _ in range(30):
            if not job.runner.triggers:
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never deleted trigger")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


def test_trigger_firing(session):
    """
    Checks that when a trigger fires, it correctly makes it into the
    event queue.
    """
    # Use a trigger that will immediately succeed
    trigger = SuccessTrigger()
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job.runner.daemon = True
    job.runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if job.runner.events:
                assert list(job.runner.events) == [(1, TriggerEvent(True))]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never sent the trigger event out")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


def test_trigger_failing(session):
    """
    Checks that when a trigger fails, it correctly makes it into the
    failure queue.
    """
    # Use a trigger that will immediately fail
    trigger = FailureTrigger()
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job.runner.daemon = True
    job.runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if job.runner.failed_triggers:
                assert list(job.runner.failed_triggers) == [1]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never marked the trigger as failed")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


def test_trigger_cleanup(session):
    """
    Checks that the triggerer will correctly clean up triggers that do not
    have any task instances depending on them.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Trigger the cleanup code
    Trigger.clean_unused(session=session)
    session.commit()
    # Make sure it's gone
    assert session.query(Trigger).count() == 0
