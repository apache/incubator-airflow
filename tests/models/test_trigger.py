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

import pytest

from airflow.models import TaskInstance, Trigger
from airflow.operators.dummy import DummyOperator
from airflow.triggers.base import TriggerEvent
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


@pytest.fixture(autouse=True)
def clear_db(session):
    session.query(TaskInstance).delete()
    session.query(Trigger).delete()
    yield session
    session.query(TaskInstance).delete()
    session.query(Trigger).delete()
    session.commit()


def test_clean_unused(session):
    """
    Tests that unused triggers (those with no task instances referencing them)
    are cleaned out automatically.
    """
    # Make three triggers
    trigger1 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    trigger1.id = 1
    trigger2 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    trigger2.id = 2
    trigger3 = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    trigger3.id = 3
    session.add(trigger1)
    session.add(trigger2)
    session.add(trigger3)
    session.commit()
    assert session.query(Trigger).count() == 3
    # Tie one to a fake TaskInstance that is not deferred, and one to one that is
    fake_task = DummyOperator(task_id="fake")
    task_instance = TaskInstance(task=fake_task, execution_date=timezone.utcnow(), state=State.DEFERRED)
    task_instance.trigger_id = trigger1.id
    session.add(task_instance)
    task_instance = TaskInstance(task=fake_task, execution_date=timezone.utcnow(), state=State.SUCCESS)
    task_instance.trigger_id = trigger2.id
    session.add(task_instance)
    session.commit()
    # Run clear operation
    Trigger.clean_unused()
    # Verify that one trigger is gone, and the right one is left
    assert session.query(Trigger).one().id == trigger1.id


def test_submit_event(session):
    """
    Tests that events submitted to a trigger re-wake their dependent
    task instances.
    """
    # Make a trigger
    trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    trigger.id = 1
    session.add(trigger)
    session.commit()
    # Make a TaskInstance that's deferred and waiting on it
    fake_task = DummyOperator(task_id="fake")
    task_instance = TaskInstance(task=fake_task, execution_date=timezone.utcnow(), state=State.DEFERRED)
    task_instance.trigger_id = trigger.id
    task_instance.next_kwargs = {"cheesecake": True}
    session.add(task_instance)
    session.commit()
    # Call submit_event
    Trigger.submit_event(trigger.id, TriggerEvent(42), session=session)
    # Check that the task instance is now scheduled
    updated_task_instance = session.query(TaskInstance).one()
    assert updated_task_instance.state == State.SCHEDULED
    assert updated_task_instance.next_kwargs == {"event": 42, "cheesecake": True}


def test_submit_failure(session):
    """
    Tests that failures submitted to a trigger fail their dependent
    task instances.
    """
    # Make a trigger
    trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
    trigger.id = 1
    session.add(trigger)
    session.commit()
    # Make a TaskInstance that's deferred and waiting on it
    fake_task = DummyOperator(task_id="fake")
    task_instance = TaskInstance(task=fake_task, execution_date=timezone.utcnow(), state=State.DEFERRED)
    task_instance.trigger_id = trigger.id
    session.add(task_instance)
    session.commit()
    # Call submit_event
    Trigger.submit_failure(trigger.id, session=session)
    # Check that the task instance is now scheduled
    updated_task_instance = session.query(TaskInstance).one()
    assert updated_task_instance.state == State.FAILED
