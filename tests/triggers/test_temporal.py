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
import datetime

import pytest
import pytz

from airflow.triggers.base import TriggerEvent
from airflow.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.utils import timezone
from airflow.utils.asyncio import create_task


def test_datetime_trigger_serialization():
    """
    Tests that the DateTimeTrigger correctly serializes its arguments
    and classpath.
    """
    moment = pytz.utc.localize(dt=datetime.datetime(2020, 4, 1, 13, 0))
    trigger = DateTimeTrigger(moment)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.triggers.temporal.DateTimeTrigger"
    assert kwargs == {"moment": moment}


@pytest.mark.asyncio
async def test_datetime_trigger_timing():
    """
    Tests that the DateTimeTrigger only goes off on or after the appropriate
    time.
    """
    past_moment = timezone.utcnow() - datetime.timedelta(seconds=60)
    future_moment = timezone.utcnow() + datetime.timedelta(seconds=60)

    # Create a task that runs the trigger for a short time then cancels it
    trigger = DateTimeTrigger(future_moment)
    trigger_task = create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # It should not have produced a result
    assert trigger_task.done() is False
    trigger_task.cancel()

    # Now, make one waiting for en event in the past and do it again
    trigger = DateTimeTrigger(past_moment)
    trigger_task = create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
    assert result.payload == past_moment


def test_timedelta_trigger_serialization():
    """
    Tests that the TimeDeltaTrigger correctly serializes its arguments
    and classpath (it turns into a DateTimeTrigger).
    """
    trigger = TimeDeltaTrigger(datetime.timedelta(seconds=10))
    expected_moment = timezone.utcnow() + datetime.timedelta(seconds=10)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.triggers.temporal.DateTimeTrigger"
    # We need to allow for a little time difference to avoid this test being
    # flaky if it runs over the boundary of a single second
    assert -2 < (kwargs["moment"] - expected_moment).total_seconds() < 2
