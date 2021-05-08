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
from typing import Any, Dict, Tuple

import pytz

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone


class DateTimeTrigger(BaseTrigger):
    """
    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.

    The provided datetime MUST be in UTC.
    """

    def __init__(self, moment: datetime.datetime):
        super().__init__()
        # Make sure it's in UTC
        if moment.tzinfo is None:
            self.moment = pytz.utc.localize(moment)
        elif moment.tzinfo == pytz.utc or getattr(moment.tzinfo, "name", None) == "UTC":
            self.moment = moment
        else:
            raise ValueError(f"The passed datetime must be in UTC, not {moment.tzinfo!r}")

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return ("airflow.triggers.temporal.DateTimeTrigger", {"moment": self.moment})

    async def run(self):
        """
        Simple time delay loop until the relevant time is met.

        We do have a two-phase delay to save some cycles, but sleeping is so
        cheap anyway that it's pretty loose.
        """
        # Sleep an hour at a time while it's more than 2 hours away
        while timezone.utcnow() - self.moment > datetime.timedelta(hours=2):
            await (asyncio.sleep(3600))
        # Sleep a second at a time otherwise
        while self.moment > timezone.utcnow():
            await asyncio.sleep(1)
        # Send our single event and then we're done
        yield TriggerEvent(self.moment)


class TimeDeltaTrigger(DateTimeTrigger):
    """
    Subclass to create DateTimeTriggers based on time delays rather
    than exact moments.

    While this is its own distinct class here, it will serialise to a
    DateTimeTrigger class, since they're operationally the same.
    """

    def __init__(self, delta: datetime.timedelta):
        DateTimeTrigger.__init__(self, moment=timezone.utcnow() + delta)
