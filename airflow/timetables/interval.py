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

from typing import Optional

from pendulum import DateTime
from pendulum.tz.timezone import Timezone

from airflow.timetables.base import DagRunInfo, TimeRestriction, TimeTable
from airflow.timetables.schedules import CronSchedule, Delta, DeltaSchedule, Schedule


class DataIntervalTimeTable(TimeTable):
    """Basis for time table implementations that schedule data intervals.

    This kind of time tables create periodic data intervals from an underlying
    schedule representation (e.g. a cron expression, or a timedelta instance),
    and schedule a DagRun at the end of each interval.
    """

    _schedule: Schedule

    def cancel_catchup(self, between: TimeRestriction) -> TimeRestriction:
        return self._schedule.cancel_catchup(between)

    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        between: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_dagrun is None:
            # First run; schedule the run at the first available time matching
            # the schedule, and retrospectively create a data interval for it.
            if between.earliest is None:
                return None
            start = self._schedule.get_next_schedule(between.earliest)
        else:
            # There's a previous run. Create a data interval starting from when
            # the end of the previous interval.
            start = self._schedule.get_next(last_automated_dagrun)
        if between.latest is not None and start > between.latest:
            return None
        end = self._schedule.get_next(start)
        return DagRunInfo.interval(start=start, end=end)


class CronDataIntervalTimeTable(DataIntervalTimeTable):
    """Time table that schedules data intervals with a cron expression.

    This corresponds to ``schedule_interval=<cron>``, where ``<cron>`` is either
    a five/six-segment representation, or one of ``cron_presets``.

    Don't pass ``@once`` in here; use ``OnceTimeTable`` instead.
    """

    def __init__(self, cron: str, timezone: Timezone) -> None:
        self._schedule = CronSchedule(cron, timezone)


class DeltaDataIntervalTimeTable(DataIntervalTimeTable):
    """Time table that schedules data intervals with a time delta.

    This corresponds to ``schedule_interval=<delta>``, where ``<delta>`` is
    either a ``datetime.timedelta`` or ``dateutil.relativedelta.relativedelta``
    instance.
    """

    def __init__(self, delta: Delta) -> None:
        self._schedule = DeltaSchedule(delta)
