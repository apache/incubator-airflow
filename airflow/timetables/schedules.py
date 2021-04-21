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
import typing

from croniter import croniter
from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum.tz.timezone import UTC, Timezone

from airflow.timetables.base import TimeRestriction
from airflow.typing_compat import Protocol
from airflow.utils.dates import cron_presets
from airflow.utils.timezone import convert_to_utc, make_aware, make_naive

Delta = typing.Union[datetime.timedelta, relativedelta]


class Schedule(Protocol):
    """Base protocol for schedules."""

    def cancel_catchup(self, restriction: TimeRestriction) -> TimeRestriction:
        """Fix time restriction to not perform catchup."""
        raise NotImplementedError()

    def get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after the current time."""
        raise NotImplementedError()

    def get_prev(self, current: DateTime) -> DateTime:
        """Get the last schedule before the current time."""
        raise NotImplementedError()

    def get_next_schedule(self, current: DateTime) -> DateTime:
        """Get the next scheduled time.

        This is ``current + interval``, unless ``current`` is first interval,
        then ``current`` is returned.
        """
        next_time = self.get_next(current)
        if self.get_prev(next_time) != current:
            return next_time
        return current


def _is_schedule_fixed(expression: str) -> bool:
    """Figures out if the schedule has a fixed time (e.g. 3 AM every day).

    :return: True if the schedule has a fixed time, False if not.

    Detection is done by "peeking" the next two cron trigger time; if the
    two times have the same minute and hour value, the schedule is fixed,
    and we *don't* need to perform the DST fix.

    This assumes DST happens on whole minute changes (e.g. 12:59 -> 12:00).
    """
    cron = croniter(expression)
    next_a = cron.get_next(datetime.datetime)
    next_b = cron.get_next(datetime.datetime)
    return next_b.minute == next_a.minute and next_b.hour == next_a.hour


class CronSchedule(Schedule):
    """Schedule things from a cron expression.

    The implementation extends on croniter to add timezone awareness. This is
    because crontier works only with naive timestamps, and cannot consider DST
    when determining the next/previous time.
    """

    def __init__(self, expression: str, timezone: Timezone) -> None:
        self._expression = expression = cron_presets.get(expression, expression)
        self._timezone = timezone
        self._should_fix_dst = not _is_schedule_fixed(expression)

    def get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after specified time, with DST fixed."""
        naive = make_naive(current)
        cron = croniter(self._expression, start_time=naive)
        scheduled = cron.get_next(datetime.datetime)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(scheduled, self._timezone))
        delta = scheduled - naive
        return convert_to_utc(current.in_timezone(self._timezone) + delta)

    def get_prev(self, current: DateTime) -> DateTime:
        """Get the first schedule before specified time, with DST fixed."""
        naive = make_naive(current)
        cron = croniter(self._expression, start_time=naive)
        scheduled = cron.get_prev(datetime.datetime)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(scheduled, self._timezone))
        delta = naive - scheduled
        return convert_to_utc(current.in_timezone(self._timezone) - delta)

    def cancel_catchup(self, restriction: TimeRestriction) -> TimeRestriction:
        """Fix time restriction to not perform catchup.

        The logic is that we move start_date up until one period before, so the
        current time is AFTER the period end, and the job can be created...

        This is slightly different from the delta version at terminal values.
        If the next schedule should start *right now*, we want the data interval
        that start right now now, not the one that ends now.
        """
        earliest = restriction.earliest
        current_time = DateTime.now(UTC)
        next_start = self.get_next(current_time)
        last_start = self.get_prev(current_time)
        if next_start == current_time:
            new_start = last_start
        elif next_start > current_time:
            new_start = self.get_prev(last_start)
        else:
            raise AssertionError("next schedule shouldn't be earlier")
        if restriction.earliest is None:
            earliest = new_start
        else:
            earliest = max(new_start, restriction.earliest)
        return TimeRestriction(earliest=earliest, latest=restriction.latest)


class DeltaSchedule(Schedule):
    """Schedule things on a fixed time delta."""

    def __init__(self, delta: Delta) -> None:
        self._delta = delta

    def get_next(self, current: DateTime) -> DateTime:
        return convert_to_utc(current + self._delta)

    def get_prev(self, current: DateTime) -> DateTime:
        return convert_to_utc(current - self._delta)

    def cancel_catchup(self, restriction: TimeRestriction) -> TimeRestriction:
        """Fix time restriction to not perform catchup.

        The logic is that we move start_date up until one period before, so the
        current time is AFTER the period end, and the job can be created...

        This is slightly different from the cron version at terminal values.
        """
        earliest = restriction.earliest
        new_start = self.get_prev(DateTime.now(UTC))
        if restriction.earliest is None:
            earliest = new_start
        else:
            earliest = max(new_start, restriction.earliest)
        return TimeRestriction(earliest=earliest, latest=restriction.latest)
