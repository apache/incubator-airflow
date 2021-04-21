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

from typing import NamedTuple, Optional

from pendulum import DateTime

from airflow.typing_compat import Protocol


class DataInterval(NamedTuple):
    """A data interval for a DagRun to operate over.

    The represented interval is ``[start, end)``.
    """

    start: DateTime
    end: DateTime


class TimeRestriction(NamedTuple):
    """A period to restrict a datetime between two values.

    This is used to bound the next DagRun schedule to a time period. If the
    scheduled time is earlier than ``earliest``, it is set to ``earliest``. If
    the time is later than ``latest``, the DagRun is not scheduled.

    Both values are inclusive; a DagRun can happen exactly at either
    ``earliest`` or ``latest``.
    """

    earliest: Optional[DateTime]
    latest: Optional[DateTime]


class DagRunInfo(NamedTuple):
    """Information to schedule a DagRun.

    Instances of this will be returned by TimeTables when they are asked to
    schedule a DagRun creation.
    """

    run_after: DateTime
    """The earliest time this DagRun is created and its tasks scheduled."""

    data_interval: DataInterval
    """The data interval this DagRun to operate over, if applicable."""

    @classmethod
    def exact(cls, at: DateTime) -> "DagRunInfo":
        """Represent a run on an exact time."""
        return cls(run_after=at, data_interval=DataInterval(at, at))

    @classmethod
    def interval(cls, start: DateTime, end: DateTime) -> "DagRunInfo":
        """Represent a run on a continuous schedule.

        In such a schedule, each data interval starts right after the previous
        one ends, and each run is scheduled right after the interval ends. This
        applies to all schedules prior to AIP-39 except ``@once`` and ``None``.
        """
        return cls(run_after=end, data_interval=DataInterval(start, end))


class TimeTable(Protocol):
    """Protocol that all TimeTable classes are expected to implement."""

    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        between: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        """Provide information to schedule the next DagRun.

        :param last_automated_dagrun: The execution_date of the associated DAG's
            last scheduled or backfilled run (manual runs not considered).
        :param later_than: The next DagRun must be scheduled later than this
            time. This is generally the earliest of ``DAG.start_date`` and each
            ``BaseOperator.start_date`` in the DAG. None means the next DagRun
            can happen anytime.

        :return: Information on when the next DagRun can be scheduled. None
            means a DagRun will not happen. This does not mean no more runs
            will be scheduled even again for this DAG; the time table can
            return a DagRunInfo when asked later.
        """
        raise NotImplementedError()
