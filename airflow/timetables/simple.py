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

from airflow.timetables.base import DagRunInfo, TimeRestriction, TimeTable


class NullTimeTable(TimeTable):
    """Time table that never schedules anything.

    This corresponds to ``schedule_interval=None``.
    """

    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        between: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        return None


class OnceTimeTable(TimeTable):
    """Time table that schedules the execution once as soon as possible.

    This corresponds to ``schedule_interval="@once"``.
    """

    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        between: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_dagrun is not None:
            return None  # Already run, no more scheduling.
        if between.earliest is None:
            return None
        run_after = between.earliest
        if between.latest is not None and run_after > between.latest:
            return None
        return DagRunInfo.exact(run_after)
