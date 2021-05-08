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
from typing import Dict, Union

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone


class DateTimeSensor(BaseSensorOperator):
    """
    Waits until the specified datetime.

    A major advantage of this sensor is idempotence for the ``target_time``.
    It handles some cases for which ``TimeSensor`` and ``TimeDeltaSensor`` are not suited.

    **Example** 1 :
        If a task needs to wait for 11am on each ``execution_date``. Using
        ``TimeSensor`` or ``TimeDeltaSensor``, all backfill tasks started at
        1am have to wait for 10 hours. This is unnecessary, e.g. a backfill
        task with ``{{ ds }} = '1970-01-01'`` does not need to wait because
        ``1970-01-01T11:00:00`` has already passed.

    **Example** 2 :
        If a DAG is scheduled to run at 23:00 daily, but one of the tasks is
        required to run at 01:00 next day, using ``TimeSensor`` will return
        ``True`` immediately because 23:00 > 01:00. Instead, we can do this:

        .. code-block:: python

            DateTimeSensor(
                task_id='wait_for_0100',
                target_time='{{ next_execution_date.tomorrow().replace(hour=1) }}',
            )

    :param target_time: datetime after which the job succeeds. (templated)
    :type target_time: str or datetime.datetime
    """

    template_fields = ("target_time",)

    def __init__(self, *, target_time: Union[str, datetime.datetime], **kwargs) -> None:
        super().__init__(**kwargs)
        if isinstance(target_time, datetime.datetime):
            self.target_time = target_time.isoformat()
        elif isinstance(target_time, str):
            self.target_time = target_time
        else:
            raise TypeError(
                f"Expected str or datetime.datetime type for target_time. Got {type(target_time)}"
            )

    def poke(self, context: Dict) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_time)
        return timezone.utcnow() > timezone.parse(self.target_time)


class DateTimeSensorAsync(DateTimeSensor):
    """
    Waits until the specified datetime, deferring itself to avoid taking up
    a worker slot while it is waiting.

    It is a drop-in replacement for DateTimeSensor.

    :param target_time: datetime after which the job succeeds. (templated)
    :type target_time: str or datetime.datetime
    """

    def execute(self, context):
        self.defer(trigger=DateTimeTrigger(moment=self.target_time), method_name="execute_complete")

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """Callback for when the trigger fires - returns immediately."""
        return None
