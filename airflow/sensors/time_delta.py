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

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone


class TimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta after the task's execution_date + schedule_interval.
    In Airflow, the daily task stamped with ``execution_date``
    2016-01-01 can only start running on 2016-01-02. The timedelta here
    represents the time after the execution period has closed.

    :param delta: time length to wait after execution_date before succeeding
    :type delta: datetime.timedelta
    """

    def __init__(self, *, delta, **kwargs):
        super().__init__(**kwargs)
        self.delta = delta

    def poke(self, context):
        dag = context['dag']
        target_dttm = dag.following_schedule(context['execution_date'])
        target_dttm += self.delta
        self.log.info('Checking if the time (%s) has come', target_dttm)
        return timezone.utcnow() > target_dttm


class TimeDeltaSensorAsync(TimeDeltaSensor):
    """
    A drop-in replacement for TimeDeltaSensor that defers itself to avoid
    taking up a worker slot while it is waiting.

    :param delta: time length to wait after execution_date before succeeding
    :type delta: datetime.timedelta
    """

    def execute(self, context):
        dag = context['dag']
        target_dttm = dag.following_schedule(context['execution_date'])
        target_dttm += self.delta
        self.defer(trigger=DateTimeTrigger(moment=target_dttm), method_name="execute_complete")

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """Callback for when the trigger fires - returns immediately."""
        return None
