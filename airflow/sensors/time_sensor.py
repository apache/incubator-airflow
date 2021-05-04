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


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    :param target_time: time after which the job succeeds
    :type target_time: datetime.time
    """

    def __init__(self, *, target_time, **kwargs):
        super().__init__(**kwargs)
        self.target_time = target_time

    def poke(self, context):
        self.log.info('Checking if the time (%s) has come', self.target_time)
        return timezone.make_naive(timezone.utcnow(), self.dag.timezone).time() > self.target_time


class TimeSensorAsync(BaseSensorOperator):
    """
    Waits until the specified time of the day, freeing up a worker slot while
    it is waiting.

    :param target_time: time after which the job succeeds
    :type target_time: datetime.time
    """

    @apply_defaults
    def __init__(self, *, target_time, **kwargs):
        super().__init__(**kwargs)
        self.target_time = target_time

    def execute(self, context):
        self.log.info("Kicking off trigger deferral")
        self.defer(trigger=DateTimeTrigger(moment=self.target_time), method_name="execute_complete")

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """Callback for when the trigger fires - returns immediately."""
        self.log.info("Trigger complete, sensor complete")
