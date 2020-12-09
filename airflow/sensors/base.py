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
import hashlib
import os
import time
from datetime import timedelta
from typing import Any, Callable, Dict, Iterable

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
)
from airflow.models import BaseOperator, SensorInstance
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskreschedule import TaskReschedule
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class BaseSensorOperator(BaseOperator, SkipMixin):
    """
    Sensor operators are derived from this class and inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: float
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: float
    :param mode: How the sensor operates.
        Options are: ``{ poke | reschedule }``, default is ``poke``.
        When set to ``poke`` the sensor is taking up a worker slot for its
        whole execution time and sleeps between pokes. Use this mode if the
        expected runtime of the sensor is short or if a short poke interval
        is required. Note that the sensor will hold onto a worker slot and
        a pool slot for the duration of the sensor's runtime in this mode.
        When set to ``reschedule`` the sensor task frees the worker slot when
        the criteria is not yet met and it's rescheduled at a later time. Use
        this mode if the time before the criteria is met is expected to be
        quite long. The poke interval should be more than one minute to
        prevent too much load on the scheduler.
    :type mode: str
    :param exponential_backoff: allow progressive longer waits between
        pokes by using exponential backoff algorithm
    :type exponential_backoff: bool
    """

    ui_color = '#e6f1f2'  # type: str
    valid_modes = ['poke', 'reschedule']  # type: Iterable[str]

    # As the poke context in smart sensor defines the poking job signature only,
    # The execution_fields defines other execution details
    # for this tasks such as the customer defined timeout, the email and the alert
    # setup. Smart sensor serialize these attributes into a different DB column so
    # that smart sensor service is able to handle corresponding execution details
    # without breaking the sensor poking logic with dedup.
    execution_fields = (
        'poke_interval',
        'retries',
        'execution_timeout',
        'timeout',
        'email',
        'email_on_retry',
        'email_on_failure',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 24 * 7,
        soft_fail: bool = False,
        mode: str = 'poke',
        exponential_backoff: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self.exponential_backoff = exponential_backoff
        self._validate_input_values()
        self.sensor_service_enabled = conf.getboolean('smart_sensor', 'use_smart_sensor')
        self.sensors_support_sensor_service = set(
            map(lambda l: l.strip(), conf.get('smart_sensor', 'sensors_enabled').split(','))
        )

    def _validate_input_values(self) -> None:
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException("The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException("The timeout must be a non-negative number")
        if self.mode not in self.valid_modes:
            raise AirflowException(
                "The mode must be one of {valid_modes},"
                "'{d}.{t}'; received '{m}'.".format(
                    valid_modes=self.valid_modes,
                    d=self.dag.dag_id if self.dag else "",
                    t=self.task_id,
                    m=self.mode,
                )
            )

    def poke(self, context: Dict) -> bool:
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException('Override me.')

    def is_smart_sensor_compatible(self):
        check_list = [
            not self.sensor_service_enabled,
            self.on_success_callback,
            self.on_retry_callback,
            self.on_failure_callback,
        ]
        for status in check_list:
            if status:
                return False

        operator = self.__class__.__name__
        return operator in self.sensors_support_sensor_service

    def register_in_sensor_service(self, ti, context):
        """
        Register ti in smart sensor service

        :param ti: Task instance object.
        :param context: TaskInstance template context from the ti.
        :return: boolean
        """
        poke_context = self.get_poke_context(context)
        execution_context = self.get_execution_context(context)

        return SensorInstance.register(ti, poke_context, execution_context)

    def get_poke_context(self, context):
        """
        Return a dictionary with all attributes in poke_context_fields. The
        poke_context with operator class can be used to identify a unique
        sensor job.

        :param context: TaskInstance template context.
        :return: A dictionary with key in poke_context_fields.
        """
        if not context:
            self.log.info("Function get_poke_context doesn't have a context input.")

        poke_context_fields = getattr(self.__class__, "poke_context_fields", None)
        result = {key: getattr(self, key, None) for key in poke_context_fields}
        return result

    def get_execution_context(self, context):
        """
        Return a dictionary with all attributes in execution_fields. The
        execution_context include execution requirement for each sensor task
        such as timeout setup, email_alert setup.

        :param context: TaskInstance template context.
        :return: A dictionary with key in execution_fields.
        """
        if not context:
            self.log.info("Function get_execution_context doesn't have a context input.")
        execution_fields = self.__class__.execution_fields

        result = {key: getattr(self, key, None) for key in execution_fields}
        if result['execution_timeout'] and isinstance(result['execution_timeout'], datetime.timedelta):
            result['execution_timeout'] = result['execution_timeout'].total_seconds()
        return result

    def execute(self, context: Dict) -> Any:
        started_at = None

        if self.reschedule:

            # If reschedule, use first start date of current try
            task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
            if task_reschedules:
                started_at = task_reschedules[0].start_date
                try_number = len(task_reschedules) + 1
            else:
                started_at = timezone.utcnow()

            def run_duration() -> float:
                # If we are in reschedule mode, then we have to compute diff
                # based on the time in a DB, so can't use time.monotonic
                nonlocal started_at
                return (timezone.utcnow() - started_at).total_seconds()

        else:
            started_at = time.monotonic()

            def run_duration() -> float:
                nonlocal started_at
                return time.monotonic() - started_at

        try_number = 1
        log_dag_id = self.dag.dag_id if self.has_dag() else ""

        while not self.poke(context):
            if run_duration() > self.timeout:
                # If sensor is in soft fail mode but times out raise AirflowSkipException.
                if self.soft_fail:
                    raise AirflowSkipException(f"Snap. Time is OUT. DAG id: {log_dag_id}")
                else:
                    raise AirflowSensorTimeout(f"Snap. Time is OUT. DAG id: {log_dag_id}")
            if self.reschedule:
                reschedule_date = timezone.utcnow() + timedelta(
                    seconds=self._get_next_poke_interval(started_at, run_duration, try_number)
                )
                raise AirflowRescheduleException(reschedule_date)
            else:
                time.sleep(self._get_next_poke_interval(started_at, run_duration, try_number))
                try_number += 1
        self.log.info("Success criteria met. Exiting.")

    def _get_next_poke_interval(self, started_at: Any, run_duration: Callable[[], int], try_number):
        """Using the similar logic which is used for exponential backoff retry delay for operators."""
        if self.exponential_backoff:
            min_backoff = int(self.poke_interval * (2 ** (try_number - 2)))

            run_hash = int(
                hashlib.sha1(
                    f"{self.dag_id}#{self.task_id}#{started_at}#{try_number}".encode("utf-8")
                ).hexdigest(),
                16,
            )
            modded_hash = min_backoff + run_hash % min_backoff

            delay_backoff_in_seconds = min(modded_hash, timedelta.max.total_seconds() - 1)
            new_interval = min(self.timeout - int(run_duration()), delay_backoff_in_seconds)
            self.log.info("new %s interval is %s", self.mode, new_interval)
            return new_interval
        else:
            return self.poke_interval

    def prepare_for_execution(self) -> BaseOperator:
        task = super().prepare_for_execution()
        # Sensors in `poke` mode can block execution of DAGs when running
        # with single process executor, thus we change the mode to`reschedule`
        # to allow parallel task being scheduled and executed
        if conf.get('core', 'executor') == "DebugExecutor":
            self.log.warning("DebugExecutor changes sensor mode to 'reschedule'.")
            task.mode = 'reschedule'
        return task

    @property
    def reschedule(self):
        """Define mode rescheduled sensors."""
        return self.mode == 'reschedule'

    @property
    def deps(self):
        """
        Adds one additional dependency for all sensor operators that
        checks if a sensor task instance can be rescheduled.
        """
        if self.reschedule:
            return super().deps | {ReadyToRescheduleDep()}
        return super().deps


def poke_mode_only(cls):
    """
    Class Decorator for child classes of BaseSensorOperator to indicate
    that instances of this class are only safe to use poke mode.

    Will decorate all methods in the class to assert they did not change
    the mode from 'poke'.

    :param cls: BaseSensor class to enforce methods only use 'poke' mode.
    :type cls: type
    """

    def decorate(cls_type):
        def mode_getter(_):
            return 'poke'

        def mode_setter(_, value):
            if value != 'poke':
                raise ValueError("cannot set mode to 'poke'.")

        if not issubclass(cls_type, BaseSensorOperator):
            raise ValueError(
                f"poke_mode_only decorator should only be "
                f"applied to subclasses of BaseSensorOperator,"
                f" got:{cls_type}."
            )

        cls_type.mode = property(mode_getter, mode_setter)

        return cls_type

    return decorate(cls)


if 'BUILDING_AIRFLOW_DOCS' in os.environ:
    # flake8: noqa: F811
    # Monkey patch hook to get good function headers while building docs
    apply_defaults = lambda x: x
