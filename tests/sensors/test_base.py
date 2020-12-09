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

import unittest
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest
from freezegun import freeze_time

from airflow.exceptions import AirflowException, AirflowRescheduleException, AirflowSensorTimeout
from airflow.models import DagBag, TaskInstance, TaskReschedule
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator, poke_mode_only
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils import db

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
DUMMY_OP = 'dummy_op'
SENSOR_OP = 'sensor_op'
DEV_NULL = 'dev/null'


class DummySensor(BaseSensorOperator):
    def __init__(self, return_value=False, **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def poke(self, context):
        return self.return_value


class TestBaseSensor(unittest.TestCase):
    @staticmethod
    def clean_db():
        db.clear_db_runs()
        db.clear_db_task_reschedule()
        db.clear_db_xcom()

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=args)
        self.clean_db()

    def tearDown(self) -> None:
        self.clean_db()

    def _make_dag_run(self):
        return self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

    def _make_sensor(self, return_value, task_id=SENSOR_OP, **kwargs):
        poke_interval = 'poke_interval'
        timeout = 'timeout'

        if poke_interval not in kwargs:
            kwargs[poke_interval] = 0
        if timeout not in kwargs:
            kwargs[timeout] = 0

        sensor = DummySensor(task_id=task_id, return_value=return_value, dag=self.dag, **kwargs)

        dummy_op = DummyOperator(task_id=DUMMY_OP, dag=self.dag)
        dummy_op.set_upstream(sensor)
        return sensor

    @classmethod
    def _run(cls, task):
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_ok(self):
        sensor = self._make_sensor(True)
        dr = self._make_dag_run()

        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_fail(self):
        sensor = self._make_sensor(False)
        dr = self._make_dag_run()

        with pytest.raises(AirflowSensorTimeout):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_soft_fail(self):
        sensor = self._make_sensor(False, soft_fail=True)
        dr = self._make_dag_run()

        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SKIPPED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_soft_fail_with_retries(self):
        sensor = self._make_sensor(
            return_value=False, soft_fail=True, retries=1, retry_delay=timedelta(milliseconds=1)
        )
        dr = self._make_dag_run()

        # first run times out and task instance is skipped
        self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SKIPPED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_ok_with_reschedule(self):
        sensor = self._make_sensor(return_value=None, poke_interval=10, timeout=25, mode='reschedule')
        sensor.poke = Mock(side_effect=[False, False, True])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify task start date is the initial one
                assert ti.start_date == date1
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date1
                assert task_reschedules[0].reschedule_date == date1 + timedelta(seconds=sensor.poke_interval)
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False and task is re-scheduled
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify task start date is the initial one
                assert ti.start_date == date1
                # verify two rows in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 2
                assert task_reschedules[1].start_date == date2
                assert task_reschedules[1].reschedule_date == date2 + timedelta(seconds=sensor.poke_interval)
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # third poke returns True and task succeeds
        date3 = date2 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
                # verify task start date is the initial one
                assert ti.start_date == date1
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_fail_with_reschedule(self):
        sensor = self._make_sensor(return_value=False, poke_interval=10, timeout=5, mode='reschedule')
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False, timeout occurs
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with pytest.raises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_soft_fail_with_reschedule(self):
        sensor = self._make_sensor(
            return_value=False, poke_interval=10, timeout=5, soft_fail=True, mode='reschedule'
        )
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False, timeout occurs
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SKIPPED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_ok_with_reschedule_and_retry(self):
        sensor = self._make_sensor(
            return_value=None,
            poke_interval=10,
            timeout=5,
            retries=1,
            retry_delay=timedelta(seconds=10),
            mode='reschedule',
        )
        sensor.poke = Mock(side_effect=[False, False, False, True])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date1
                assert task_reschedules[0].reschedule_date == date1 + timedelta(seconds=sensor.poke_interval)
                assert task_reschedules[0].try_number == 1
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke timesout and task instance is failed
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with pytest.raises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.FAILED
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # third poke returns False and task is rescheduled again
        date3 = date2 + timedelta(seconds=sensor.poke_interval) + sensor.retry_delay
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date3
                assert task_reschedules[0].reschedule_date == date3 + timedelta(seconds=sensor.poke_interval)
                assert task_reschedules[0].try_number == 2
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # fourth poke return True and task succeeds
        date4 = date3 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date4):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_should_include_ready_to_reschedule_dep_in_reschedule_mode(self):
        sensor = self._make_sensor(True, mode='reschedule')
        deps = sensor.deps
        assert ReadyToRescheduleDep() in deps

    def test_should_not_include_ready_to_reschedule_dep_in_poke_mode(self):
        sensor = self._make_sensor(True)
        deps = sensor.deps
        assert ReadyToRescheduleDep() not in deps

    def test_invalid_mode(self):
        with pytest.raises(AirflowException):
            self._make_sensor(return_value=True, mode='foo')

    def test_ok_with_custom_reschedule_exception(self):
        sensor = self._make_sensor(return_value=None, mode='reschedule')
        date1 = timezone.utcnow()
        date2 = date1 + timedelta(seconds=60)
        date3 = date1 + timedelta(seconds=120)
        sensor.poke = Mock(
            side_effect=[
                AirflowRescheduleException(date2),
                AirflowRescheduleException(date3),
                True,
            ]
        )
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 1
                assert task_reschedules[0].start_date == date1
                assert task_reschedules[0].reschedule_date == date2
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # second poke returns False and task is re-scheduled
        with freeze_time(date2):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # verify task is re-scheduled, i.e. state set to NONE
                assert ti.state == State.UP_FOR_RESCHEDULE
                # verify two rows in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 2
                assert task_reschedules[1].start_date == date2
                assert task_reschedules[1].reschedule_date == date3
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

        # third poke returns True and task succeeds
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                assert ti.state == State.SUCCESS
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_reschedule_with_test_mode(self):
        sensor = self._make_sensor(return_value=None, poke_interval=10, timeout=25, mode='reschedule')
        sensor.poke = Mock(side_effect=[False])
        dr = self._make_dag_run()

        # poke returns False and AirflowRescheduleException is raised
        date1 = timezone.utcnow()
        with freeze_time(date1):
            for date in self.dag.date_range(DEFAULT_DATE, end_date=DEFAULT_DATE):
                TaskInstance(sensor, date).run(ignore_ti_state=True, test_mode=True)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                # in test mode state is not modified
                assert ti.state == State.NONE
                # in test mode no reschedule request is recorded
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                assert len(task_reschedules) == 0
            if ti.task_id == DUMMY_OP:
                assert ti.state == State.NONE

    def test_sensor_with_invalid_poke_interval(self):
        negative_poke_interval = -10
        non_number_poke_interval = "abcd"
        positive_poke_interval = 10
        with pytest.raises(AirflowException):
            self._make_sensor(
                task_id='test_sensor_task_1',
                return_value=None,
                poke_interval=negative_poke_interval,
                timeout=25,
            )

        with pytest.raises(AirflowException):
            self._make_sensor(
                task_id='test_sensor_task_2',
                return_value=None,
                poke_interval=non_number_poke_interval,
                timeout=25,
            )

        self._make_sensor(
            task_id='test_sensor_task_3', return_value=None, poke_interval=positive_poke_interval, timeout=25
        )

    def test_sensor_with_invalid_timeout(self):
        negative_timeout = -25
        non_number_timeout = "abcd"
        positive_timeout = 25
        with pytest.raises(AirflowException):
            self._make_sensor(
                task_id='test_sensor_task_1', return_value=None, poke_interval=10, timeout=negative_timeout
            )

        with pytest.raises(AirflowException):
            self._make_sensor(
                task_id='test_sensor_task_2', return_value=None, poke_interval=10, timeout=non_number_timeout
            )

        self._make_sensor(
            task_id='test_sensor_task_3', return_value=None, poke_interval=10, timeout=positive_timeout
        )

    def test_sensor_with_exponential_backoff_off(self):
        sensor = self._make_sensor(return_value=None, poke_interval=5, timeout=60, exponential_backoff=False)

        started_at = timezone.utcnow() - timedelta(seconds=10)

        def run_duration():
            return (timezone.utcnow - started_at).total_seconds()

        assert sensor._get_next_poke_interval(started_at, run_duration, 1) == sensor.poke_interval
        assert sensor._get_next_poke_interval(started_at, run_duration, 2) == sensor.poke_interval

    def test_sensor_with_exponential_backoff_on(self):

        sensor = self._make_sensor(return_value=None, poke_interval=5, timeout=60, exponential_backoff=True)

        with patch('airflow.utils.timezone.utcnow') as mock_utctime:
            mock_utctime.return_value = DEFAULT_DATE

            started_at = timezone.utcnow() - timedelta(seconds=10)

            def run_duration():
                return (timezone.utcnow - started_at).total_seconds()

            interval1 = sensor._get_next_poke_interval(started_at, run_duration, 1)
            interval2 = sensor._get_next_poke_interval(started_at, run_duration, 2)

            assert interval1 >= 0
            assert interval1 <= sensor.poke_interval
            assert interval2 >= sensor.poke_interval
            assert interval2 > interval1

    def test_reschedule_and_retry_fail(self):
        """
        poke_interval=5
        timeout=10
        retries=2
        retry_delay=timedelta(seconds=3)

        Actual behaviour:
        00:00 False, try_number=1, max_retries=2
        00:05 RuntimeError
        00:08 False, try_number=2, max_retries=2
        00:13 False, try_number=2, max_retries=2
        00:18 AirflowSensorTimeout, try_number=2, max_retries=2

        Expected behaviour:
        00:00 False, try_number=1, max_retries=2
        00:05 RuntimeError
        00:08 False, try_number=2, max_retries=2
        00:13 AirflowSensorTimeout, try_number=2, max_retries=2

        Expected behaviour with clear:
        00:00 False, try_number=1, max_retries=2
        00:05 RuntimeError
        00:08 False, try_number=2, max_retries=2
        00:13 AirflowSensorTimeout, try_number=2, max_retries=2
        00:18 Clear (deleted reschedules)
        00:18 False, try_number=2, max_retries=3
        00:23 False, try_number=2, max_retries=3
        00:25 False, try_number=2, max_retries=3
        00:30 AirflowSensorTimeout, try_number=2, max_retries=3
        """
        sensor = self._make_sensor(
            return_value=None,
            poke_interval=5,
            timeout=10,
            retries=2,
            retry_delay=timedelta(seconds=3),
            mode='reschedule',
        )

        sensor.poke = Mock(side_effect=[False, RuntimeError, False, False, False, False, False, False])
        dr = self._make_dag_run()

        # first poke returns False and task is re-scheduled
        date1 = timezone.utcnow()
        with freeze_time(date1):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEqual(ti.state, State.UP_FOR_RESCHEDULE)
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEqual(len(task_reschedules), 1)
                self.assertEqual(task_reschedules[0].start_date, date1)
                self.assertEqual(
                    task_reschedules[0].reschedule_date, date1 + timedelta(seconds=sensor.poke_interval)
                )
                self.assertEqual(task_reschedules[0].try_number, 1)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

        # second poke raises RuntimeError and task instance retries
        date2 = date1 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date2):
            with self.assertRaises(RuntimeError):
                self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEqual(ti.state, State.UP_FOR_RETRY)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

        # third poke returns False and task is rescheduled again
        date3 = date2 + timedelta(seconds=sensor.poke_interval) + sensor.retry_delay
        with freeze_time(date3):
            self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEqual(ti.state, State.UP_FOR_RESCHEDULE)
                # verify one row in task_reschedule table
                task_reschedules = TaskReschedule.find_for_task_instance(ti)
                self.assertEqual(len(task_reschedules), 1)
                self.assertEqual(task_reschedules[0].start_date, date3)
                self.assertEqual(
                    task_reschedules[0].reschedule_date, date3 + timedelta(seconds=sensor.poke_interval)
                )
                self.assertEqual(task_reschedules[0].try_number, 2)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

        # fourth poke times out and raises AirflowSensorTimeout
        date4 = date3 + timedelta(seconds=sensor.poke_interval)
        with freeze_time(date4):
            with self.assertRaises(AirflowSensorTimeout):
                self._run(sensor)
        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)
        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEqual(ti.state, State.FAILED)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

        # Clear the failed sensor
        sensor.clear()

        date5 = date4 + timedelta(seconds=20)

        with freeze_time(date5):
            self._run(sensor)

        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEqual(ti.state, State.UP_FOR_RESCHEDULE)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)

        date6 = date5 + timedelta(seconds=sensor.poke_interval)

        with freeze_time(date6):
            self._run(sensor)

        date7 = date6 + timedelta(seconds=sensor.poke_interval)

        with freeze_time(date7):
            self._run(sensor)

        date8 = date8 + timedelta(seconds=sensor.poke_interval)

        with freeze_time(date8):
            with self.assertRaises(AirflowSensorTimeout):
                self._run(sensor)

        for ti in tis:
            if ti.task_id == SENSOR_OP:
                self.assertEqual(ti.state, State.FAILED)
            if ti.task_id == DUMMY_OP:
                self.assertEqual(ti.state, State.NONE)


@poke_mode_only
class DummyPokeOnlySensor(BaseSensorOperator):
    def __init__(self, poke_changes_mode=False, **kwargs):
        self.mode = kwargs['mode']
        super().__init__(**kwargs)
        self.poke_changes_mode = poke_changes_mode
        self.return_value = True

    def poke(self, context):
        if self.poke_changes_mode:
            self.change_mode('reschedule')
        return self.return_value

    def change_mode(self, mode):
        self.mode = mode


class TestPokeModeOnly(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

    def test_poke_mode_only_allows_poke_mode(self):
        try:
            sensor = DummyPokeOnlySensor(task_id='foo', mode='poke', poke_changes_mode=False, dag=self.dag)
        except ValueError:
            self.fail("__init__ failed with mode='poke'.")
        try:
            sensor.poke({})
        except ValueError:
            self.fail("poke failed without changing mode from 'poke'.")
        try:
            sensor.change_mode('poke')
        except ValueError:
            self.fail("class method failed without changing mode from 'poke'.")

    def test_poke_mode_only_bad_class_method(self):
        sensor = DummyPokeOnlySensor(task_id='foo', mode='poke', poke_changes_mode=False, dag=self.dag)
        with pytest.raises(ValueError):
            sensor.change_mode('reschedule')

    def test_poke_mode_only_bad_init(self):
        with pytest.raises(ValueError):
            DummyPokeOnlySensor(task_id='foo', mode='reschedule', poke_changes_mode=False, dag=self.dag)

    def test_poke_mode_only_bad_poke(self):
        sensor = DummyPokeOnlySensor(task_id='foo', mode='poke', poke_changes_mode=True, dag=self.dag)
        with pytest.raises(ValueError):
            sensor.poke({})
