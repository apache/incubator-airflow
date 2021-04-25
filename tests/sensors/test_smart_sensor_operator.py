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
import logging
import os
import time
import unittest
from unittest.mock import Mock

from freezegun import freeze_time

from airflow import DAG, settings
from airflow.configuration import conf
from airflow.models import DagRun, SensorInstance, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.smart_sensor import SmartSensorOperator
from airflow.utils import timezone
from airflow.utils.state import State

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
TEST_SENSOR_DAG_ID = 'unit_test_sensor_dag'
DUMMY_OP = 'dummy_op'
SMART_OP = 'smart_op'
SENSOR_OP = 'sensor_op'


class DummySmartSensor(SmartSensorOperator):
    def __init__(
        self, shard_max=conf.getint('smart_sensor', 'shard_code_upper_limit'), shard_min=0, **kwargs
    ):
        super().__init__(shard_min=shard_min, shard_max=shard_max, **kwargs)


class DummySensor(BaseSensorOperator):
    poke_context_fields = ('input_field', 'return_value')
    exec_fields = ('soft_fail', 'execution_timeout', 'timeout')

    def __init__(self, input_field='test', return_value=False, **kwargs):
        super().__init__(**kwargs)
        self.input_field = input_field
        self.return_value = return_value

    def poke(self, context):
        return context.get('return_value', False)

    def is_smart_sensor_compatible(self):
        return not self.on_failure_callback


class SmartSensorTest(unittest.TestCase):
    def setUp(self):
        os.environ['AIRFLOW__SMART_SENSOR__USE_SMART_SENSOR'] = 'true'
        os.environ['AIRFLOW__SMART_SENSOR__SENSORS_ENABLED'] = 'DummySmartSensor'

        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=args)
        self.sensor_dag = DAG(TEST_SENSOR_DAG_ID, default_args=args)
        self.log = logging.getLogger('BaseSmartTest')

        session = settings.Session()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        session.query(SensorInstance).delete()
        session.commit()

    def tearDown(self):
        session = settings.Session()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        session.query(SensorInstance).delete()
        session.commit()

        os.environ.pop('AIRFLOW__SMART_SENSOR__USE_SMART_SENSOR')
        os.environ.pop('AIRFLOW__SMART_SENSOR__SENSORS_ENABLED')

    def _make_dag_run(self):
        return self.dag.create_dagrun(
            run_id='manual__' + TEST_DAG_ID,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

    def _make_sensor_dag_run(self):
        return self.sensor_dag.create_dagrun(
            run_id='manual__' + TEST_SENSOR_DAG_ID,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

    def _make_sensor(self, return_value, **kwargs):
        poke_interval = 'poke_interval'
        timeout = 'timeout'
        if poke_interval not in kwargs:
            kwargs[poke_interval] = 0
        if timeout not in kwargs:
            kwargs[timeout] = 0

        sensor = DummySensor(task_id=SENSOR_OP, return_value=return_value, dag=self.sensor_dag, **kwargs)

        return sensor

    def _make_sensor_instance(self, index, return_value, **kwargs):
        poke_interval = 'poke_interval'
        timeout = 'timeout'
        if poke_interval not in kwargs:
            kwargs[poke_interval] = 0
        if timeout not in kwargs:
            kwargs[timeout] = 0

        task_id = SENSOR_OP + str(index)
        sensor = DummySensor(task_id=task_id, return_value=return_value, dag=self.sensor_dag, **kwargs)

        ti = TaskInstance(task=sensor, execution_date=DEFAULT_DATE)

        return ti

    def _make_smart_operator(self, index, **kwargs):
        poke_interval = 'poke_interval'
        smart_sensor_timeout = 'smart_sensor_timeout'
        if poke_interval not in kwargs:
            kwargs[poke_interval] = 0
        if smart_sensor_timeout not in kwargs:
            kwargs[smart_sensor_timeout] = 0

        smart_task = DummySmartSensor(task_id=SMART_OP + "_" + str(index), dag=self.dag, **kwargs)

        dummy_op = DummyOperator(task_id=DUMMY_OP, dag=self.dag)
        dummy_op.set_upstream(smart_task)
        return smart_task

    @classmethod
    def _run(cls, task):
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_load_sensor_works(self):
        # Mock two sensor tasks return True and one return False
        # The hashcode for si1 and si2 should be same. Test dedup on these two instances
        si1 = self._make_sensor_instance(1, True)
        si2 = self._make_sensor_instance(2, True)
        si3 = self._make_sensor_instance(3, False)

        # Confirm initial state
        smart = self._make_smart_operator(0)
        smart.flush_cached_sensor_poke_results()
        assert len(smart.cached_dedup_works) == 0
        assert len(smart.cached_sensor_exceptions) == 0

        si1.run(ignore_all_deps=True)
        # Test single sensor
        smart._load_sensor_works()
        assert len(smart.sensor_works) == 1
        assert len(smart.cached_dedup_works) == 0
        assert len(smart.cached_sensor_exceptions) == 0

        si2.run(ignore_all_deps=True)
        si3.run(ignore_all_deps=True)

        # Test multiple sensors with duplication
        smart._load_sensor_works()
        assert len(smart.sensor_works) == 3
        assert len(smart.cached_dedup_works) == 0
        assert len(smart.cached_sensor_exceptions) == 0

    def test_execute_single_task_with_dup(self):
        sensor_dr = self._make_sensor_dag_run()
        si1 = self._make_sensor_instance(1, True)
        si2 = self._make_sensor_instance(2, True)
        si3 = self._make_sensor_instance(3, False, timeout=0)

        si1.run(ignore_all_deps=True)
        si2.run(ignore_all_deps=True)
        si3.run(ignore_all_deps=True)

        smart = self._make_smart_operator(0)
        smart.flush_cached_sensor_poke_results()

        smart._load_sensor_works()
        assert len(smart.sensor_works) == 3

        for sensor_work in smart.sensor_works:
            _, task_id, _ = sensor_work.ti_key
            if task_id == SENSOR_OP + "1":
                smart._execute_sensor_work(sensor_work)
                break

        assert len(smart.cached_dedup_works) == 1

        tis = sensor_dr.get_task_instances()
        for ti in tis:
            if ti.task_id == SENSOR_OP + "1":
                assert ti.state == State.SUCCESS
            if ti.task_id == SENSOR_OP + "2":
                assert ti.state == State.SUCCESS
            if ti.task_id == SENSOR_OP + "3":
                assert ti.state == State.SENSING

        for sensor_work in smart.sensor_works:
            _, task_id, _ = sensor_work.ti_key
            if task_id == SENSOR_OP + "2":
                smart._execute_sensor_work(sensor_work)
                break

        assert len(smart.cached_dedup_works) == 1

        time.sleep(1)
        for sensor_work in smart.sensor_works:
            _, task_id, _ = sensor_work.ti_key
            if task_id == SENSOR_OP + "3":
                smart._execute_sensor_work(sensor_work)
                break

        assert len(smart.cached_dedup_works) == 2

        tis = sensor_dr.get_task_instances()
        for ti in tis:
            # Timeout=0, the Failed poke lead to task fail
            if ti.task_id == SENSOR_OP + "3":
                assert ti.state == State.FAILED

    def test_smart_operator_timeout(self):
        sensor_dr = self._make_sensor_dag_run()
        si1 = self._make_sensor_instance(1, False, timeout=10)
        smart = self._make_smart_operator(0, poke_interval=6)
        smart.poke = Mock(side_effect=[False, False, False, False])

        date1 = timezone.utcnow()
        with freeze_time(date1):
            si1.run(ignore_all_deps=True)
            smart.flush_cached_sensor_poke_results()
            smart._load_sensor_works()

            for sensor_work in smart.sensor_works:
                smart._execute_sensor_work(sensor_work)

        # Before timeout the state should be SENSING
        sis = sensor_dr.get_task_instances()
        for sensor_instance in sis:
            if sensor_instance.task_id == SENSOR_OP + "1":
                assert sensor_instance.state == State.SENSING

        date2 = date1 + datetime.timedelta(seconds=smart.poke_interval)
        with freeze_time(date2):
            smart.flush_cached_sensor_poke_results()
            smart._load_sensor_works()

            for sensor_work in smart.sensor_works:
                smart._execute_sensor_work(sensor_work)

        sis = sensor_dr.get_task_instances()
        for sensor_instance in sis:
            if sensor_instance.task_id == SENSOR_OP + "1":
                assert sensor_instance.state == State.SENSING

        date3 = date2 + datetime.timedelta(seconds=smart.poke_interval)
        with freeze_time(date3):
            smart.flush_cached_sensor_poke_results()
            smart._load_sensor_works()

            for sensor_work in smart.sensor_works:
                smart._execute_sensor_work(sensor_work)

        sis = sensor_dr.get_task_instances()
        for sensor_instance in sis:
            if sensor_instance.task_id == SENSOR_OP + "1":
                assert sensor_instance.state == State.FAILED

    def test_register_in_sensor_service(self):
        si1 = self._make_sensor_instance(1, True)
        si1.run(ignore_all_deps=True)
        assert si1.state == State.SENSING

        session = settings.Session()

        SI = SensorInstance
        sensor_instance = (
            session.query(SI)
            .filter(
                SI.dag_id == si1.dag_id, SI.task_id == si1.task_id, SI.execution_date == si1.execution_date
            )
            .first()
        )

        assert sensor_instance is not None
        assert sensor_instance.state == State.SENSING
        assert sensor_instance.operator == "DummySensor"
