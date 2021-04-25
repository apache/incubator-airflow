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

import datetime as dt
import getpass
import unittest

import pytest
from marshmallow import ValidationError
from parameterized import parameterized

from airflow.api_connexion.schemas.task_instance_schema import (
    clear_task_instance_form,
    set_task_instance_state_form,
    task_instance_schema,
)
from airflow.models import DAG, SlaMiss, TaskInstance as TI
from airflow.operators.dummy import DummyOperator
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime


class TestTaskInstanceSchema(unittest.TestCase):
    def setUp(self):
        self.default_time = datetime(2020, 1, 1)
        with DAG(dag_id="TEST_DAG_ID"):
            self.task = DummyOperator(task_id="TEST_TASK_ID", start_date=self.default_time)

        self.default_ti_init = {
            "execution_date": self.default_time,
            "state": State.RUNNING,
        }
        self.default_ti_extras = {
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
        }

    def tearDown(self):
        with create_session() as session:
            session.query(TI).delete()
            session.query(SlaMiss).delete()

    @provide_session
    def test_task_instance_schema_without_sla(self, session):
        ti = TI(task=self.task, **self.default_ti_init)
        for key, value in self.default_ti_extras.items():
            setattr(ti, key, value)
        session.add(ti)
        session.commit()
        serialized_ti = task_instance_schema.dump((ti, None))
        expected_json = {
            "dag_id": "TEST_DAG_ID",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "max_tries": 0,
            "operator": "DummyOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "TEST_TASK_ID",
            "try_number": 0,
            "unixname": getpass.getuser(),
        }
        assert serialized_ti == expected_json

    @provide_session
    def test_task_instance_schema_with_sla(self, session):
        ti = TI(task=self.task, **self.default_ti_init)
        for key, value in self.default_ti_extras.items():
            setattr(ti, key, value)
        sla_miss = SlaMiss(
            task_id="TEST_TASK_ID",
            dag_id="TEST_DAG_ID",
            execution_date=self.default_time,
        )
        session.add(ti)
        session.add(sla_miss)
        session.commit()
        serialized_ti = task_instance_schema.dump((ti, sla_miss))
        expected_json = {
            "dag_id": "TEST_DAG_ID",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "max_tries": 0,
            "operator": "DummyOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": {
                "dag_id": "TEST_DAG_ID",
                "description": None,
                "email_sent": False,
                "execution_date": "2020-01-01T00:00:00+00:00",
                "notification_sent": False,
                "task_id": "TEST_TASK_ID",
                "timestamp": None,
            },
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "TEST_TASK_ID",
            "try_number": 0,
            "unixname": getpass.getuser(),
        }
        assert serialized_ti == expected_json


class TestClearTaskInstanceFormSchema(unittest.TestCase):
    @parameterized.expand(
        [
            (
                [
                    {
                        "dry_run": False,
                        "reset_dag_runs": True,
                        "only_failed": True,
                        "only_running": True,
                    }
                ]
            ),
            (
                [
                    {
                        "dry_run": False,
                        "reset_dag_runs": True,
                        "end_date": "2020-01-01T00:00:00+00:00",
                        "start_date": "2020-01-02T00:00:00+00:00",
                    }
                ]
            ),
        ]
    )
    def test_validation_error(self, payload):
        with pytest.raises(ValidationError):
            clear_task_instance_form.load(payload)


class TestSetTaskInstanceStateFormSchema(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.current_input = {
            "dry_run": True,
            "task_id": "print_the_context",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "include_upstream": True,
            "include_downstream": True,
            "include_future": True,
            "include_past": True,
            "new_state": "failed",
        }

    def test_success(self):
        result = set_task_instance_state_form.load(self.current_input)
        expected_result = {
            'dry_run': True,
            'execution_date': dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone(dt.timedelta(0), '+0000')),
            'include_downstream': True,
            'include_future': True,
            'include_past': True,
            'include_upstream': True,
            'new_state': 'failed',
            'task_id': 'print_the_context',
        }
        assert expected_result == result

    @parameterized.expand(
        [
            ({"task_id": None},),
            ({"include_future": "foo"},),
            ({"execution_date": "NOW"},),
            ({"new_state": "INVALID_STATE"},),
        ]
    )
    def test_validation_error(self, override_data):
        self.current_input.update(override_data)

        with pytest.raises(ValidationError):
            set_task_instance_state_form.load(self.current_input)
