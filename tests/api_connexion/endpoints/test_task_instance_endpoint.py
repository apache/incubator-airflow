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
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.models import DagBag, DagRun, SlaMiss, TaskInstance
from airflow.security import permissions
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_runs, clear_db_sla_miss

DEFAULT_DATETIME_1 = datetime(2020, 1, 1)
DEFAULT_DATETIME_STR_1 = "2020-01-01T00:00:00+00:00"
DEFAULT_DATETIME_STR_2 = "2020-01-02T00:00:00+00:00"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore


class TestTaskInstanceEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.default_time = DEFAULT_DATETIME_1
        self.ti_init = {
            "execution_date": self.default_time,
            "state": State.RUNNING,
        }
        self.ti_extras = {
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
            "job_id": 0,
        }
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_runs()
        clear_db_sla_miss()
        DagBag(include_examples=True, read_dags_from_db=False).sync_to_db()
        self.dagbag = DagBag(include_examples=True, read_dags_from_db=True)

    def create_task_instances(
        self,
        session,
        dag_id: str = "example_python_operator",
        update_extras: bool = True,
        single_dag_run: bool = True,
        task_instances=None,
        dag_run_state=State.RUNNING,
    ):
        """Method to create task instances using kwargs and default arguments"""

        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        counter = len(tasks)
        if task_instances is not None:
            counter = min(len(task_instances), counter)

        for i in range(counter):
            if task_instances is None:
                pass
            elif update_extras:
                self.ti_extras.update(task_instances[i])
            else:
                self.ti_init.update(task_instances[i])
            ti = TaskInstance(task=tasks[i], **self.ti_init)

            for key, value in self.ti_extras.items():
                setattr(ti, key, value)
            session.add(ti)

            if single_dag_run is False:
                dr = DagRun(
                    dag_id=dag_id,
                    run_id=f"TEST_DAG_RUN_ID_{i}",
                    execution_date=self.ti_init["execution_date"],
                    run_type=DagRunType.MANUAL.value,
                    state=dag_run_state,
                )
                session.add(dr)

        if single_dag_run:
            dr = DagRun(
                dag_id=dag_id,
                run_id="TEST_DAG_RUN_ID",
                execution_date=self.default_time,
                run_type=DagRunType.MANUAL.value,
                state=dag_run_state,
            )
            session.add(dr)
        session.commit()


class TestGetTaskInstance(TestTaskInstanceEndpoint):
    def test_should_respond_200(self, session):
        self.create_task_instances(session)
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "max_tries": 0,
            "operator": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 6,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getpass.getuser(),
        }

    def test_should_respond_200_with_task_state_in_removed(self, session):
        self.create_task_instances(session, task_instances=[{"state": State.REMOVED}], update_extras=True)
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "max_tries": 0,
            "operator": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 6,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "removed",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getpass.getuser(),
        }

    def test_should_respond_200_task_instance_with_sla(self, session):
        self.create_task_instances(session)
        session.query()
        sla_miss = SlaMiss(
            task_id="print_the_context",
            dag_id="example_python_operator",
            execution_date=self.default_time,
            timestamp=self.default_time,
        )
        session.add(sla_miss)
        session.commit()
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200

        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "max_tries": 0,
            "operator": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 6,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": {
                "dag_id": "example_python_operator",
                "description": None,
                "email_sent": False,
                "execution_date": "2020-01-01T00:00:00+00:00",
                "notification_sent": False,
                "task_id": "print_the_context",
                "timestamp": "2020-01-01T00:00:00+00:00",
            },
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getpass.getuser(),
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(
            "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403


class TestGetTaskInstances(TestTaskInstanceEndpoint):
    @parameterized.expand(
        [
            (
                "test execution date filter",
                [
                    {"execution_date": DEFAULT_DATETIME_1},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                False,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/"
                    f"taskInstances?execution_date_lte={DEFAULT_DATETIME_STR_1}"
                ),
                1,
            ),
            (
                "test start date filter",
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                    f"?start_date_gte={DEFAULT_DATETIME_STR_1}&start_date_lte={DEFAULT_DATETIME_STR_2}"
                ),
                2,
            ),
            (
                "test start date filter with ~",
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                (
                    "/api/v1/dags/~/dagRuns/~/taskInstances?start_date_gte"
                    f"={DEFAULT_DATETIME_STR_1}&start_date_lte={DEFAULT_DATETIME_STR_2}"
                ),
                2,
            ),
            (
                "test end date filter",
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances?"
                    f"end_date_gte={DEFAULT_DATETIME_STR_1}&end_date_lte={DEFAULT_DATETIME_STR_2}"
                ),
                2,
            ),
            (
                "test end date filter ~",
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                (
                    "/api/v1/dags/~/dagRuns/~/taskInstances?end_date_gte"
                    f"={DEFAULT_DATETIME_STR_1}&end_date_lte={DEFAULT_DATETIME_STR_2}"
                ),
                2,
            ),
            (
                "test duration filter",
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/"
                    "taskInstances?duration_gte=100&duration_lte=200"
                ),
                3,
            ),
            (
                "test duration filter ~",
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                "/api/v1/dags/~/dagRuns/~/taskInstances?duration_gte=100&duration_lte=200",
                3,
            ),
            (
                "test state filter",
                [
                    {"state": State.RUNNING},
                    {"state": State.QUEUED},
                    {"state": State.SUCCESS},
                ],
                False,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/"
                    "TEST_DAG_RUN_ID/taskInstances?state=running,queued"
                ),
                2,
            ),
            (
                "test pool filter",
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/"
                    "TEST_DAG_RUN_ID/taskInstances?pool=test_pool_1,test_pool_2"
                ),
                2,
            ),
            (
                "test pool filter ~",
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                "/api/v1/dags/~/dagRuns/~/taskInstances?pool=test_pool_1,test_pool_2",
                2,
            ),
            (
                "test queue filter",
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID"
                    "/taskInstances?queue=test_queue_1,test_queue_2"
                ),
                2,
            ),
            (
                "test queue filter ~",
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                "/api/v1/dags/~/dagRuns/~/taskInstances?queue=test_queue_1,test_queue_2",
                2,
            ),
        ]
    )
    @provide_session
    def test_should_respond_200(self, _, task_instances, update_extras, url, expected_ti, session):
        self.create_task_instances(
            session,
            update_extras=update_extras,
            task_instances=task_instances,
        )
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == expected_ti
        assert len(response.json["task_instances"]) == expected_ti

    def test_should_respond_200_for_dag_id_filter(self, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        count = session.query(TaskInstance).filter(TaskInstance.dag_id == "example_python_operator").count()
        assert count == response.json["total_entries"]
        assert count == len(response.json["task_instances"])

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances",
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403


class TestGetTaskInstancesBatch(TestTaskInstanceEndpoint):
    @parameterized.expand(
        [
            (
                "test queue filter",
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                True,
                {"queue": ["test_queue_1", "test_queue_2"]},
                2,
            ),
            (
                "test pool filter",
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                True,
                {"pool": ["test_pool_1", "test_pool_2"]},
                2,
            ),
            (
                "test state filter",
                [
                    {"state": State.RUNNING},
                    {"state": State.QUEUED},
                    {"state": State.SUCCESS},
                ],
                False,
                True,
                {"state": ["running", "queued"]},
                2,
            ),
            (
                "test duration filter",
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                True,
                {"duration_gte": 100, "duration_lte": 200},
                3,
            ),
            (
                "test end date filter",
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                True,
                {
                    "end_date_gte": DEFAULT_DATETIME_STR_1,
                    "end_date_lte": DEFAULT_DATETIME_STR_2,
                },
                2,
            ),
            (
                "test start date filter",
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                True,
                {
                    "start_date_gte": DEFAULT_DATETIME_STR_1,
                    "start_date_lte": DEFAULT_DATETIME_STR_2,
                },
                2,
            ),
            (
                "with execution date filter",
                [
                    {"execution_date": DEFAULT_DATETIME_1},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5)},
                ],
                False,
                True,
                {
                    "execution_date_gte": DEFAULT_DATETIME_1,
                    "execution_date_lte": (DEFAULT_DATETIME_1 + dt.timedelta(days=2)),
                },
                3,
            ),
        ]
    )
    @provide_session
    def test_should_respond_200(
        self, _, task_instances, update_extras, single_dag_run, payload, expected_ti_count, session
    ):
        self.create_task_instances(
            session,
            update_extras=update_extras,
            task_instances=task_instances,
            single_dag_run=single_dag_run,
        )
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200, response.json
        assert expected_ti_count == response.json["total_entries"]
        assert expected_ti_count == len(response.json["task_instances"])

    @parameterized.expand(
        [
            (
                "task_instance properties",
                [
                    {"task": "test_1"},
                    {"task": "test_2"},
                ],
                True,
                {"dag_ids": ["latest_only"]},
                2,
            ),
        ],
    )
    @provide_session
    def test_should_respond_200_when_task_instance_properties_are_none(
        self, _, task_instances, single_dag_run, payload, expected_ti_count, session
    ):
        self.ti_extras.update(
            {
                "start_date": None,
                "end_date": None,
                "state": None,
            }
        )
        self.create_task_instances(
            session,
            dag_id="latest_only",
            task_instances=task_instances,
            single_dag_run=single_dag_run,
        )
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200, response.json
        assert expected_ti_count == response.json["total_entries"]
        assert expected_ti_count == len(response.json["task_instances"])

    @parameterized.expand(
        [
            (
                "with dag filter",
                {"dag_ids": ["example_python_operator", "example_skip_dag"]},
                15,
                15,
            ),
        ],
    )
    @provide_session
    def test_should_respond_200_dag_ids_filter(self, _, payload, expected_ti, total_ti, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200
        assert len(response.json["task_instances"]) == expected_ti
        assert response.json["total_entries"] == total_ti

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            json={"dag_ids": ["example_python_operator", "example_skip_dag"]},
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
            json={"dag_ids": ["example_python_operator", "example_skip_dag"]},
        )
        assert response.status_code == 403

    @parameterized.expand(
        [
            ({"end_date_lte": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"end_date_gte": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"start_date_lte": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"start_date_gte": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"execution_date_gte": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"execution_date_lte": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
        ]
    )
    @provide_session
    def test_should_raise_400_for_naive_and_bad_datetime(self, payload, expected, session):
        self.create_task_instances(session)
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={'REMOTE_USER': "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected


class TestPostClearTaskInstances(TestTaskInstanceEndpoint):
    @parameterized.expand(
        [
            (
                "clear start date filter",
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "start_date": DEFAULT_DATETIME_STR_2,
                    "only_failed": True,
                },
                2,
            ),
            (
                "clear end date filter",
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "end_date": DEFAULT_DATETIME_STR_2,
                    "only_failed": True,
                },
                2,
            ),
            (
                "clear only running",
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.RUNNING,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {"dry_run": True, "only_running": True, "only_failed": False},
                2,
            ),
            (
                "clear only failed",
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.RUNNING,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "only_failed": True,
                },
                2,
            ),
            (
                "include parent dag",
                "example_subdag_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                        "state": State.FAILED,
                    },
                ],
                "example_subdag_operator.section-1",
                {"dry_run": True, "include_parentdag": True},
                4,
            ),
            (
                "include sub dag",
                "example_subdag_operator.section-1",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                ],
                "example_subdag_operator",
                {
                    "dry_run": True,
                    "include_subdags": True,
                },
                4,
            ),
        ]
    )
    @provide_session
    def test_should_respond_200(
        self, _, main_dag, task_instances, request_dag, payload, expected_ti, session
    ):
        self.create_task_instances(
            session,
            dag_id=main_dag,
            task_instances=task_instances,
            update_extras=False,
            single_dag_run=False,
        )
        self.app.dag_bag.sync_to_db()  # pylint: disable=no-member
        response = self.client.post(
            f"/api/v1/dags/{request_dag}/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200
        assert len(response.json["task_instances"]) == expected_ti

    def test_should_respond_200_with_reset_dag_run(self, session):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": True,
            "only_failed": False,
            "only_running": True,
            "include_subdags": True,
        }
        task_instances = [
            {"execution_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                "state": State.RUNNING,
            },
        ]

        self.create_task_instances(
            session,
            dag_id=dag_id,
            single_dag_run=False,
            task_instances=task_instances,
            update_extras=False,
            dag_run_state=State.FAILED,
        )
        response = self.client.post(
            f"/api/v1/dags/{dag_id}/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )

        failed_dag_runs = (
            session.query(DagRun).filter(DagRun.state == "failed").count()  # pylint: disable=W0143
        )
        assert 200 == response.status_code
        expected_response = [
            {
                'dag_id': 'example_python_operator',
                'dag_run_id': 'TEST_DAG_RUN_ID_0',
                'execution_date': '2020-01-01T00:00:00+00:00',
                'task_id': 'print_the_context',
            },
            {
                'dag_id': 'example_python_operator',
                'dag_run_id': 'TEST_DAG_RUN_ID_1',
                'execution_date': '2020-01-02T00:00:00+00:00',
                'task_id': 'sleep_for_0',
            },
            {
                'dag_id': 'example_python_operator',
                'dag_run_id': 'TEST_DAG_RUN_ID_2',
                'execution_date': '2020-01-03T00:00:00+00:00',
                'task_id': 'sleep_for_1',
            },
            {
                'dag_id': 'example_python_operator',
                'dag_run_id': 'TEST_DAG_RUN_ID_3',
                'execution_date': '2020-01-04T00:00:00+00:00',
                'task_id': 'sleep_for_2',
            },
            {
                'dag_id': 'example_python_operator',
                'dag_run_id': 'TEST_DAG_RUN_ID_4',
                'execution_date': '2020-01-05T00:00:00+00:00',
                'task_id': 'sleep_for_3',
            },
        ]
        for task_instance in expected_response:
            assert task_instance in response.json["task_instances"]
        assert 5 == len(response.json["task_instances"])
        assert 0 == failed_dag_runs, 0

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/clearTaskInstances",
            json={
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False,
                "only_running": True,
                "include_subdags": True,
            },
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/clearTaskInstances",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
            json={
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False,
                "only_running": True,
                "include_subdags": True,
            },
        )
        assert response.status_code == 403

    @parameterized.expand(
        [
            ({"end_date": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"end_date": '2020-11-10T12:4po'}, "{'end_date': ['Not a valid datetime.']}"),
            ({"start_date": '2020-11-10T12:42:39.442973'}, "Naive datetime is disallowed"),
            ({"start_date": '2020-11-10T12:4po'}, "{'start_date': ['Not a valid datetime.']}"),
        ]
    )
    @provide_session
    def test_should_raise_400_for_naive_and_bad_datetime(self, payload, expected, session):
        task_instances = [
            {"execution_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
        ]
        self.create_task_instances(
            session,
            dag_id="example_python_operator",
            task_instances=task_instances,
            update_extras=False,
            single_dag_run=False,
        )
        self.app.dag_bag.sync_to_db()  # pylint: disable=no-member
        response = self.client.post(
            "/api/v1/dags/example_python_operator/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected


class TestPostSetTaskInstanceState(TestTaskInstanceEndpoint):
    @mock.patch('airflow.api_connexion.endpoints.task_instance_endpoint.set_state')
    def test_should_assert_call_mocked_api(self, mock_set_state, session):
        self.create_task_instances(session)
        mock_set_state.return_value = (
            session.query(TaskInstance).filter(TaskInstance.task_id == "print_the_context").all()
        )
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={'REMOTE_USER': "test"},
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "execution_date": DEFAULT_DATETIME_1,
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 200
        assert response.json == {
            'task_instances': [
                {
                    'dag_id': 'example_python_operator',
                    'dag_run_id': 'TEST_DAG_RUN_ID',
                    'execution_date': '2020-01-01T00:00:00+00:00',
                    'task_id': 'print_the_context',
                }
            ]
        }

        dag = self.app.dag_bag.dags['example_python_operator']  # pylint: disable=no-member
        task = dag.task_dict['print_the_context']
        mock_set_state.assert_called_once_with(
            commit=False,
            downstream=True,
            execution_date=DEFAULT_DATETIME_1,
            future=True,
            past=True,
            state='failed',
            tasks=[task],
            upstream=True,
        )

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "execution_date": DEFAULT_DATETIME_1,
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "execution_date": DEFAULT_DATETIME_1,
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 403

    def test_should_raise_404_not_found_dag(self):
        response = self.client.post(
            "/api/v1/dags/INVALID_DAG/updateTaskInstancesState",
            environ_overrides={'REMOTE_USER': "test"},
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "execution_date": DEFAULT_DATETIME_1,
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 404

    def test_should_raise_404_not_found_task(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={'REMOTE_USER': "test"},
            json={
                "dry_run": True,
                "task_id": "INVALID_TASK",
                "execution_date": DEFAULT_DATETIME_1,
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 404

    @parameterized.expand(
        [
            (
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "execution_date": '2020-11-10T12:42:39.442973',
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
                "Naive datetime is disallowed",
            ),
            (
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "execution_date": '2020-11-10T12:4opfo',
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
                "{'execution_date': ['Not a valid datetime.']}",
            ),
        ]
    )
    @provide_session
    def test_should_raise_400_for_naive_and_bad_datetime(self, payload, expected, session):
        self.create_task_instances(session)
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={'REMOTE_USER': "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected
