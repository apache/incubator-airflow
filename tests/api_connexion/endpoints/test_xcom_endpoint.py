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
from datetime import timedelta

import pytest
from parameterized import parameterized

from airflow.models import DagModel, DagRun as DR, XCom
from airflow.security import permissions
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_xcom


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
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        ],
    )
    create_user(
        app,  # type: ignore
        username="test_granular_permissions",
        role_name="TestGranularDag",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        ],
    )
    app.appbuilder.sm.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
        "test-dag-id-1",
        access_control={'TestGranularDag': [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]},
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestXComEndpoint:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        """
        Setup For XCom endpoint TC
        """
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        # clear existing xcoms
        self.clean_db()

    def teardown_method(self) -> None:
        """
        Clear Hanging XComs
        """
        self.clean_db()


class TestGetXComEntry(TestXComEndpoint):
    def test_should_respond_200(self):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        xcom_key = 'test-xcom-key'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entry(dag_id, dag_run_id, execution_date_parsed, task_id, xcom_key)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert 200 == response.status_code

        current_data = response.json
        current_data['timestamp'] = 'TIMESTAMP'
        assert current_data == {
            'dag_id': dag_id,
            'execution_date': execution_date,
            'key': xcom_key,
            'task_id': task_id,
            'timestamp': 'TIMESTAMP',
            'value': 'TEST_VALUE',
        }

    def test_should_raises_401_unauthenticated(self):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        xcom_key = 'test-xcom-key'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entry(dag_id, dag_run_id, execution_date_parsed, task_id, xcom_key)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        xcom_key = 'test-xcom-key'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)

        self._create_xcom_entry(dag_id, dag_run_id, execution_date_parsed, task_id, xcom_key)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403

    @provide_session
    def _create_xcom_entry(self, dag_id, dag_run_id, execution_date, task_id, xcom_key, session=None):
        XCom.set(
            key=xcom_key,
            value="TEST_VALUE",
            execution_date=execution_date,
            task_id=task_id,
            dag_id=dag_id,
        )
        dagrun = DR(
            dag_id=dag_id,
            run_id=dag_run_id,
            execution_date=execution_date,
            start_date=execution_date,
            run_type=DagRunType.MANUAL,
        )
        session.add(dagrun)


class TestGetXComEntries(TestXComEndpoint):
    def test_should_respond_200(self):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)

        self._create_xcom_entries(dag_id, dag_run_id, execution_date_parsed, task_id)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries",
            environ_overrides={'REMOTE_USER': "test"},
        )

        assert 200 == response.status_code
        response_data = response.json
        for xcom_entry in response_data['xcom_entries']:
            xcom_entry['timestamp'] = "TIMESTAMP"
        assert response.json == {
            'xcom_entries': [
                {
                    'dag_id': dag_id,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-1',
                    'task_id': task_id,
                    'timestamp': "TIMESTAMP",
                },
                {
                    'dag_id': dag_id,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-2',
                    'task_id': task_id,
                    'timestamp': "TIMESTAMP",
                },
            ],
            'total_entries': 2,
        }

    def test_should_respond_200_with_tilde_and_access_to_all_dags(self):
        dag_id_1 = 'test-dag-id-1'
        task_id_1 = 'test-task-id-1'
        execution_date = '2005-04-02T00:00:00+00:00'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id_1 = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_1, dag_run_id_1, execution_date_parsed, task_id_1)

        dag_id_2 = 'test-dag-id-2'
        task_id_2 = 'test-task-id-2'
        dag_run_id_2 = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_2, dag_run_id_2, execution_date_parsed, task_id_2)
        self._create_invalid_xcom_entries(execution_date_parsed)

        response = self.client.get(
            "/api/v1/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
            environ_overrides={'REMOTE_USER': "test"},
        )

        assert 200 == response.status_code
        response_data = response.json
        for xcom_entry in response_data['xcom_entries']:
            xcom_entry['timestamp'] = "TIMESTAMP"
        assert response.json == {
            'xcom_entries': [
                {
                    'dag_id': dag_id_1,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-1',
                    'task_id': task_id_1,
                    'timestamp': "TIMESTAMP",
                },
                {
                    'dag_id': dag_id_1,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-2',
                    'task_id': task_id_1,
                    'timestamp': "TIMESTAMP",
                },
                {
                    'dag_id': dag_id_2,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-1',
                    'task_id': task_id_2,
                    'timestamp': "TIMESTAMP",
                },
                {
                    'dag_id': dag_id_2,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-2',
                    'task_id': task_id_2,
                    'timestamp': "TIMESTAMP",
                },
            ],
            'total_entries': 4,
        }

    def test_should_respond_200_with_tilde_and_granular_dag_access(self):
        dag_id_1 = 'test-dag-id-1'
        task_id_1 = 'test-task-id-1'
        execution_date = '2005-04-02T00:00:00+00:00'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id_1 = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_1, dag_run_id_1, execution_date_parsed, task_id_1)

        dag_id_2 = 'test-dag-id-2'
        task_id_2 = 'test-task-id-2'
        dag_run_id_2 = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_2, dag_run_id_2, execution_date_parsed, task_id_2)
        self._create_invalid_xcom_entries(execution_date_parsed)
        response = self.client.get(
            "/api/v1/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
            environ_overrides={'REMOTE_USER': "test_granular_permissions"},
        )

        assert 200 == response.status_code
        response_data = response.json
        for xcom_entry in response_data['xcom_entries']:
            xcom_entry['timestamp'] = "TIMESTAMP"
        assert response.json == {
            'xcom_entries': [
                {
                    'dag_id': dag_id_1,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-1',
                    'task_id': task_id_1,
                    'timestamp': "TIMESTAMP",
                },
                {
                    'dag_id': dag_id_1,
                    'execution_date': execution_date,
                    'key': 'test-xcom-key-2',
                    'task_id': task_id_1,
                    'timestamp': "TIMESTAMP",
                },
            ],
            'total_entries': 2,
        }

    def test_should_raises_401_unauthenticated(self):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id, dag_run_id, execution_date_parsed, task_id)

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"
        )

        assert_401(response)

    @provide_session
    def _create_xcom_entries(self, dag_id, dag_run_id, execution_date, task_id, session=None):
        for i in [1, 2]:
            XCom.set(
                key=f'test-xcom-key-{i}',
                value="TEST",
                execution_date=execution_date,
                task_id=task_id,
                dag_id=dag_id,
            )

        dag = DagModel(dag_id=dag_id)
        session.add(dag)

        dagrun = DR(
            dag_id=dag_id,
            run_id=dag_run_id,
            execution_date=execution_date,
            start_date=execution_date,
            run_type=DagRunType.MANUAL,
        )
        session.add(dagrun)

    @provide_session
    def _create_invalid_xcom_entries(self, execution_date, session=None):
        """
        Invalid XCom entries to test join query
        """
        for i in [1, 2]:
            XCom.set(
                key=f'invalid-xcom-key-{i}',
                value="TEST",
                execution_date=execution_date,
                task_id="invalid_task",
                dag_id="invalid_dag",
            )

        dag = DagModel(dag_id="invalid_dag")
        session.add(dag)
        dagrun = DR(
            dag_id="invalid_dag",
            run_id="invalid_run_id",
            execution_date=execution_date + timedelta(days=1),
            start_date=execution_date,
            run_type=DagRunType.MANUAL,
        )
        session.add(dagrun)
        dagrun = DR(
            dag_id="invalid_dag_1",
            run_id="invalid_run_id",
            execution_date=execution_date,
            start_date=execution_date,
            run_type=DagRunType.MANUAL,
        )
        session.commit()


class TestPaginationGetXComEntries(TestXComEndpoint):
    def setup_method(self):
        self.dag_id = 'test-dag-id'
        self.task_id = 'test-task-id'
        self.execution_date = '2005-04-02T00:00:00+00:00'
        self.execution_date_parsed = parse_execution_date(self.execution_date)
        self.dag_run_id = DR.generate_run_id(DagRunType.MANUAL, self.execution_date_parsed)

    @parameterized.expand(
        [
            (
                "limit=1",
                ["TEST_XCOM_KEY1"],
            ),
            (
                "limit=2",
                ["TEST_XCOM_KEY1", "TEST_XCOM_KEY10"],
            ),
            (
                "offset=5",
                [
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9",
                ],
            ),
            (
                "offset=0",
                [
                    "TEST_XCOM_KEY1",
                    "TEST_XCOM_KEY10",
                    "TEST_XCOM_KEY2",
                    "TEST_XCOM_KEY3",
                    "TEST_XCOM_KEY4",
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9",
                ],
            ),
            (
                "limit=1&offset=5",
                ["TEST_XCOM_KEY5"],
            ),
            (
                "limit=1&offset=1",
                ["TEST_XCOM_KEY10"],
            ),
            (
                "limit=2&offset=2",
                ["TEST_XCOM_KEY2", "TEST_XCOM_KEY3"],
            ),
        ]
    )
    @provide_session
    def test_handle_limit_offset(self, query_params, expected_xcom_ids, session):
        url = "/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries?{query_params}"
        url = url.format(
            dag_id=self.dag_id, dag_run_id=self.dag_run_id, task_id=self.task_id, query_params=query_params
        )
        dagrun = DR(
            dag_id=self.dag_id,
            run_id=self.dag_run_id,
            execution_date=self.execution_date_parsed,
            start_date=self.execution_date_parsed,
            run_type=DagRunType.MANUAL,
        )
        xcom_models = self._create_xcoms(10)
        session.add_all(xcom_models)
        session.add(dagrun)
        session.commit()
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 10
        conn_ids = [conn["key"] for conn in response.json["xcom_entries"] if conn]
        assert conn_ids == expected_xcom_ids

    def _create_xcoms(self, count):
        return [
            XCom(
                key=f'TEST_XCOM_KEY{i}',
                execution_date=self.execution_date_parsed,
                task_id=self.task_id,
                dag_id=self.dag_id,
                timestamp=self.execution_date_parsed,
            )
            for i in range(1, count + 1)
        ]
