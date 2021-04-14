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
import os
import unittest.mock
from datetime import datetime

import pytest

from airflow import DAG
from airflow.models import DagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.dummy import DummyOperator
from airflow.security import permissions
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags


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
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestTaskEndpoint:
    dag_id = "test_dag"
    task_id = "op1"

    @pytest.fixture(scope="class")
    def setup_dag(self, configured_app):
        with DAG(self.dag_id, start_date=datetime(2020, 6, 15), doc_md="details") as dag:
            DummyOperator(task_id=self.task_id)

        dag_bag = DagBag(os.devnull, include_examples=False)
        dag_bag.dags = {dag.dag_id: dag}
        configured_app.dag_bag = dag_bag  # type:ignore

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app, setup_dag) -> None:  # pylint: disable=unused-argument
        self.clean_db()
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

    def teardown_method(self) -> None:
        self.clean_db()


class TestGetTask(TestTaskEndpoint):
    def test_should_respond_200(self):
        expected = {
            "class_ref": {
                "class_name": "DummyOperator",
                "module_path": "airflow.operators.dummy",
            },
            "depends_on_past": False,
            "downstream_task_ids": [],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "owner": "airflow",
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00+00:00",
            "task_id": "op1",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
        }
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks/{self.task_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == expected

    def test_should_respond_200_serialized(self):

        # Get the dag out of the dagbag before we patch it to an empty one
        SerializedDagModel.write_dag(self.app.dag_bag.get_dag(self.dag_id))

        dag_bag = DagBag(os.devnull, include_examples=False, read_dags_from_db=True)
        patcher = unittest.mock.patch.object(self.app, 'dag_bag', dag_bag)
        patcher.start()

        expected = {
            "class_ref": {
                "class_name": "DummyOperator",
                "module_path": "airflow.operators.dummy",
            },
            "depends_on_past": False,
            "downstream_task_ids": [],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "owner": "airflow",
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00+00:00",
            "task_id": "op1",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
        }
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks/{self.task_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == expected
        patcher.stop()

    def test_should_respond_404(self):
        task_id = "xxxx_not_existing"
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks/{task_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/tasks/{self.task_id}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetTasks(TestTaskEndpoint):
    def test_should_respond_200(self):
        expected = {
            "tasks": [
                {
                    "class_ref": {
                        "class_name": "DummyOperator",
                        "module_path": "airflow.operators.dummy",
                    },
                    "depends_on_past": False,
                    "downstream_task_ids": [],
                    "end_date": None,
                    "execution_timeout": None,
                    "extra_links": [],
                    "owner": "airflow",
                    "pool": "default_pool",
                    "pool_slots": 1.0,
                    "priority_weight": 1.0,
                    "queue": "default",
                    "retries": 0.0,
                    "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
                    "retry_exponential_backoff": False,
                    "start_date": "2020-06-15T00:00:00+00:00",
                    "task_id": "op1",
                    "template_fields": [],
                    "trigger_rule": "all_success",
                    "ui_color": "#e8f7e4",
                    "ui_fgcolor": "#000",
                    "wait_for_downstream": False,
                    "weight_rule": "downstream",
                }
            ],
            "total_entries": 1,
        }
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == expected

    def test_should_respond_404(self):
        dag_id = "xxxx_not_existing"
        response = self.client.get(f"/api/v1/dags/{dag_id}/tasks", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/tasks")

        assert_401(response)
