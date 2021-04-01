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
import ast
import os
from unittest import mock

import pytest
from itsdangerous import URLSafeSerializer
from parameterized import parameterized

from airflow import DAG
from airflow.configuration import conf
from airflow.models import DagBag
from airflow.security import permissions
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_dag_code, clear_db_dags, clear_db_serialized_dags

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type:ignore
        username="test",
        role_name="Test",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE)],  # type: ignore
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestGetSource:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.clear_db()

    def teardown_method(self) -> None:
        self.clear_db()

    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_code()

    @staticmethod
    def _get_dag_file_docstring(fileloc: str) -> str:
        with open(fileloc) as f:
            file_contents = f.read()
        module = ast.parse(file_contents)
        docstring = ast.get_docstring(module)
        return docstring

    @parameterized.expand([(True,), (False,)])
    def test_should_respond_200_text(self, store_dag_code):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
            dagbag.sync_to_db()
            first_dag: DAG = next(iter(dagbag.dags.values()))
            dag_docstring = self._get_dag_file_docstring(first_dag.fileloc)

            url = f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}"
            response = self.client.get(
                url, headers={"Accept": "text/plain"}, environ_overrides={'REMOTE_USER': "test"}
            )

            assert 200 == response.status_code
            assert dag_docstring in response.data.decode()
            assert 'text/plain' == response.headers['Content-Type']

    @parameterized.expand([(True,), (False,)])
    def test_should_respond_200_json(self, store_dag_code):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
            dagbag.sync_to_db()
            first_dag: DAG = next(iter(dagbag.dags.values()))
            dag_docstring = self._get_dag_file_docstring(first_dag.fileloc)

            url = f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}"
            response = self.client.get(
                url, headers={"Accept": 'application/json'}, environ_overrides={'REMOTE_USER': "test"}
            )

            assert 200 == response.status_code
            assert dag_docstring in response.json['content']
            assert 'application/json' == response.headers['Content-Type']

    @parameterized.expand([(True,), (False,)])
    def test_should_respond_406(self, store_dag_code):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
            dagbag.sync_to_db()
            first_dag: DAG = next(iter(dagbag.dags.values()))

            url = f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}"
            response = self.client.get(
                url, headers={"Accept": 'image/webp'}, environ_overrides={'REMOTE_USER': "test"}
            )

            assert 406 == response.status_code

    @parameterized.expand([(True,), (False,)])
    def test_should_respond_404(self, store_dag_code):
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            wrong_fileloc = "abcd1234"
            url = f"/api/v1/dagSources/{wrong_fileloc}"
            response = self.client.get(
                url, headers={"Accept": 'application/json'}, environ_overrides={'REMOTE_USER': "test"}
            )

            assert 404 == response.status_code

    def test_should_raises_401_unauthenticated(self):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        first_dag: DAG = next(iter(dagbag.dags.values()))

        response = self.client.get(
            f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}",
            headers={"Accept": "text/plain"},
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        first_dag: DAG = next(iter(dagbag.dags.values()))

        response = self.client.get(
            f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}",
            headers={"Accept": "text/plain"},
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403
