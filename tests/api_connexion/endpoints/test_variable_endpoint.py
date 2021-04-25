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
import pytest
from parameterized import parameterized

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import Variable
from airflow.security import permissions
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_variables


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VARIABLE),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestVariableEndpoint:
    @pytest.fixture(autouse=True)
    def setup_method(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_variables()


class TestDeleteVariable(TestVariableEndpoint):
    def test_should_delete_variable(self):
        Variable.set("delete_var1", 1)
        # make sure variable is added
        response = self.client.get("/api/v1/variables/delete_var1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        response = self.client.delete(
            "/api/v1/variables/delete_var1", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 204

        # make sure variable is deleted
        response = self.client.get("/api/v1/variables/delete_var1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404

    def test_should_respond_404_if_key_does_not_exist(self):
        response = self.client.delete(
            "/api/v1/variables/NONEXIST_VARIABLE_KEY", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        Variable.set("delete_var1", 1)
        # make sure variable is added
        response = self.client.delete("/api/v1/variables/delete_var1")

        assert_401(response)

        # make sure variable is not deleted
        response = self.client.get("/api/v1/variables/delete_var1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

    def test_should_raise_403_forbidden(self):
        expected_value = '{"foo": 1}'
        Variable.set("TEST_VARIABLE_KEY", expected_value)
        response = self.client.get(
            "/api/v1/variables/TEST_VARIABLE_KEY", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetVariable(TestVariableEndpoint):
    def test_should_respond_200(self):
        expected_value = '{"foo": 1}'
        Variable.set("TEST_VARIABLE_KEY", expected_value)
        response = self.client.get(
            "/api/v1/variables/TEST_VARIABLE_KEY", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == {"key": "TEST_VARIABLE_KEY", "value": expected_value}

    def test_should_respond_404_if_not_found(self):
        response = self.client.get(
            "/api/v1/variables/NONEXIST_VARIABLE_KEY", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        Variable.set("TEST_VARIABLE_KEY", '{"foo": 1}')

        response = self.client.get("/api/v1/variables/TEST_VARIABLE_KEY")

        assert_401(response)


class TestGetVariables(TestVariableEndpoint):
    @parameterized.expand(
        [
            (
                "/api/v1/variables?limit=2&offset=0",
                {
                    "variables": [
                        {"key": "var1", "value": "1"},
                        {"key": "var2", "value": "foo"},
                    ],
                    "total_entries": 3,
                },
            ),
            (
                "/api/v1/variables?limit=2&offset=1",
                {
                    "variables": [
                        {"key": "var2", "value": "foo"},
                        {"key": "var3", "value": "[100, 101]"},
                    ],
                    "total_entries": 3,
                },
            ),
            (
                "/api/v1/variables?limit=1&offset=2",
                {
                    "variables": [
                        {"key": "var3", "value": "[100, 101]"},
                    ],
                    "total_entries": 3,
                },
            ),
        ]
    )
    def test_should_get_list_variables(self, query, expected):
        Variable.set("var1", 1)
        Variable.set("var2", "foo")
        Variable.set("var3", "[100, 101]")
        response = self.client.get(query, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json == expected

    def test_should_respect_page_size_limit_default(self):
        for i in range(101):
            Variable.set(f"var{i}", i)
        response = self.client.get("/api/v1/variables", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 101
        assert len(response.json["variables"]) == 100

    def test_should_raise_400_for_invalid_order_by(self):
        for i in range(101):
            Variable.set(f"var{i}", i)
        response = self.client.get(
            "/api/v1/variables?order_by=invalid", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self):
        for i in range(200):
            Variable.set(f"var{i}", i)
        response = self.client.get("/api/v1/variables?limit=180", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert len(response.json['variables']) == 150

    def test_should_raises_401_unauthenticated(self):
        Variable.set("var1", 1)

        response = self.client.get("/api/v1/variables?limit=2&offset=0")

        assert_401(response)


class TestPatchVariable(TestVariableEndpoint):
    def test_should_update_variable(self):
        Variable.set("var1", "foo")
        response = self.client.patch(
            "/api/v1/variables/var1",
            json={
                "key": "var1",
                "value": "updated",
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "key": "var1",
            "value": "updated",
        }

    def test_should_reject_invalid_update(self):
        Variable.set("var1", "foo")
        response = self.client.patch(
            "/api/v1/variables/var1",
            json={
                "key": "var2",
                "value": "updated",
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "title": "Invalid post body",
            "status": 400,
            "type": EXCEPTIONS_LINK_MAP[400],
            "detail": "key from request body doesn't match uri parameter",
        }

        response = self.client.patch(
            "/api/v1/variables/var1",
            json={
                "key": "var2",
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.json == {
            "title": "Invalid Variable schema",
            "status": 400,
            "type": EXCEPTIONS_LINK_MAP[400],
            "detail": "{'value': ['Missing data for required field.']}",
        }

    def test_should_raises_401_unauthenticated(self):
        Variable.set("var1", "foo")

        response = self.client.patch(
            "/api/v1/variables/var1",
            json={
                "key": "var1",
                "value": "updated",
            },
        )

        assert_401(response)


class TestPostVariables(TestVariableEndpoint):
    def test_should_create_variable(self):
        response = self.client.post(
            "/api/v1/variables",
            json={
                "key": "var_create",
                "value": "{}",
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        response = self.client.get("/api/v1/variables/var_create", environ_overrides={'REMOTE_USER': "test"})
        assert response.json == {
            "key": "var_create",
            "value": "{}",
        }

    def test_should_reject_invalid_request(self):
        response = self.client.post(
            "/api/v1/variables",
            json={
                "key": "var_create",
                "v": "{}",
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "title": "Invalid Variable schema",
            "status": 400,
            "type": EXCEPTIONS_LINK_MAP[400],
            "detail": "{'value': ['Missing data for required field.'], 'v': ['Unknown field.']}",
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/variables",
            json={
                "key": "var_create",
                "value": "{}",
            },
        )

        assert_401(response)
