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
from flask_appbuilder.security.sqla.models import User
from parameterized import parameterized

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.security import permissions
from airflow.utils import timezone
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars

DEFAULT_TIME = "2020-06-11T18:00:00+00:00"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_LIST, permissions.RESOURCE_USER_DB_MODELVIEW),
            (permissions.ACTION_CAN_SHOW, permissions.RESOURCE_USER_DB_MODELVIEW),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestUserEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.session = self.app.appbuilder.get_session

    def teardown_method(self) -> None:
        # Delete users that have our custom default time
        users = self.session.query(User).filter(User.changed_on == timezone.parse(DEFAULT_TIME)).all()
        for user in users:
            self.session.delete(user)
        self.session.commit()

    def _create_users(self, count, roles=None):
        # create users with defined created_on and changed_on date
        # for easy testing
        if roles is None:
            roles = []
        return [
            User(
                first_name=f'test{i}',
                last_name=f'test{i}',
                username=f'TEST_USER{i}',
                email=f'mytest@test{i}.org',
                roles=roles or [],
                created_on=timezone.parse(DEFAULT_TIME),
                changed_on=timezone.parse(DEFAULT_TIME),
            )
            for i in range(1, count + 1)
        ]


class TestGetUser(TestUserEndpoint):
    def test_should_respond_200(self):
        users = self._create_users(1)
        self.session.add_all(users)
        self.session.commit()
        response = self.client.get("/api/v1/users/TEST_USER1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json == {
            'active': None,
            'changed_on': DEFAULT_TIME,
            'created_on': DEFAULT_TIME,
            'email': 'mytest@test1.org',
            'fail_login_count': None,
            'first_name': 'test1',
            'last_login': None,
            'last_name': 'test1',
            'login_count': None,
            'roles': [],
            'user_id': users[0].id,
            'username': 'TEST_USER1',
        }

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/users/invalid-user", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404
        assert {
            'detail': "The User with username `invalid-user` was not found",
            'status': 404,
            'title': 'User not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/users/TEST_USER1")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/users/TEST_USER1", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetUsers(TestUserEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/users", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        usernames = [user["username"] for user in response.json["users"] if user]
        assert usernames == ['test', 'test_no_permissions']

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/users")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get("/api/v1/users", environ_overrides={'REMOTE_USER': "test_no_permissions"})
        assert response.status_code == 403


class TestGetUsersPagination(TestUserEndpoint):
    @parameterized.expand(
        [
            ("/api/v1/users?limit=1", ["test"]),
            ("/api/v1/users?limit=2", ["test", "test_no_permissions"]),
            (
                "/api/v1/users?offset=5",
                [
                    "TEST_USER4",
                    "TEST_USER5",
                    "TEST_USER6",
                    "TEST_USER7",
                    "TEST_USER8",
                    "TEST_USER9",
                    "TEST_USER10",
                ],
            ),
            (
                "/api/v1/users?offset=0",
                [
                    "test",
                    "test_no_permissions",
                    "TEST_USER1",
                    "TEST_USER2",
                    "TEST_USER3",
                    "TEST_USER4",
                    "TEST_USER5",
                    "TEST_USER6",
                    "TEST_USER7",
                    "TEST_USER8",
                    "TEST_USER9",
                    "TEST_USER10",
                ],
            ),
            ("/api/v1/users?limit=1&offset=5", ["TEST_USER4"]),
            ("/api/v1/users?limit=1&offset=1", ["test_no_permissions"]),
            (
                "/api/v1/users?limit=2&offset=2",
                ["TEST_USER1", "TEST_USER2"],
            ),
        ]
    )
    def test_handle_limit_offset(self, url, expected_usernames):
        users = self._create_users(10)
        self.session.add_all(users)
        self.session.commit()
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 12
        usernames = [user["username"] for user in response.json["users"] if user]
        assert usernames == expected_usernames

    def test_should_respect_page_size_limit_default(self):
        users = self._create_users(200)
        self.session.add_all(users)
        self.session.commit()

        response = self.client.get("/api/v1/users", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        # Explicitly add the 2 users on setUp
        assert response.json["total_entries"] == 200 + len(['test', 'test_no_permissions'])
        assert len(response.json["users"]) == 100

    def test_limit_of_zero_should_return_default(self):
        users = self._create_users(200)
        self.session.add_all(users)
        self.session.commit()

        response = self.client.get("/api/v1/users?limit=0", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        # Explicit add the 2 users on setUp
        assert response.json["total_entries"] == 200 + len(['test', 'test_no_permissions'])
        assert len(response.json["users"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self):
        users = self._create_users(200)
        self.session.add_all(users)
        self.session.commit()

        response = self.client.get("/api/v1/users?limit=180", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert len(response.json['users']) == 150
