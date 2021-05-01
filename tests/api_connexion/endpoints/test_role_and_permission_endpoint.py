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
from flask_appbuilder.security.sqla.models import Role
from parameterized import parameterized

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.security import permissions
from airflow.www.security import EXISTING_ROLES
from tests.test_utils.api_connexion_utils import (
    assert_401,
    create_role,
    create_user,
    delete_role,
    delete_user,
)


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_PERMISSION),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore
    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestRoleEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

    def teardown_method(self):
        """
        Delete all roles except these ones.
        Test and TestNoPermissions are deleted by delete_user above
        """
        session = self.app.appbuilder.get_session
        existing_roles = set(EXISTING_ROLES)
        existing_roles.update(['Test', 'TestNoPermissions'])
        roles = session.query(Role).filter(~Role.name.in_(existing_roles)).all()
        for role in roles:
            delete_role(self.app, role.name)


class TestGetRoleEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/roles/Admin", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json['name'] == "Admin"

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/roles/invalid-role", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404
        assert {
            'detail': "The Role with name `invalid-role` was not found",
            'status': 404,
            'title': 'Role not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/roles/Admin")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/roles/Admin", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetRolesEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/roles", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        existing_roles = set(EXISTING_ROLES)
        existing_roles.update(['Test', 'TestNoPermissions'])
        assert response.json['total_entries'] == len(existing_roles)
        roles = {role['name'] for role in response.json['roles']}
        assert roles == existing_roles

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/roles")
        assert_401(response)

    def test_should_raises_400_for_invalid_order_by(self):
        response = self.client.get(
            "/api/v1/roles?order_by=invalid", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        assert response.json['detail'] == msg

    def test_should_raise_403_forbidden(self):
        response = self.client.get("/api/v1/roles", environ_overrides={'REMOTE_USER': "test_no_permissions"})
        assert response.status_code == 403


class TestGetRolesEndpointPaginationandFilter(TestRoleEndpoint):
    @parameterized.expand(
        [
            ("/api/v1/roles?limit=1", ['Admin']),
            ("/api/v1/roles?limit=2", ['Admin', "Op"]),
            (
                "/api/v1/roles?offset=1",
                ['Op', 'Public', 'Test', 'TestNoPermissions', 'User', 'Viewer'],
            ),
            (
                "/api/v1/roles?offset=0",
                ['Admin', 'Op', 'Public', 'Test', 'TestNoPermissions', 'User', 'Viewer'],
            ),
            ("/api/v1/roles?limit=1&offset=2", ["Public"]),
            ("/api/v1/roles?limit=1&offset=1", ["Op"]),
            (
                "/api/v1/roles?limit=2&offset=2",
                ['Public', 'Test'],
            ),
        ]
    )
    def test_can_handle_limit_and_offset(self, url, expected_roles):
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        existing_roles = set(EXISTING_ROLES)
        existing_roles.update(['Test', 'TestNoPermissions'])
        assert response.json['total_entries'] == len(existing_roles)
        roles = [role['name'] for role in response.json['roles'] if role]

        assert roles == expected_roles


class TestGetPermissionsEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/permissions", environ_overrides={'REMOTE_USER': "test"})
        actions = {i[0] for i in self.app.appbuilder.sm.get_all_permissions() if i}
        assert response.status_code == 200
        assert response.json['total_entries'] == len(actions)
        returned_actions = {perm['name'] for perm in response.json['actions']}
        assert actions == returned_actions

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/permissions")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/permissions", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestPostRole(TestRoleEndpoint):
    def test_post_should_respond_200(self):
        payload = {
            'name': 'Test2',
            'permissions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
        }
        response = self.client.post("/api/v1/roles", json=payload, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        role = self.app.appbuilder.sm.find_role('Test2')
        assert role is not None

    @parameterized.expand(
        [
            (
                {
                    'permissions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
                },
                "{'name': ['Missing data for required field.']}",
            ),
            (
                {
                    'name': "TestRole",
                    'permissionss': [
                        {
                            'resource': {'name': 'Connections'},  # permissionss not correct
                            'action': {'name': 'can_create'},
                        }
                    ],
                },
                "{'permissionss': ['Unknown field.']}",
            ),
            (
                {
                    'name': "TestRole",
                    'permissions': [
                        {
                            'resources': {'name': 'Connections'},  # resources is invalid, should be resource
                            'action': {'name': 'can_create'},
                        }
                    ],
                },
                "{'permissions': {0: {'resources': ['Unknown field.']}}}",
            ),
            (
                {
                    'name': "TestRole",
                    'permissions': [
                        {'resource': {'name': 'Connections'}, 'actions': {'name': 'can_create'}}
                    ],  # actions is invalid, should be action
                },
                "{'permissions': {0: {'actions': ['Unknown field.']}}}",
            ),
            (
                {
                    'name': "TestRole",
                    'permissions': [
                        {
                            'resource': {'name': 'FooBars'},  # FooBars is not a resource
                            'action': {'name': 'can_create'},
                        }
                    ],
                },
                "The specified resource: 'FooBars' was not found",
            ),
            (
                {
                    'name': "TestRole",
                    'permissions': [
                        {'resource': {'name': 'Connections'}, 'action': {'name': 'can_amend'}}
                    ],  # can_amend is not an action
                },
                "The specified action: 'can_amend' was not found",
            ),
        ]
    )
    def test_post_should_respond_400_for_invalid_payload(self, payload, error_message):
        response = self.client.post("/api/v1/roles", json=payload, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 400
        assert response.json == {
            'detail': error_message,
            'status': 400,
            'title': 'Bad Request',
            'type': EXCEPTIONS_LINK_MAP[400],
        }

    def test_post_should_respond_409_already_exist(self):
        payload = {
            'name': 'Test',
            'permissions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
        }
        response = self.client.post("/api/v1/roles", json=payload, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 409
        assert response.json == {
            'detail': "Role with name `Test` already exist. Please update with patch endpoint",
            'status': 409,
            'title': 'Conflict',
            'type': EXCEPTIONS_LINK_MAP[409],
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/roles",
            json={
                'name': 'Test2',
                'permissions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
            },
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.post(
            "/api/v1/roles",
            json={
                "name": "mytest2",
                "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
            },
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403


class TestDeleteRole(TestRoleEndpoint):
    def test_delete_should_respond_204(self, session):
        role = create_role(self.app, "mytestrole")
        response = self.client.delete(f"/api/v1/roles/{role.name}", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 204
        role_obj = session.query(Role).filter(Role.name == role.name).all()
        assert len(role_obj) == 0

    def test_delete_should_respond_404(self):
        response = self.client.delete(
            "/api/v1/roles/invalidrolename", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        assert response.json == {
            'detail': "The Role with name `invalidrolename` was not found",
            'status': 404,
            'title': 'Role not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.delete("/api/v1/roles/test")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.delete(
            "/api/v1/roles/test", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestPatchRole(TestRoleEndpoint):
    @parameterized.expand(
        [
            ({"name": "mytest"}, "mytest", []),
            (
                {
                    "name": "mytest2",
                    "permissions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
                },
                "mytest2",
                [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
            ),
        ]
    )
    def test_patch_should_respond_200(self, payload, expected_name, expected_permissions):
        role = create_role(self.app, 'mytestrole')
        response = self.client.patch(
            f"/api/v1/roles/{role.name}", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json['name'] == expected_name
        assert response.json["permissions"] == expected_permissions

    @parameterized.expand(
        [
            (
                "?update_mask=name",
                {
                    "name": "mytest2",
                    "permissions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
                },
                "mytest2",
                [],
            ),
            (
                "?update_mask=name, actions",  # both name and actions in update mask
                {
                    "name": "mytest2",
                    "permissions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
                },
                "mytest2",
                [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
            ),
        ]
    )
    def test_patch_should_respond_200_with_update_mask(
        self, update_mask, payload, expected_name, expected_permissions
    ):
        role = create_role(self.app, "mytestrole")
        assert role.permissions == []
        response = self.client.patch(
            f"/api/v1/roles/{role.name}{update_mask}",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        assert response.json['name'] == expected_name
        assert response.json['permissions'] == expected_permissions

    def test_patch_should_respond_400_for_invalid_fields_in_update_mask(self):
        role = create_role(self.app, "mytestrole")
        payload = {"name": "testme"}
        response = self.client.patch(
            f"/api/v1/roles/{role.name}?update_mask=invalid_name",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 400
        assert response.json['detail'] == "'invalid_name' in update_mask is unknown"

    @parameterized.expand(
        [
            (
                {
                    "name": "testme",
                    "actions": [  # Using actions instead of permissions should raise
                        {"resource": {"name": "Connections"}, "action": {"name": "can_create"}}
                    ],
                },
                "{'actions': ['Unknown field.']}",
            ),
            (
                {
                    "name": "testme",
                    "permissions": [
                        {
                            "view_menu": {"name": "Connections"},  # Using view_menu instead of resource
                            "action": {"name": "can_create"},
                        }
                    ],
                },
                "{'permissions': {0: {'view_menu': ['Unknown field.']}}}",
            ),
            (
                {
                    "name": "testme",
                    "permissions": [
                        {
                            "resource": {"name": "FooBars"},  # Using wrong resource name
                            "action": {"name": "can_create"},
                        }
                    ],
                },
                "The specified resource: 'FooBars' was not found",
            ),
            (
                {
                    "name": "testme",
                    "permissions": [
                        {
                            "resource": {"name": "Connections"},  # Using wrong action name
                            "action": {"name": "can_invalid"},
                        }
                    ],
                },
                "The specified action: 'can_invalid' was not found",
            ),
        ]
    )
    def test_patch_should_respond_400_for_invalid_update(self, payload, expected_error):
        role = create_role(self.app, "mytestrole")
        response = self.client.patch(
            f"/api/v1/roles/{role.name}",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected_error

    def test_should_raises_401_unauthenticated(self):
        response = self.client.patch(
            "/api/v1/roles/test",
            json={
                "name": "mytest2",
                "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
            },
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.patch(
            "/api/v1/roles/test",
            json={
                "name": "mytest2",
                "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
            },
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403
