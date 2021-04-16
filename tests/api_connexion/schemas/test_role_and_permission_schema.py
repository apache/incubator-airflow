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

from airflow.api_connexion.schemas.role_and_permission_schema import (
    RoleCollection,
    role_collection_schema,
    role_schema,
)
from airflow.security import permissions
from tests.test_utils.api_connexion_utils import create_role, delete_role


class TestRoleCollectionItemSchema:
    @pytest.fixture(scope="class")
    def role(self, minimal_app_for_api):
        yield create_role(
            minimal_app_for_api,  # type: ignore
            name="Test",
            permissions=[
                (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
            ],
        )
        delete_role(minimal_app_for_api, "Test")

    @pytest.fixture(autouse=True)
    def _set_attrs(self, minimal_app_for_api, role):
        self.app = minimal_app_for_api
        self.role = role

    def test_serialize(self):
        deserialized_role = role_schema.dump(self.role)
        assert deserialized_role == {
            'name': 'Test',
            'actions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
        }

    def test_deserialize(self):
        role = {
            'name': 'Test',
            'actions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
        }
        role_obj = role_schema.load(role)
        assert role_obj == {
            'name': 'Test',
            'permissions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
        }


class TestRoleCollectionSchema:
    @pytest.fixture(scope="class")
    def role1(self, minimal_app_for_api):
        yield create_role(
            minimal_app_for_api,  # type: ignore
            name="Test1",
            permissions=[
                (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
            ],
        )
        delete_role(minimal_app_for_api, 'Test1')

    @pytest.fixture(scope="class")
    def role2(self, minimal_app_for_api):
        yield create_role(
            minimal_app_for_api,  # type: ignore
            name="Test2",
            permissions=[
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            ],
        )
        delete_role(minimal_app_for_api, 'Test2')

    def test_serialize(self, role1, role2):
        instance = RoleCollection([role1, role2], total_entries=2)
        deserialized = role_collection_schema.dump(instance)
        assert deserialized == {
            'roles': [
                {
                    'name': 'Test1',
                    'actions': [{'resource': {'name': 'Connections'}, 'action': {'name': 'can_create'}}],
                },
                {
                    'name': 'Test2',
                    'actions': [{'resource': {'name': 'DAGs'}, 'action': {'name': 'can_edit'}}],
                },
            ],
            'total_entries': 2,
        }
