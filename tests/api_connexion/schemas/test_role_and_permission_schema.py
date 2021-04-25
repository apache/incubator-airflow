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

import unittest

from airflow.api_connexion.schemas.role_and_permission_schema import (
    RoleCollection,
    role_collection_schema,
    role_schema,
)
from airflow.security import permissions
from airflow.www import app
from tests.test_utils.api_connexion_utils import create_role, delete_role
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


class TestRoleCollectionItemSchema(unittest.TestCase):
    @classmethod
    @dont_initialize_flask_app_submodules(skip_all_except=["init_appbuilder"])
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore
        cls.role = create_role(
            cls.app,  # type: ignore
            name="Test",
            permissions=[
                (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
            ],
        )

    @classmethod
    @dont_initialize_flask_app_submodules(skip_all_except=["init_appbuilder"])
    def tearDownClass(cls) -> None:
        delete_role(cls.app, 'Test')

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
            'permissions': [{'view_menu': {'name': 'Connections'}, 'permission': {'name': 'can_create'}}],
        }


class TestRoleCollectionSchema(unittest.TestCase):
    @classmethod
    @dont_initialize_flask_app_submodules(skip_all_except=["init_appbuilder"])
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore
        cls.role1 = create_role(
            cls.app,  # type: ignore
            name="Test1",
            permissions=[
                (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
            ],
        )
        cls.role2 = create_role(
            cls.app,  # type: ignore
            name="Test2",
            permissions=[
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            ],
        )

    @classmethod
    @dont_initialize_flask_app_submodules(skip_all_except=["init_appbuilder"])
    def tearDownClass(cls) -> None:
        delete_role(cls.app, 'Test1')
        delete_role(cls.app, 'Test2')

    def test_serialize(self):
        instance = RoleCollection([self.role1, self.role2], total_entries=2)
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
