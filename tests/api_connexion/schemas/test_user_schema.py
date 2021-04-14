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

from airflow.api_connexion.schemas.user_schema import user_collection_item_schema, user_schema
from airflow.utils import timezone
from tests.test_utils.api_connexion_utils import create_role, delete_role

TEST_EMAIL = "test@example.org"

DEFAULT_TIME = "2021-01-09T13:59:56.336000+00:00"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_role(
        app,
        name="TestRole",
        permissions=[],
    )
    yield app

    delete_role(app, 'TestRole')  # type:ignore


class TestUserBase:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.role = self.app.appbuilder.sm.find_role("TestRole")
        self.session = self.app.appbuilder.get_session

    def teardown_method(self):
        user = self.session.query(User).filter(User.email == TEST_EMAIL).first()
        if user:
            self.session.delete(user)
            self.session.commit()


class TestUserCollectionItemSchema(TestUserBase):
    def test_serialize(self):
        user_model = User(
            first_name="Foo",
            last_name="Bar",
            username="test",
            password="test",
            email=TEST_EMAIL,
            roles=[self.role],
            created_on=timezone.parse(DEFAULT_TIME),
            changed_on=timezone.parse(DEFAULT_TIME),
        )
        self.session.add(user_model)
        self.session.commit()
        user = self.session.query(User).filter(User.email == TEST_EMAIL).first()
        deserialized_user = user_collection_item_schema.dump(user)
        # No password in dump
        assert deserialized_user == {
            'created_on': DEFAULT_TIME,
            'email': 'test@example.org',
            'changed_on': DEFAULT_TIME,
            'user_id': user.id,
            'active': None,
            'last_login': None,
            'last_name': 'Bar',
            'fail_login_count': None,
            'first_name': 'Foo',
            'username': 'test',
            'login_count': None,
            'roles': [{'name': 'TestRole'}],
        }


class TestUserSchema(TestUserBase):
    def test_serialize(self):
        user_model = User(
            first_name="Foo",
            last_name="Bar",
            username="test",
            password="test",
            email=TEST_EMAIL,
            created_on=timezone.parse(DEFAULT_TIME),
            changed_on=timezone.parse(DEFAULT_TIME),
        )
        self.session.add(user_model)
        self.session.commit()
        user = self.session.query(User).filter(User.email == TEST_EMAIL).first()
        deserialized_user = user_schema.dump(user)
        # No password in dump
        assert deserialized_user == {
            'roles': [],
            'created_on': DEFAULT_TIME,
            'email': 'test@example.org',
            'changed_on': DEFAULT_TIME,
            'user_id': user.id,
            'active': None,
            'last_login': None,
            'last_name': 'Bar',
            'fail_login_count': None,
            'first_name': 'Foo',
            'username': 'test',
            'login_count': None,
        }

    def test_deserialize_user(self):
        user_dump = {
            'roles': [{'name': 'TestRole'}],
            'email': 'test@example.org',
            'last_name': 'Bar',
            'first_name': 'Foo',
            'username': 'test',
            'password': 'test',  # loads password
        }
        result = user_schema.load(user_dump)
        assert result == {
            'roles': [{'name': "TestRole"}],
            'email': 'test@example.org',
            'last_name': 'Bar',
            'first_name': 'Foo',
            'username': 'test',
            'password': 'test',  # Password loaded
        }
