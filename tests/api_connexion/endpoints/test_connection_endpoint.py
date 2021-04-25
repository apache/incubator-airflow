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
from airflow.models import Connection
from airflow.security import permissions
from airflow.utils.session import provide_session
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_connections


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestConnectionEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        # we want only the connection created here for this test
        clear_db_connections(False)

    def teardown_method(self) -> None:
        clear_db_connections()

    def _create_connection(self, session):
        connection_model = Connection(conn_id='test-connection-id', conn_type='test_type')
        session.add(connection_model)
        session.commit()


class TestDeleteConnection(TestConnectionEndpoint):
    def test_delete_should_respond_204(self, session):
        connection_model = Connection(conn_id='test-connection', conn_type='test_type')

        session.add(connection_model)
        session.commit()
        conn = session.query(Connection).all()
        assert len(conn) == 1
        response = self.client.delete(
            "/api/v1/connections/test-connection", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 204
        connection = session.query(Connection).all()
        assert len(connection) == 0

    def test_delete_should_respond_404(self):
        response = self.client.delete(
            "/api/v1/connections/test-connection", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        assert response.json == {
            'detail': "The Connection with connection_id: `test-connection` was not found",
            'status': 404,
            'title': 'Connection not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.delete("/api/v1/connections/test-connection")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/connections/test-connection-id", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetConnection(TestConnectionEndpoint):
    def test_should_respond_200(self, session):
        connection_model = Connection(
            conn_id='test-connection-id',
            conn_type='mysql',
            host='mysql',
            login='login',
            schema='testschema',
            port=80,
            extra="{'param': 'value'}",
        )
        session.add(connection_model)
        session.commit()
        result = session.query(Connection).all()
        assert len(result) == 1
        response = self.client.get(
            "/api/v1/connections/test-connection-id", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == {
            "connection_id": "test-connection-id",
            "conn_type": 'mysql',
            "host": 'mysql',
            "login": 'login',
            'schema': 'testschema',
            'port': 80,
            'extra': "{'param': 'value'}",
        }

    def test_should_respond_404(self):
        response = self.client.get(
            "/api/v1/connections/invalid-connection", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        assert {
            'detail': "The Connection with connection_id: `invalid-connection` was not found",
            'status': 404,
            'title': 'Connection not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/connections/test-connection-id")

        assert_401(response)


class TestGetConnections(TestConnectionEndpoint):
    def test_should_respond_200(self, session):
        connection_model_1 = Connection(conn_id='test-connection-id-1', conn_type='test_type')
        connection_model_2 = Connection(conn_id='test-connection-id-2', conn_type='test_type')
        connections = [connection_model_1, connection_model_2]
        session.add_all(connections)
        session.commit()
        result = session.query(Connection).all()
        assert len(result) == 2
        response = self.client.get("/api/v1/connections", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json == {
            'connections': [
                {
                    "connection_id": "test-connection-id-1",
                    "conn_type": 'test_type',
                    "host": None,
                    "login": None,
                    'schema': None,
                    'port': None,
                },
                {
                    "connection_id": "test-connection-id-2",
                    "conn_type": 'test_type',
                    "host": None,
                    "login": None,
                    'schema': None,
                    'port': None,
                },
            ],
            'total_entries': 2,
        }

    def test_should_respond_200_with_order_by(self, session):
        connection_model_1 = Connection(conn_id='test-connection-id-1', conn_type='test_type')
        connection_model_2 = Connection(conn_id='test-connection-id-2', conn_type='test_type')
        connections = [connection_model_1, connection_model_2]
        session.add_all(connections)
        session.commit()
        result = session.query(Connection).all()
        assert len(result) == 2
        response = self.client.get(
            "/api/v1/connections?order_by=-connection_id", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        # Using - means descending
        assert response.json == {
            'connections': [
                {
                    "connection_id": "test-connection-id-2",
                    "conn_type": 'test_type',
                    "host": None,
                    "login": None,
                    'schema': None,
                    'port': None,
                },
                {
                    "connection_id": "test-connection-id-1",
                    "conn_type": 'test_type',
                    "host": None,
                    "login": None,
                    'schema': None,
                    'port': None,
                },
            ],
            'total_entries': 2,
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/connections")

        assert_401(response)


class TestGetConnectionsPagination(TestConnectionEndpoint):
    @parameterized.expand(
        [
            ("/api/v1/connections?limit=1", ['TEST_CONN_ID1']),
            ("/api/v1/connections?limit=2", ['TEST_CONN_ID1', "TEST_CONN_ID2"]),
            (
                "/api/v1/connections?offset=5",
                [
                    "TEST_CONN_ID6",
                    "TEST_CONN_ID7",
                    "TEST_CONN_ID8",
                    "TEST_CONN_ID9",
                    "TEST_CONN_ID10",
                ],
            ),
            (
                "/api/v1/connections?offset=0",
                [
                    "TEST_CONN_ID1",
                    "TEST_CONN_ID2",
                    "TEST_CONN_ID3",
                    "TEST_CONN_ID4",
                    "TEST_CONN_ID5",
                    "TEST_CONN_ID6",
                    "TEST_CONN_ID7",
                    "TEST_CONN_ID8",
                    "TEST_CONN_ID9",
                    "TEST_CONN_ID10",
                ],
            ),
            ("/api/v1/connections?limit=1&offset=5", ["TEST_CONN_ID6"]),
            ("/api/v1/connections?limit=1&offset=1", ["TEST_CONN_ID2"]),
            (
                "/api/v1/connections?limit=2&offset=2",
                ["TEST_CONN_ID3", "TEST_CONN_ID4"],
            ),
        ]
    )
    @provide_session
    def test_handle_limit_offset(self, url, expected_conn_ids, session):
        connections = self._create_connections(10)
        session.add_all(connections)
        session.commit()
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 10
        conn_ids = [conn["connection_id"] for conn in response.json["connections"] if conn]
        assert conn_ids == expected_conn_ids

    def test_should_respect_page_size_limit_default(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get("/api/v1/connections", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["connections"]) == 100

    def test_invalid_order_by_raises_400(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get(
            "/api/v1/connections?order_by=invalid", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert (
            response.json['detail'] == "Ordering with 'invalid' is disallowed or"
            " the attribute does not exist on the model"
        )

    def test_limit_of_zero_should_return_default(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get("/api/v1/connections?limit=0", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["connections"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get("/api/v1/connections?limit=180", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert len(response.json['connections']) == 150

    def _create_connections(self, count):
        return [
            Connection(conn_id='TEST_CONN_ID' + str(i), conn_type='TEST_CONN_TYPE' + str(i))
            for i in range(1, count + 1)
        ]


class TestPatchConnection(TestConnectionEndpoint):
    @parameterized.expand(
        [
            ({"connection_id": "test-connection-id", "conn_type": 'test_type', "extra": "{'key': 'var'}"},),
            ({"extra": "{'key': 'var'}"},),
        ]
    )
    @provide_session
    def test_patch_should_respond_200(self, payload, session):
        self._create_connection(session)

        response = self.client.patch(
            "/api/v1/connections/test-connection-id", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200

    def test_patch_should_respond_200_with_update_mask(self, session):
        self._create_connection(session)
        test_connection = "test-connection-id"
        payload = {
            "connection_id": test_connection,
            "conn_type": 'test_type_2',
            "extra": "{'key': 'var'}",
            'login': "login",
            "port": 80,
        }
        response = self.client.patch(
            "/api/v1/connections/test-connection-id?update_mask=port,login",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        connection = session.query(Connection).filter_by(conn_id=test_connection).first()
        assert connection.password is None
        assert response.json == {
            "connection_id": test_connection,  # not updated
            "conn_type": 'test_type',  # Not updated
            "extra": None,  # Not updated
            'login': "login",  # updated
            "port": 80,  # updated
            "schema": None,
            "host": None,
        }

    @parameterized.expand(
        [
            (
                {
                    "connection_id": 'test-connection-id',
                    "conn_type": 'test_type_2',
                    "extra": "{'key': 'var'}",
                    'login': "login",
                    "port": 80,
                },
                'update_mask=ports, login',  # posts is unknown
                "'ports' is unknown or cannot be updated.",
            ),
            (
                {
                    "connection_id": 'test-connection-id',
                    "conn_type": 'test_type_2',
                    "extra": "{'key': 'var'}",
                    'login': "login",
                    "port": 80,
                },
                'update_mask=port, login, conn_id',  # conn_id is unknown
                "'conn_id' is unknown or cannot be updated.",
            ),
            (
                {
                    "connection_id": 'test-connection-id',
                    "conn_type": 'test_type_2',
                    "extra": "{'key': 'var'}",
                    'login': "login",
                    "port": 80,
                },
                'update_mask=port, login, connection_id',  # connection_id cannot be updated
                "'connection_id' is unknown or cannot be updated.",
            ),
            (
                {
                    "connection_id": "test-connection",  # trying to change connection_id
                    "conn_type": "test-type",
                    "login": "login",
                },
                '',  # not necessary
                "The connection_id cannot be updated.",
            ),
        ]
    )
    @provide_session
    def test_patch_should_respond_400_for_invalid_fields_in_update_mask(
        self, payload, update_mask, error_message, session
    ):
        self._create_connection(session)
        response = self.client.patch(
            f"/api/v1/connections/test-connection-id?{update_mask}",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 400
        assert response.json['detail'] == error_message

    @parameterized.expand(
        [
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": "test-type",
                    "extra": 0,  # expected string
                },
                "0 is not of type 'string' - 'extra'",
            ),
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": "test-type",
                    "extras": "{}",  # extras not a known field e.g typo
                },
                "extras",
            ),
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": "test-type",
                    "invalid_field": "invalid field",  # unknown field
                    "_password": "{}",  # _password not a known field
                },
                "_password",
            ),
        ]
    )
    @provide_session
    def test_patch_should_respond_400_for_invalid_update(self, payload, error_message, session):
        self._create_connection(session)
        response = self.client.patch(
            "/api/v1/connections/test-connection-id", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert error_message in response.json['detail']

    def test_patch_should_respond_404_not_found(self):
        payload = {"connection_id": "test-connection-id", "conn_type": "test-type", "port": 90}
        response = self.client.patch(
            "/api/v1/connections/test-connection-id", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        assert {
            'detail': "The Connection with connection_id: `test-connection-id` was not found",
            'status': 404,
            'title': 'Connection not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, session):
        self._create_connection(session)

        response = self.client.patch(
            "/api/v1/connections/test-connection-id",
            json={"connection_id": "test-connection-id", "conn_type": 'test_type', "extra": "{'key': 'var'}"},
        )

        assert_401(response)


class TestPostConnection(TestConnectionEndpoint):
    def test_post_should_respond_200(self, session):
        payload = {"connection_id": "test-connection-id", "conn_type": 'test_type'}
        response = self.client.post(
            "/api/v1/connections", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        connection = session.query(Connection).all()
        assert len(connection) == 1
        assert connection[0].conn_id == 'test-connection-id'

    def test_post_should_respond_400_for_invalid_payload(self):
        payload = {
            "connection_id": "test-connection-id",
        }  # conn_type missing
        response = self.client.post(
            "/api/v1/connections", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert response.json == {
            'detail': "{'conn_type': ['Missing data for required field.']}",
            'status': 400,
            'title': 'Bad Request',
            'type': EXCEPTIONS_LINK_MAP[400],
        }

    def test_post_should_respond_409_already_exist(self):
        payload = {"connection_id": "test-connection-id", "conn_type": 'test_type'}
        response = self.client.post(
            "/api/v1/connections", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        # Another request
        response = self.client.post(
            "/api/v1/connections", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 409
        assert response.json == {
            'detail': 'Connection already exist. ID: test-connection-id',
            'status': 409,
            'title': 'Conflict',
            'type': EXCEPTIONS_LINK_MAP[409],
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/connections", json={"connection_id": "test-connection-id", "conn_type": 'test_type'}
        )

        assert_401(response)
