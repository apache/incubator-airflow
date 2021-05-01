#
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
#
import os
import unittest
from unittest import mock

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class TestSnowflakeHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(rowcount=0)
        self.cur2 = mock.MagicMock(rowcount=0)

        self.cur.sfqid = 'uuid'
        self.cur2.sfqid = 'uuid2'

        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        self.conn.execute_string.return_value = [self.cur, self.cur2]

        self.conn.login = 'user'
        self.conn.password = 'pw'
        self.conn.schema = 'public'
        self.conn.extra_dejson = {
            'database': 'db',
            'account': 'airflow',
            'warehouse': 'af_wh',
            'region': 'af_region',
            'role': 'af_role',
        }

        class UnitTestSnowflakeHook(SnowflakeHook):
            conn_name_attr = 'snowflake_conn_id'

            def get_conn(self):
                return conn

            def get_connection(self, _):
                return conn

        self.db_hook = UnitTestSnowflakeHook(session_parameters={"QUERY_TAG": "This is a test hook"})

        self.non_encrypted_private_key = "/tmp/test_key.pem"
        self.encrypted_private_key = "/tmp/test_key.p8"

        # Write some temporary private keys. First is not encrypted, second is with a passphrase.
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        private_key = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )

        with open(self.non_encrypted_private_key, "wb") as file:
            file.write(private_key)

        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        private_key = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(self.conn.password.encode()),
        )

        with open(self.encrypted_private_key, "wb") as file:
            file.write(private_key)

    def tearDown(self):
        os.remove(self.encrypted_private_key)
        os.remove(self.non_encrypted_private_key)

    def test_get_uri(self):
        uri_shouldbe = (
            'snowflake://user:pw@airflow/db/public?warehouse=af_wh&role=af_role&authenticator=snowflake'
        )
        assert uri_shouldbe == self.db_hook.get_uri()

    def test_single_element_list_calls_execute(self):
        self.db_hook.run(['select * from table'])
        self.cur.execute.assert_called()
        assert self.db_hook.query_ids == ['uuid']

    def test_passed_string_calls_execute_string(self):
        self.db_hook.run('select * from table; select * from table2')

        assert self.db_hook.query_ids == ['uuid', 'uuid2']
        self.conn.execute_string.assert_called()
        self.cur.close.assert_called()
        self.cur2.close.assert_called()

    def test_get_conn_params(self):
        conn_params_shouldbe = {
            'user': 'user',
            'password': 'pw',
            'schema': 'public',
            'database': 'db',
            'account': 'airflow',
            'warehouse': 'af_wh',
            'region': 'af_region',
            'role': 'af_role',
            'authenticator': 'snowflake',
            'session_parameters': {"QUERY_TAG": "This is a test hook"},
        }
        assert self.db_hook.snowflake_conn_id == 'snowflake_default'  # pylint: disable=no-member
        assert conn_params_shouldbe == self.db_hook._get_conn_params()

    def test_get_conn(self):
        assert self.db_hook.get_conn() == self.conn

    def test_key_pair_auth_encrypted(self):
        self.conn.extra_dejson = {
            'database': 'db',
            'account': 'airflow',
            'warehouse': 'af_wh',
            'region': 'af_region',
            'role': 'af_role',
            'private_key_file': self.encrypted_private_key,
        }

        params = self.db_hook._get_conn_params()
        assert 'private_key' in params

    def test_key_pair_auth_not_encrypted(self):
        self.conn.extra_dejson = {
            'database': 'db',
            'account': 'airflow',
            'warehouse': 'af_wh',
            'region': 'af_region',
            'role': 'af_role',
            'private_key_file': self.non_encrypted_private_key,
        }

        self.conn.password = ''
        params = self.db_hook._get_conn_params()
        assert 'private_key' in params

        self.conn.password = None
        params = self.db_hook._get_conn_params()
        assert 'private_key' in params
