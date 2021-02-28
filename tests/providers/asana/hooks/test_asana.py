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
import unittest
from unittest import mock

from asana import Client
from airflow.models import Connection
from airflow.providers.asana.hooks.asana import AsanaHook


class TestAsanaHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.asana_hook = AsanaHook()

    def test_asana_client_retrieved(self):

        self.asana_hook.get_connection = mock.Mock()
        self.asana_hook.get_connection.return_value = Connection(
            conn_type="asana", password="test"
        )
        client = self.asana_hook.get_conn()
        self.assertEqual(type(client), Client)

    def test_missing_password_raises(self):

        self.asana_hook.get_connection = mock.Mock()
        self.asana_hook.get_connection.return_value = Connection(
            conn_type="asana"
        )

        with self.assertRaises(ValueError):
            self.asana_hook.get_conn()
