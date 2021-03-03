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
from unittest.mock import patch

from asana import Client

from airflow.models import Connection
from airflow.providers.asana.hooks.asana import AsanaHook


class TestAsanaHook(unittest.TestCase):
    """
    Tests for AsanaHook Asana client retrieval
    """

    def test_asana_client_retrieved(self):
        """
        Test that we successfully retrieve an Asana client given a Connection with complete information.
        :return: None
        """
        with patch.object(AsanaHook, "get_connection", return_value=Connection(conn_type="asana", password="test")):
            hook = AsanaHook()
        client = hook.get_conn()
        self.assertEqual(type(client), Client)

    def test_missing_password_raises(self):
        """
        Test that the Asana hook raises an exception if password not provided in connection.
        :return: None
        """
        with patch.object(AsanaHook, "get_connection", return_value=Connection(conn_type="asana")):
            hook = AsanaHook()
        with self.assertRaises(ValueError):
            hook.get_conn()

    def test_conflicting_project_params_raises(self):
        """
        Test that the hook raises a ValueError on init if we pass it default parameters containing both
        'projects' and 'project'.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"project": "1", "projects": ["1", "2"]}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            with self.assertRaises(ValueError):
                AsanaHook()

    def test_merge_create_task_parameters_default_project(self):
        """
        Test that merge_create_task_parameters correctly merges the default and method parameters when we
        do not override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"project": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "projects": "1"}
        self.assertEqual(expected_merged_params, hook.merge_create_task_parameters("test", {}))

    def test_merge_create_task_parameters_specified_project(self):
        """
        Test that merge_create_task_parameters correctly merges the default and method parameters when we
        override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"project": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "projects": ["1", "2"]}
        self.assertEqual(expected_merged_params,
                         hook.merge_create_task_parameters("test", {"projects": ["1", "2"]}))

    def test_merge_find_task_parameters_default_project(self):
        """
        Test that merge_find_task_parameters correctly merges the default and method parameters when we
        do not override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"projects": ["1"]}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"project": "1"}
        self.assertEqual(expected_merged_params, hook.merge_find_task_parameters({}))

    def test_merge_find_task_parameters_specified_project(self):
        """
        Test that merge_find_task_parameters correctly merges the default and method parameters when we
        do override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"projects": ["1"]}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"project": "2"}
        self.assertEqual(expected_merged_params, hook.merge_find_task_parameters({"project": "2"}))

