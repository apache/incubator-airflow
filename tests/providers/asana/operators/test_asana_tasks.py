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
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.asana.operators.asana_tasks import (
    AsanaCreateTaskOperator,
    AsanaDeleteTaskOperator,
    AsanaFindTaskOperator,
    AsanaUpdateTaskOperator,
)
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
asana_client_mock = Mock(name="asana_client_for_test")


class TestAsanaTaskOperators(unittest.TestCase):
    """
    Test that the AsanaTaskOperators are using the python-asana methods as expected.
    """

    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag
        db.merge_conn(Connection(conn_id="asana_test", conn_type="asana", password="test"))

    @patch("airflow.providers.asana.hooks.asana.Client", autospec=True, return_value=asana_client_mock)
    def test_asana_create_task_operator(self, asana_client):
        """
        Tests that the AsanaCreateTaskOperator makes the expected call to python-asana given valid arguments.
        """
        asana_client.access_token.return_value.tasks.create.return_value = {"gid": "1"}
        create_task = AsanaCreateTaskOperator(
            task_id="create_task",
            conn_id="asana_test",
            name="test",
            task_parameters={"workspace": "1"},
            dag=self.dag,
        )
        create_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert asana_client.access_token.return_value.tasks.create.called

    @patch("airflow.providers.asana.hooks.asana.Client", autospec=True, return_value=asana_client_mock)
    def test_asana_find_task_operator(self, asana_client):
        """
        Tests that the AsanaFindTaskOperator makes the expected call to python-asana given valid arguments.
        """
        asana_client.access_token.return_value.tasks.create.return_value = {"gid": "1"}
        find_task = AsanaFindTaskOperator(
            task_id="find_task",
            conn_id="asana_test",
            search_parameters={"project": "test"},
            dag=self.dag,
        )
        find_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert asana_client.access_token.return_value.tasks.find_all.called

    @patch("airflow.providers.asana.hooks.asana.Client", autospec=True, return_value=asana_client_mock)
    def test_asana_update_task_operator(self, asana_client):
        """
        Tests that the AsanaUpdateTaskOperator makes the expected call to python-asana given valid arguments.
        """
        update_task = AsanaUpdateTaskOperator(
            task_id="update_task",
            conn_id="asana_test",
            asana_task_gid="test",
            task_parameters={"completed": True},
            dag=self.dag,
        )
        update_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert asana_client.access_token.return_value.tasks.update.called

    @patch("airflow.providers.asana.hooks.asana.Client", autospec=True, return_value=asana_client_mock)
    def test_asana_delete_task_operator(self, asana_client):
        """
        Tests that the AsanaDeleteTaskOperator makes the expected call to python-asana given valid arguments.
        """
        delete_task = AsanaDeleteTaskOperator(
            task_id="delete_task", conn_id="asana_test", asana_task_gid="test", dag=self.dag
        )
        delete_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert asana_client.access_token.return_value.tasks.delete_task.called
