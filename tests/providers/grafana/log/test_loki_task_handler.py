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
from datetime import datetime
from unittest import mock

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.grafana.log.loki_task_handler import LokiTaskHandler
from airflow.utils.state import State


class TestLokiTaskHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.base_log_folder = "local/log/location"
        self.filename_template = "{try_number}.log"
        self.loki_task_handler = LokiTaskHandler(
            base_log_folder=self.base_log_folder,
            filename_template=self.filename_template,
            loki_conn_id="loki_default",
            labels={"key": "value"},
        )

        date = datetime(2019, 1, 1)
        self.dag = DAG("dag_for_testing_loki_task_handler", start_date=date)
        task = DummyOperator(task_id="task_for_testing_loki_task_handler", dag=self.dag)
        self.ti = TaskInstance(
            task=task,
            execution_date=date,
        )
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.ti.start_date = datetime(2020, 1, 1, 1, 0, 0)
        self.ti.end_date = None
        self.addCleanup(self.dag.clear)

    def test_get_label(self):
        value = {
            "airflow_dag_id": "dag_for_testing_loki_task_handler",
            "airflow_task_id": "task_for_testing_loki_task_handler",
            "airflow_try_number": "1",
            "key": "value",
        }
        self.assertEqual(self.loki_task_handler.get_label(self.ti), value)

    def test_task_label(self):
        value = {
            "airflow_dag_id": "dag_for_testing_loki_task_handler",
            "airflow_task_id": "task_for_testing_loki_task_handler",
            "airflow_try_number": "1",
        }
        self.assertEqual(self.loki_task_handler._task_label(self.ti), value)

    def test_query_time_range(self):
        self.assertEqual(self.loki_task_handler.query_time_range(self.ti), (1577840340, 1577926800))
        self.ti.end_date = datetime(2020, 1, 1, 1, 2, 30)
        self.assertEqual(self.loki_task_handler.query_time_range(self.ti), (1577840340, 1577840670))

    def test_read_parameters(self):
        self.ti.end_date = datetime(2020, 1, 1, 1, 2, 30)
        query = (
            '{key="value", airflow_dag_id="dag_for_testing_loki_task_handler", '
            'airflow_task_id="task_for_testing_loki_task_handler", '
            'airflow_try_number="1"} != "WARNING" '
        )
        params = {
            "direction": "forward",
            "start": 1577840340,
            "end": 1577840670,
            "query": query,
            "limit": 5000,
        }
        self.assertDictEqual(self.loki_task_handler.get_read_parameters(self.ti, 1), params)

    @mock.patch.object(
        LokiTaskHandler,
        "is_loki_alive",
        return_value=200,
    )
    @mock.patch(
        "requests.Session.get",
    )
    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_read_loki(self, mock_con, mock_session, mock_live):
        mock_session.return_value.json.return_value = {
            "data": {
                "result": [
                    {
                        "values": [
                            ["1589981923", "test"],
                            ["1589981408", "value"],
                        ]
                    }
                ],
            },
        }
        text, metadata = self.loki_task_handler._read(self.ti, 1)
        self.assertEqual(len(text.splitlines()), 4)
        # skipping query string and testing only log messages
        self.assertEqual(text.splitlines()[2], "value")
        self.assertEqual(text.splitlines()[3], "test")
        self.assertEqual(metadata, {"end_of_log": True})

    @mock.patch.object(
        LokiTaskHandler,
        "is_loki_alive",
        return_value=400,
    )
    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_when_loki_absent(
        self,
        mock_con,
        mock_live,
    ):
        log, metadata = self.loki_task_handler.read(self.ti)
        self.assertEqual(1, len(log))
        self.assertEqual(len(log), len(metadata))
        self.assertEqual({"end_of_log": True}, metadata[0])
