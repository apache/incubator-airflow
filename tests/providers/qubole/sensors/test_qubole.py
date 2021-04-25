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

import unittest
from datetime import datetime
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, Connection
from airflow.providers.qubole.sensors.qubole import QuboleFileSensor, QubolePartitionSensor
from airflow.utils import db

DAG_ID = "qubole_test_dag"
TASK_ID = "test_task"
DEFAULT_CONN = "qubole_default"
TEMPLATE_CONN = "my_conn_id"
DEFAULT_DATE = datetime(2017, 1, 1)


class TestQuboleSensor(unittest.TestCase):
    def setUp(self):
        db.merge_conn(Connection(conn_id=DEFAULT_CONN, conn_type='HTTP'))

    @patch('airflow.providers.qubole.sensors.qubole.QuboleFileSensor.poke')
    def test_file_sensor(self, patched_poke):
        patched_poke.return_value = True
        sensor = QuboleFileSensor(
            task_id='test_qubole_file_sensor', data={"files": ["s3://some_bucket/some_file"]}
        )
        assert sensor.poke({})

    @patch('airflow.providers.qubole.sensors.qubole.QubolePartitionSensor.poke')
    def test_partition_sensor(self, patched_poke):
        patched_poke.return_value = True

        sensor = QubolePartitionSensor(
            task_id='test_qubole_partition_sensor',
            data={
                "schema": "default",
                "table": "my_partitioned_table",
                "columns": [{"column": "month", "values": ["1", "2"]}],
            },
        )

        assert sensor.poke({})

    @patch('airflow.providers.qubole.sensors.qubole.QubolePartitionSensor.poke')
    def test_partition_sensor_error(self, patched_poke):
        patched_poke.return_value = True

        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with pytest.raises(AirflowException):
            QubolePartitionSensor(
                task_id='test_qubole_partition_sensor',
                poke_interval=1,
                data={
                    "schema": "default",
                    "table": "my_partitioned_table",
                    "columns": [{"column": "month", "values": ["1", "2"]}],
                },
                dag=dag,
            )
