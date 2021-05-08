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

import sys
import unittest.mock
from datetime import timedelta

from airflow.decorators import task
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    'AIRFLOW_CTX_DAG_ID',
    'AIRFLOW_CTX_TASK_ID',
    'AIRFLOW_CTX_EXECUTION_DATE',
    'AIRFLOW_CTX_DAG_RUN_ID',
]

PYTHON_VERSION = sys.version_info[0]


class TestDockerDecorator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super().setUp()
        self.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def clear_run(self):
        self.run = False

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_basic_docker_operator(self):
        @task.docker(
            image="quay.io/bitnami/python:3.8.8",
            force_pull=True,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            api_version='auto',
        )
        def f():
            import random

            return [random.random() for i in range(100)]

        with self.dag:
            ret = f()

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=no-member
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100

    def test_basic_docker_operator_with_param(self):
        @task.docker(
            image="quay.io/bitnami/python:3.8.8",
            force_pull=True,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            api_version='auto',
        )
        def f(num_results):
            import random

            return [random.random() for i in range(num_results)]

        with self.dag:
            ret = f(100)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=no-member
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100

    def test_basic_docker_operator_multiple_output(self):
        @task.docker(
            image="quay.io/bitnami/python:3.8.8",
            force_pull=True,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            api_version='auto',
            multiple_outputs=True,
        )
        def return_dict(number: int):
            return {'number': number + 1, '43': 43}

        test_number = 10
        with self.dag:
            ret = return_dict(test_number)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key='number') == test_number + 1
        assert ti.xcom_pull(key='43') == 43
        assert ti.xcom_pull() == {'number': test_number + 1, '43': 43}

    def test_call_20(self):
        """Test calling decorated function 21 times in a DAG"""

        @task.docker(
            image="quay.io/bitnami/python:3.8.8",
            force_pull=True,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            api_version='auto',
        )
        def __do_run():
            return 4

        with self.dag:
            __do_run()
            for _ in range(20):
                __do_run()

        assert self.dag.task_ids[-1] == '__do_run__20'
