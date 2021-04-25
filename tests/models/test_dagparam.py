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

import unittest
from datetime import timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs


class TestDagParamRuntime(unittest.TestCase):
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": timezone.utcnow(),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    VALUE = 42
    DEFAULT_DATE = timezone.datetime(2016, 1, 1)

    def tearDown(self):
        super().tearDown()
        clear_db_runs()

    def test_dag_param_resolves(self):
        """Test dagparam resolves on operator execution"""
        with DAG(dag_id="test_xcom_pass_to_op", default_args=self.DEFAULT_ARGS) as dag:
            value = dag.param('value', default=self.VALUE)

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            state=State.RUNNING,
        )

        # pylint: disable=maybe-no-member
        xcom_arg.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == self.VALUE

    def test_dag_param_overwrite(self):
        """Test dag param is overwritten from dagrun config"""
        with DAG(dag_id="test_xcom_pass_to_op", default_args=self.DEFAULT_ARGS) as dag:
            value = dag.param('value', default=self.VALUE)

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        assert dag.params['value'] == self.VALUE
        new_value = 2
        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            state=State.RUNNING,
            conf={'value': new_value},
        )

        # pylint: disable=maybe-no-member
        xcom_arg.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == new_value

    def test_dag_param_default(self):
        """Test dag param is overwritten from dagrun config"""
        with DAG(
            dag_id="test_xcom_pass_to_op", default_args=self.DEFAULT_ARGS, params={'value': 'test'}
        ) as dag:
            value = dag.param('value')

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            state=State.RUNNING,
        )

        # pylint: disable=maybe-no-member
        xcom_arg.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == 'test'
