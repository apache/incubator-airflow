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

import time

import pytest

from kubernetes_tests.test_base import EXECUTOR, TestBase


# These tests are here because only KubernetesExecutor can run the tests in
# test_kubernetes_executor.py
# Also, the skipping is necessary as there's no gain in running these tests in KubernetesExecutor
@pytest.mark.skipif(EXECUTOR == 'KubernetesExecutor', reason="Does not run on KubernetesExecutor")
class TestCeleryAndLocalExecutor(TestBase):
    def test_integration_run_dag(self):
        dag_id = 'example_bash_operator'
        dag_run_id, execution_date = self.start_job_in_kubernetes(dag_id, self.host)
        print(f"Found the job with execution_date {execution_date}")

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id='run_after_loop',
            expected_final_state='success',
            timeout=40,
        )

        self.ensure_dag_expected_state(
            host=self.host,
            execution_date=execution_date,
            dag_id=dag_id,
            expected_final_state='success',
            timeout=60,
        )

    def test_integration_run_dag_with_scheduler_failure(self):
        dag_id = 'example_xcom'

        dag_run_id, execution_date = self.start_job_in_kubernetes(dag_id, self.host)

        self._delete_airflow_pod("scheduler")

        time.sleep(10)  # give time for pod to restart

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id='push',
            expected_final_state='success',
            timeout=40,  # This should fail fast if failing
        )

        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id='puller',
            expected_final_state='success',
            timeout=40,
        )

        self.ensure_dag_expected_state(
            host=self.host,
            execution_date=execution_date,
            dag_id=dag_id,
            expected_final_state='success',
            timeout=60,
        )

        assert self._num_pods_in_namespace('test-namespace') == 0, "failed to delete pods in other namespace"
