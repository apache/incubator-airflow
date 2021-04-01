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

"""
Example DAG demonstrating setting up inter-DAG dependencies using ExternalTaskSensor and
ExternalTaskMarker

In this example, child_task1 in example_external_task_marker_child depends on parent_task in
example_external_task_marker_parent. When parent_task is cleared with "Recursive" selected,
the presence of ExternalTaskMarker tells Airflow to clear child_task1 and its
downstream tasks.

ExternalTaskSensor will keep poking for the status of remote ExternalTaskMarker task at a regular
interval till one of the following will happen:
1. ExternalTaskMarker reaches the states mentioned in the allowed_states list
    In this case, ExternalTaskSensor will exit with a success status code
2. ExternalTaskMarker reaches the states mentioned in the failed_states list
    In this case, ExternalTaskSensor will raise an AirflowException and user need to handle this
    with multiple downstream tasks
3. ExternalTaskSensor times out
    In this case, ExternalTaskSensor will raise AirflowSkipException or AirflowSensorTimeout
    exception
"""

import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

start_date = datetime.datetime(2015, 1, 1)

with DAG(
    dag_id="example_external_task_marker_parent",
    start_date=start_date,
    schedule_interval=None,
    tags=['example2'],
) as parent_dag:
    # [START howto_operator_external_task_marker]
    parent_task = ExternalTaskMarker(
        task_id="parent_task",
        external_dag_id="example_external_task_marker_child",
        external_task_id="child_task1",
    )
    # [END howto_operator_external_task_marker]

with DAG(
    dag_id="example_external_task_marker_child",
    start_date=start_date,
    schedule_interval=None,
    tags=['example2'],
) as child_dag:
    # [START howto_operator_external_task_sensor]
    child_task1 = ExternalTaskSensor(
        task_id="child_task1",
        external_dag_id=parent_dag.dag_id,
        external_task_id=parent_task.task_id,
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",
    )
    # [END howto_operator_external_task_sensor]
    child_task2 = DummyOperator(task_id="child_task2")
    child_task1 >> child_task2
