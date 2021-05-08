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
Example showing how to use Asana CreateTaskOperator.
"""

from airflow import DAG
from airflow.providers.asana.operators.asana_tasks import (
    AsanaCreateTaskOperator,
    AsanaDeleteTaskOperator,
    AsanaFindTaskOperator,
    AsanaUpdateTaskOperator,
)
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
}


with DAG(
    "example_asana",
    default_args=default_args,
    start_date=days_ago(1),
    tags=["example"],
) as dag:
    conn_id = "asana_test"

    # [START run_asana_create_task_operator]
    create = AsanaCreateTaskOperator(
        task_id="run_asana_create_task",
        task_parameters={"projects": "your_project"},
        conn_id=conn_id,
        name="Test Task Create",
    )
    # [END run_asana_create_task_operator]

    # [START run_asana_find_task_operator]
    find = AsanaFindTaskOperator(
        task_id="run_asana_find_task",
        search_parameters={"project": "your_project"},
        conn_id=conn_id,
    )
    # [END run_asana_find_task_operator]

    # [START run_asana_update_task_operator]
    update = AsanaUpdateTaskOperator(
        task_id="run_asana_update_task",
        asana_task_gid="your_task_id",
        task_parameters={"notes": "This task was updated!", "completed": True},
        conn_id=conn_id,
    )
    # [END run_asana_update_task_operator]

    # [START run_asana_delete_task_operator]
    delete = AsanaDeleteTaskOperator(
        task_id="run_asana_delete_task",
        conn_id=conn_id,
        asana_task_gid="your_task_id",
    )
    # [END run_asana_delete_task_operator]

    create >> find >> update >> delete
