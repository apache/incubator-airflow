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
from airflow.providers.asana.operators.asana_tasks import AsanaCreateTaskOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    'example_asana',
    default_args=default_args,
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    # [START run_asana_create_task_operator]
    task = AsanaCreateTaskOperator(
        task_id='run_asana_create_task',
        projects=["your_project"],
        asana_conn_id='asana_test',
        name='Test Task',
    )
    # [END run_asana_create_task_operator]

