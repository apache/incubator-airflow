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

"""Example DAG demonstrating the usage of the AirbyteTriggerSyncOperator."""

from datetime import timedelta

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_airbyte_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
) as dag:

    # [START howto_operator_airbyte_synchronous]
    sync_source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_source_dest_example',
        airbyte_conn_id='airbyte_default',
        connection_id='15bc3800-82e4-48c3-a32d-620661273f28',
    )
    # [END howto_operator_airbyte_synchronous]

    # [START howto_operator_airbyte_asynchronous]
    async_source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_async_source_dest_example',
        airbyte_conn_id='airbyte_default',
        connection_id='15bc3800-82e4-48c3-a32d-620661273f28',
        asynchronous=True,
    )

    airbyte_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor_source_dest_example',
        airbyte_job_id="{{task_instance.xcom_pull(task_ids='airbyte_async_source_dest_example')}}",
        airbyte_conn_id='airbyte_default',
    )
    # [END howto_operator_airbyte_asynchronous]

    async_source_destination >> airbyte_sensor
