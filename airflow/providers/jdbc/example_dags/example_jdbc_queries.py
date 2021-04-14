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

"""Example DAG demonstrating the usage of the JdbcOperator."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_jdbc_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
) as dag:

    run_this_last = DummyOperator(
        task_id='run_this_last',
        dag=dag,
    )

    # [START howto_operator_jdbc_template]
    delete_data = JdbcOperator(
        task_id='delete_data',
        sql='delete from my_schema.my_table where dt = {{ ds }}',
        jdbc_conn_id='my_jdbc_connection',
        autocommit=True,
        dag=dag,
    )
    # [END howto_operator_jdbc_template]

    # [START howto_operator_jdbc]
    insert_data = JdbcOperator(
        task_id='insert_data',
        sql='insert into my_schema.my_table select dt, value from my_schema.source_data',
        jdbc_conn_id='my_jdbc_connection',
        autocommit=True,
        dag=dag,
    )
    # [END howto_operator_jdbc]

    delete_data >> insert_data >> run_this_last
