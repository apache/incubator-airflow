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
# [START dag]
"""This dag only runs some simple tasks to test Airflow's task execution."""
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

now = datetime.now()
now_to_the_hour = (now - timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
START_DATE = now_to_the_hour
DAG_NAME = 'test_dag_v1'

default_args = {'owner': 'airflow', 'depends_on_past': True, 'start_date': days_ago(2)}
dag = DAG(DAG_NAME, schedule_interval='*/10 * * * *', default_args=default_args)

run_this_1 = DummyOperator(task_id='run_this_1', dag=dag)
run_this_2 = DummyOperator(task_id='run_this_2', dag=dag)
run_this_2.set_upstream(run_this_1)
run_this_3 = DummyOperator(task_id='run_this_3', dag=dag)
run_this_3.set_upstream(run_this_2)
# [END dag]
