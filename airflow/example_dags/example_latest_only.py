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

"""Example of the LatestOnlyOperator"""

import datetime as dt

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='latest_only',
    schedule_interval=dt.timedelta(hours=4),
    start_date=days_ago(2),
    tags=['example2', 'example3'],
) as dag1:

    latest_only = LatestOnlyOperator(task_id='latest_only')
    task1 = DummyOperator(task_id='task1')

    latest_only >> task1


# [START latest_only_param]
with DAG(
    dag_id='latest_only_param',
    schedule_interval='0 0 * * *',
    start_date=days_ago(5),
) as dag2:
    will_skip = DummyOperator(task_id='will_skip', latest_only=True)
    wont_skip = DummyOperator(task_id='wont_skip', latest_only=False)
# [END latest_only_param]
