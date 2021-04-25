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
This is an example dag that performs two refresh operations on a Tableau Workbook aka Extract. The first one
waits until it succeeds. The second does not wait since this is an asynchronous operation and we don't know
when the operation actually finishes. That's why we have another task that checks only that.
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.tableau.operators.tableau_refresh_workbook import TableauRefreshWorkbookOperator
from airflow.providers.tableau.sensors.tableau_job_status import TableauJobStatusSensor
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='example_tableau_refresh_workbook',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # Refreshes a workbook and waits until it succeeds.
    task_refresh_workbook_blocking = TableauRefreshWorkbookOperator(
        site_id='my_site',
        workbook_name='MyWorkbook',
        blocking=True,
        task_id='refresh_tableau_workbook_blocking',
    )
    # Refreshes a workbook and does not wait until it succeeds.
    task_refresh_workbook_non_blocking = TableauRefreshWorkbookOperator(
        site_id='my_site',
        workbook_name='MyWorkbook',
        blocking=False,
        task_id='refresh_tableau_workbook_non_blocking',
    )
    # The following task queries the status of the workbook refresh job until it succeeds.
    task_check_job_status = TableauJobStatusSensor(
        site_id='my_site',
        job_id="{{ ti.xcom_pull(task_ids='refresh_tableau_workbook_non_blocking') }}",
        task_id='check_tableau_job_status',
    )
    task_refresh_workbook_non_blocking >> task_check_job_status
