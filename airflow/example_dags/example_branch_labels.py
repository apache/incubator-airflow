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
Example DAG demonstrating the usage of labels with different branches.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label

with DAG("example_branch_labels", schedule_interval="@daily", start_date=days_ago(2)) as dag:

    ingest = DummyOperator(task_id="ingest")
    analyse = DummyOperator(task_id="analyze")
    check = DummyOperator(task_id="check_integrity")
    describe = DummyOperator(task_id="describe_integrity")
    error = DummyOperator(task_id="email_error")
    save = DummyOperator(task_id="save")
    report = DummyOperator(task_id="report")

    ingest >> analyse >> check
    check >> Label("No errors") >> save >> report  # pylint: disable=expression-not-assigned
    check >> Label("Errors found") >> describe >> error >> report  # pylint: disable=expression-not-assigned
