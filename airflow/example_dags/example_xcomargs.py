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

"""Example DAG demonstrating the usage of the XComArgs."""
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)


def generate_value():
    """Dummy function"""
    return "Bring me a shrubbery!"


@task()
def print_value(value):
    """Dummy function"""
    ctx = get_current_context()
    log.info("The knights of Ni say: %s (at %s)", value, ctx['ts'])


with DAG(
    dag_id='example_xcom_args',
    default_args={'owner': 'airflow'},
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
) as dag:
    task1 = PythonOperator(
        task_id='generate_value',
        python_callable=generate_value,
    )

    print_value(task1.output)


with DAG(
    "example_xcom_args_with_operators",
    default_args={'owner': 'airflow'},
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
) as dag2:
    bash_op1 = BashOperator(task_id="c", bash_command="echo c")
    bash_op2 = BashOperator(task_id="d", bash_command="echo c")
    xcom_args_a = print_value("first!")
    xcom_args_b = print_value("second!")

    bash_op1 >> xcom_args_a >> xcom_args_b >> bash_op2
