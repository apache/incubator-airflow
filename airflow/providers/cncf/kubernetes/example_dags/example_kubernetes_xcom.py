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
This is an example dag for using xcom with KubernetesPodOperator.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago


def python_operator_xcom_kw(**kwargs):
    """
    Xcom pull task
    """
    result = kwargs['ti'].xcom_pull(key='return_value', task_ids='k8s_pod_operator_xcom_task')
    print(f'Value received from k8s_pod_operator  date : {result["date"]}   release : {result["release"]}')


# [START kubernetes_xcom]
with DAG(
    dag_id="example_k8s_operator_xcom", start_date=days_ago(1), schedule_interval='@once', tags=["example"]
) as dag:
    start_1 = KubernetesPodOperator(
        task_id='k8s_pod_operator_xcom_task',
        name='airflow_pod_operator_xcom',
        namespace='default',
        image='alpine',
        cmds=[
            "sh",
            "-c",
            'mkdir -p /airflow/xcom/;echo {\\"date\\": \\"$(date)\\", \\"release\\": '
            '\\"$(uname -r)\\"} > /airflow/xcom/return.json',
        ],
        do_xcom_push=True,
        in_cluster=False,
        startup_timeout_seconds=60,
    )

    end_1 = PythonOperator(task_id='python_operator_xcom', python_callable=python_operator_xcom_kw)

    start_1 >> end_1
# [END kubernetes_xcom]
