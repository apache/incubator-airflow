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

from flask import current_app

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.exceptions import TaskNotFound
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun as DR
from airflow.security import permissions
from airflow.utils.session import provide_session


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ]
)
@provide_session
def get_extra_links(dag_id: str, dag_run_id: str, task_id: str, session):
    """Get extra links for task instance"""
    dagbag: DagBag = current_app.dag_bag
    dag: DAG = dagbag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found", detail=f'DAG with ID = "{dag_id}" not found')

    try:
        task = dag.get_task(task_id)
    except TaskNotFound:
        raise NotFound("Task not found", detail=f'Task with ID = "{task_id}" not found')

    execution_date = (
        session.query(DR.execution_date).filter(DR.dag_id == dag_id).filter(DR.run_id == dag_run_id).scalar()
    )
    if not execution_date:
        raise NotFound("DAG Run not found", detail=f'DAG Run with ID = "{dag_run_id}" not found')

    all_extra_link_pairs = (
        (link_name, task.get_extra_links(execution_date, link_name)) for link_name in task.extra_links
    )
    all_extra_links = {
        link_name: link_url if link_url else None for link_name, link_url in all_extra_link_pairs
    }
    return all_extra_links
