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
from typing import Any, List, Optional, Tuple

from flask import current_app, request
from marshmallow import ValidationError
from sqlalchemy import and_, func

from airflow.api.common.experimental.mark_tasks import set_state
from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import format_datetime, format_parameters
from airflow.api_connexion.schemas.task_instance_schema import (
    TaskInstanceCollection,
    TaskInstanceReferenceCollection,
    clear_task_instance_form,
    set_task_instance_state_form,
    task_instance_batch_form,
    task_instance_collection_schema,
    task_instance_reference_collection_schema,
    task_instance_schema,
)
from airflow.exceptions import SerializedDagNotFound
from airflow.models import SlaMiss
from airflow.models.dagrun import DagRun as DR
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.security import permissions
from airflow.utils.session import provide_session
from airflow.utils.state import State


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ]
)
@provide_session
def get_task_instance(dag_id: str, dag_run_id: str, task_id: str, session=None):
    """Get task instance"""
    query = (
        session.query(TI)
        .filter(TI.dag_id == dag_id)
        .join(DR, and_(TI.dag_id == DR.dag_id, TI.execution_date == DR.execution_date))
        .filter(DR.run_id == dag_run_id)
        .filter(TI.task_id == task_id)
        .outerjoin(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.execution_date == TI.execution_date,
                SlaMiss.task_id == TI.task_id,
            ),
        )
        .add_entity(SlaMiss)
    )
    task_instance = query.one_or_none()
    if task_instance is None:
        raise NotFound("Task instance not found")

    return task_instance_schema.dump(task_instance)


def _apply_array_filter(query, key, values):
    if values is not None:
        query = query.filter(key.in_(values))
    return query


def _apply_range_filter(query, key, value_range: Tuple[Any, Any]):
    gte_value, lte_value = value_range
    if gte_value is not None:
        query = query.filter(key >= gte_value)
    if lte_value is not None:
        query = query.filter(key <= lte_value)
    return query


@format_parameters(
    {
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
    }
)
@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ]
)
@provide_session
def get_task_instances(
    limit: int,
    dag_id: Optional[str] = None,
    dag_run_id: Optional[str] = None,
    execution_date_gte: Optional[str] = None,
    execution_date_lte: Optional[str] = None,
    start_date_gte: Optional[str] = None,
    start_date_lte: Optional[str] = None,
    end_date_gte: Optional[str] = None,
    end_date_lte: Optional[str] = None,
    duration_gte: Optional[float] = None,
    duration_lte: Optional[float] = None,
    state: Optional[str] = None,
    pool: Optional[List[str]] = None,
    queue: Optional[List[str]] = None,
    offset: Optional[int] = None,
    session=None,
):  # pylint: disable=too-many-arguments
    """Get list of task instances."""
    base_query = session.query(TI)

    if dag_id != "~":
        base_query = base_query.filter(TI.dag_id == dag_id)
    if dag_run_id != "~":
        base_query = base_query.join(DR, and_(TI.dag_id == DR.dag_id, TI.execution_date == DR.execution_date))
        base_query = base_query.filter(DR.run_id == dag_run_id)
    base_query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(execution_date_gte, execution_date_lte),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.start_date, value_range=(start_date_gte, start_date_lte)
    )
    base_query = _apply_range_filter(base_query, key=TI.end_date, value_range=(end_date_gte, end_date_lte))
    base_query = _apply_range_filter(base_query, key=TI.duration, value_range=(duration_gte, duration_lte))
    base_query = _apply_array_filter(base_query, key=TI.state, values=state)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=pool)
    base_query = _apply_array_filter(base_query, key=TI.queue, values=queue)

    # Count elements before joining extra columns
    total_entries = base_query.with_entities(func.count('*')).scalar()
    # Add join
    base_query = base_query.join(
        SlaMiss,
        and_(
            SlaMiss.dag_id == TI.dag_id,
            SlaMiss.task_id == TI.task_id,
            SlaMiss.execution_date == TI.execution_date,
        ),
        isouter=True,
    )
    ti_query = base_query.add_entity(SlaMiss)
    task_instances = ti_query.offset(offset).limit(limit).all()

    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ]
)
@provide_session
def get_task_instances_batch(session=None):
    """Get list of task instances."""
    body = request.get_json()
    try:
        data = task_instance_batch_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    base_query = session.query(TI)

    base_query = _apply_array_filter(base_query, key=TI.dag_id, values=data["dag_ids"])
    base_query = _apply_range_filter(
        base_query,
        key=TI.execution_date,
        value_range=(data["execution_date_gte"], data["execution_date_lte"]),
    )
    base_query = _apply_range_filter(
        base_query,
        key=TI.start_date,
        value_range=(data["start_date_gte"], data["start_date_lte"]),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.end_date, value_range=(data["end_date_gte"], data["end_date_lte"])
    )
    base_query = _apply_range_filter(
        base_query, key=TI.duration, value_range=(data["duration_gte"], data["duration_lte"])
    )
    base_query = _apply_array_filter(base_query, key=TI.state, values=data["state"])
    base_query = _apply_array_filter(base_query, key=TI.pool, values=data["pool"])
    base_query = _apply_array_filter(base_query, key=TI.queue, values=data["queue"])

    # Count elements before joining extra columns
    total_entries = base_query.with_entities(func.count('*')).scalar()
    # Add join
    base_query = base_query.join(
        SlaMiss,
        and_(
            SlaMiss.dag_id == TI.dag_id,
            SlaMiss.task_id == TI.task_id,
            SlaMiss.execution_date == TI.execution_date,
        ),
        isouter=True,
    )
    ti_query = base_query.add_entity(SlaMiss)
    task_instances = ti_query.all()

    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ]
)
@provide_session
def post_clear_task_instances(dag_id: str, session=None):
    """Clear task instances."""
    body = request.get_json()
    try:
        data = clear_task_instance_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    dag = current_app.dag_bag.get_dag(dag_id)
    if not dag:
        error_message = f"Dag id {dag_id} not found"
        raise NotFound(error_message)
    reset_dag_runs = data.pop('reset_dag_runs')
    task_instances = dag.clear(get_tis=True, **data)
    if not data["dry_run"]:
        clear_task_instances(
            task_instances, session, dag=dag, dag_run_state=State.RUNNING if reset_dag_runs else False
        )
    task_instances = task_instances.join(
        DR, and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date)
    ).add_column(DR.run_id)
    return task_instance_reference_collection_schema.dump(
        TaskInstanceReferenceCollection(task_instances=task_instances.all())
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ]
)
@provide_session
def post_set_task_instances_state(dag_id, session):
    """Set a state of task instances."""
    body = request.get_json()
    try:
        data = set_task_instance_state_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    error_message = f"Dag ID {dag_id} not found"
    try:
        dag = current_app.dag_bag.get_dag(dag_id)
        if not dag:
            raise NotFound(error_message)
    except SerializedDagNotFound:
        # If DAG is not found in serialized_dag table
        raise NotFound(error_message)

    task_id = data['task_id']
    task = dag.task_dict.get(task_id)

    if not task:
        error_message = f"Task ID {task_id} not found"
        raise NotFound(error_message)

    tis = set_state(
        tasks=[task],
        execution_date=data["execution_date"],
        upstream=data["include_upstream"],
        downstream=data["include_downstream"],
        future=data["include_future"],
        past=data["include_past"],
        state=data["new_state"],
        commit=not data["dry_run"],
    )
    execution_dates = {ti.execution_date for ti in tis}
    execution_date_to_run_id_map = dict(
        session.query(DR.execution_date, DR.run_id).filter(
            DR.dag_id == dag_id, DR.execution_date.in_(execution_dates)
        )
    )
    tis_with_run_id = [(ti, execution_date_to_run_id_map.get(ti.execution_date)) for ti in tis]
    return task_instance_reference_collection_schema.dump(
        TaskInstanceReferenceCollection(task_instances=tis_with_run_id)
    )
